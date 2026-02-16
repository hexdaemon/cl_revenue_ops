"""
Boltz Reverse Swap (Loop Out) module for cl-revenue-ops

Implements Lightning -> on-chain BTC swaps using Boltz v2 API.
Tracks costs and swap state in SQLite.

Design goals:
- No rune requirements (runs inside CLN plugin)
- Explicit cost tracking
- Cooperative claim flow (preimage -> Boltz co-sign + broadcast)
- Safe polling with timeouts
"""

import os
import time
import json
import hashlib
import secrets
import subprocess
import tempfile
from typing import Dict, Any, Optional, Tuple

try:
    import urllib.request as _urlreq
    import urllib.error as _urlerr
except Exception:
    _urlreq = None

DEFAULT_BOLTZ_API = os.environ.get("BOLTZ_API", "https://api.boltz.exchange/v2")


class BoltzSwapManager:
    def __init__(self, database, safe_plugin, config):
        self.db = database
        self.plugin = safe_plugin
        self.rpc = safe_plugin.rpc
        self.config = config
        self.api = DEFAULT_BOLTZ_API

        # Ensure table exists (idempotent)
        self._ensure_table()

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------

    def _log(self, msg: str, level: str = "info"):
        try:
            self.plugin.log(f"Boltz: {msg}", level=level)
        except Exception:
            pass

    def _http_get(self, path: str, timeout: int = 15) -> Dict[str, Any]:
        if _urlreq is None:
            raise RuntimeError("urllib not available")
        url = f"{self.api}{path}"
        req = _urlreq.Request(url)
        with _urlreq.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            return json.loads(data)

    def _http_post(self, path: str, payload: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
        if _urlreq is None:
            raise RuntimeError("urllib not available")
        url = f"{self.api}{path}"
        body = json.dumps(payload).encode()
        req = _urlreq.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with _urlreq.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except _urlerr.HTTPError as e:
            try:
                return json.loads(e.read())
            except Exception:
                raise

    def _now_ts(self) -> int:
        return int(time.time())

    def _ensure_table(self):
        conn = self.db._get_connection()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS boltz_swaps (
                id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                node_id TEXT,
                invoice_amount_sats INTEGER NOT NULL,
                onchain_amount_sats INTEGER NOT NULL,
                boltz_fee_pct REAL NOT NULL,
                boltz_fee_sats INTEGER NOT NULL,
                miner_fee_lockup_sats INTEGER NOT NULL,
                miner_fee_claim_sats INTEGER NOT NULL,
                total_cost_sats INTEGER NOT NULL,
                cost_ppm INTEGER NOT NULL,
                status TEXT NOT NULL,
                preimage_hash TEXT NOT NULL,
                preimage TEXT,
                claim_privkey TEXT,
                claim_pubkey TEXT,
                address TEXT,
                timeout_block INTEGER,
                lockup_txid TEXT,
                claim_txid TEXT,
                error TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_boltz_swaps_status ON boltz_swaps(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_boltz_swaps_created ON boltz_swaps(created_at)")

    def _record_swap(self, rec: Dict[str, Any]):
        conn = self.db._get_connection()
        fields = [
            "id", "created_at", "updated_at", "node_id", "invoice_amount_sats",
            "onchain_amount_sats", "boltz_fee_pct", "boltz_fee_sats",
            "miner_fee_lockup_sats", "miner_fee_claim_sats", "total_cost_sats",
            "cost_ppm", "status", "preimage_hash", "preimage", "claim_privkey",
            "claim_pubkey", "address", "timeout_block", "lockup_txid", "claim_txid",
            "error"
        ]
        values = [rec.get(f) for f in fields]
        placeholders = ",".join(["?"] * len(fields))
        conn.execute(
            f"INSERT OR REPLACE INTO boltz_swaps ({','.join(fields)}) VALUES ({placeholders})",
            values
        )

    def _get_swap(self, swap_id: str) -> Optional[Dict[str, Any]]:
        conn = self.db._get_connection()
        row = conn.execute("SELECT * FROM boltz_swaps WHERE id = ?", (swap_id,)).fetchone()
        if not row:
            return None
        return dict(row)

    def _list_swaps(self, limit: int = 20) -> Dict[str, Any]:
        conn = self.db._get_connection()
        rows = conn.execute(
            "SELECT * FROM boltz_swaps ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        swaps = [dict(r) for r in rows]
        return swaps

    # ---------------------------------------------------------------------
    # Key + preimage generation
    # ---------------------------------------------------------------------

    def _generate_preimage(self) -> Tuple[bytes, bytes]:
        preimage = secrets.token_bytes(32)
        preimage_hash = hashlib.sha256(preimage).digest()
        return preimage, preimage_hash

    def _generate_secp256k1_keypair(self) -> Tuple[str, str]:
        """
        Generate a secp256k1 keypair using OpenSSL.
        Returns (privkey_hex, xonly_pubkey_hex).

        This avoids adding Python crypto dependencies.
        """
        with tempfile.NamedTemporaryFile() as f:
            key_path = f.name
            try:
                subprocess.check_call([
                    "openssl", "ecparam", "-name", "secp256k1", "-genkey", "-noout", "-out", key_path
                ])
                output = subprocess.check_output([
                    "openssl", "ec", "-in", key_path, "-text", "-noout"
                ]).decode()
            except FileNotFoundError:
                raise RuntimeError("openssl not found; cannot generate secp256k1 keys")
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"openssl key generation failed: {e}")

        priv_bytes = b""
        pub_bytes = b""
        in_priv = False
        in_pub = False

        for line in output.splitlines():
            line = line.strip()
            if line.startswith("priv:"):
                in_priv = True
                in_pub = False
                continue
            if line.startswith("pub:"):
                in_pub = True
                in_priv = False
                continue
            if line.startswith("ASN1 OID:") or line.startswith("NIST CURVE:"):
                in_priv = False
                in_pub = False
                continue

            if in_priv and line:
                priv_bytes += bytes.fromhex(line.replace(":", ""))
            if in_pub and line:
                pub_bytes += bytes.fromhex(line.replace(":", ""))

        if len(priv_bytes) >= 32:
            priv_bytes = priv_bytes[-32:]
        priv_hex = priv_bytes.hex()
        if len(priv_hex) != 64:
            raise RuntimeError("Invalid privkey length from OpenSSL")

        if len(pub_bytes) >= 65 and pub_bytes[0] == 0x04:
            x_only = pub_bytes[1:33]
        else:
            raise RuntimeError("Failed to parse pubkey from OpenSSL")

        pub_hex = x_only.hex()
        return priv_hex, pub_hex

    # ---------------------------------------------------------------------
    # API / business logic
    # ---------------------------------------------------------------------

    def quote(self, amount_sats: int) -> Dict[str, Any]:
        pairs = self._http_get("/swap/reverse")
        btc_pair = pairs.get("BTC", {}).get("BTC", {})
        if not btc_pair:
            return {"error": "BTC/BTC reverse pair not available"}

        limits = btc_pair.get("limits", {})
        fees = btc_pair.get("fees", {})
        pct = fees.get("percentage", 0.5)
        miner_claim = fees.get("minerFees", {}).get("claim", 222)
        miner_lockup = fees.get("minerFees", {}).get("lockup", 308)

        boltz_fee_sats = int(amount_sats * pct / 100)
        total_miner = miner_claim + miner_lockup
        total_cost = boltz_fee_sats + total_miner
        onchain_amount = amount_sats - total_cost

        return {
            "invoice_amount_sats": amount_sats,
            "onchain_amount_sats": onchain_amount,
            "boltz_fee_pct": pct,
            "boltz_fee_sats": boltz_fee_sats,
            "miner_fee_claim_sats": miner_claim,
            "miner_fee_lockup_sats": miner_lockup,
            "total_miner_sats": total_miner,
            "total_cost_sats": total_cost,
            "cost_ppm": int(total_cost * 1_000_000 / amount_sats) if amount_sats else 0,
            "limits": limits,
            "pair_hash": btc_pair.get("hash", "")
        }

    def _get_node_id(self) -> str:
        try:
            info = self.rpc.getinfo()
            return info.get("id") or ""
        except Exception:
            return ""

    def loop_out(self, amount_sats: int, address: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
        if amount_sats < 25000:
            return {"error": "amount_sats must be >= 25,000"}

        quote = self.quote(amount_sats)
        if "error" in quote:
            return quote

        limits = quote.get("limits", {})
        if amount_sats < limits.get("minimal", 25000):
            return {"error": f"amount_sats below minimum {limits.get('minimal')}"}
        if amount_sats > limits.get("maximal", 25000000):
            return {"error": f"amount_sats above maximum {limits.get('maximal')}"}

        if dry_run:
            return {"dry_run": True, "quote": quote}

        # address from node if not provided
        if not address:
            try:
                addr_res = self.rpc.newaddr(addresstype="bech32")
            except Exception:
                addr_res = self.rpc.newaddr()
            address = addr_res.get("bech32") or addr_res.get("address")
            if not address:
                return {"error": f"newaddr returned unexpected response: {addr_res}"}

        # Preimage + keypair
        preimage, preimage_hash = self._generate_preimage()
        claim_priv, claim_pub = self._generate_secp256k1_keypair()

        # Create reverse swap
        payload = {
            "from": "BTC",
            "to": "BTC",
            "preimageHash": preimage_hash.hex(),
            "claimPublicKey": claim_pub,
            "invoiceAmount": amount_sats,
            "address": address,
            "description": "cl-revenue-ops loop-out"
        }
        swap = self._http_post("/swap/reverse", payload)

        if not swap.get("id"):
            return {"error": f"Boltz swap creation failed: {swap}"}

        swap_id = swap["id"]
        invoice = swap["invoice"]
        onchain_amount = swap.get("onchainAmount", quote["onchain_amount_sats"])
        timeout_block = swap.get("timeoutBlockHeight", 0)

        # Record
        rec = {
            "id": swap_id,
            "created_at": self._now_ts(),
            "updated_at": self._now_ts(),
            "node_id": self._get_node_id(),
            "invoice_amount_sats": amount_sats,
            "onchain_amount_sats": onchain_amount,
            "boltz_fee_pct": quote["boltz_fee_pct"],
            "boltz_fee_sats": quote["boltz_fee_sats"],
            "miner_fee_lockup_sats": quote["miner_fee_lockup_sats"],
            "miner_fee_claim_sats": quote["miner_fee_claim_sats"],
            "total_cost_sats": amount_sats - onchain_amount,
            "cost_ppm": int((amount_sats - onchain_amount) * 1_000_000 / amount_sats),
            "status": "created",
            "preimage_hash": preimage_hash.hex(),
            "preimage": preimage.hex(),
            "claim_privkey": claim_priv,
            "claim_pubkey": claim_pub,
            "address": address,
            "timeout_block": timeout_block,
            "lockup_txid": None,
            "claim_txid": None,
            "error": None,
        }
        self._record_swap(rec)

        # Pay invoice
        rec["status"] = "paying"
        rec["updated_at"] = self._now_ts()
        self._record_swap(rec)

        try:
            try:
                pay_res = self.rpc.xpay(invstring=invoice)
            except Exception:
                pay_res = self.rpc.pay(bolt11=invoice)
            rec["status"] = "paid"
            rec["updated_at"] = self._now_ts()
            self._record_swap(rec)
        except Exception as e:
            rec["status"] = "failed"
            rec["error"] = str(e)
            rec["updated_at"] = self._now_ts()
            self._record_swap(rec)
            return {"error": f"payment failed: {e}", "swap_id": swap_id}

        # Wait for lockup
        lockup = self._wait_for_lockup(swap_id)
        if lockup.get("error"):
            rec["status"] = "timeout_lockup"
            rec["error"] = lockup.get("error")
            rec["updated_at"] = self._now_ts()
            self._record_swap(rec)
            return {"error": lockup.get("error"), "swap_id": swap_id}
        rec["lockup_txid"] = lockup.get("lockup_txid")
        rec["status"] = "locked"
        rec["updated_at"] = self._now_ts()
        self._record_swap(rec)

        # Cooperative claim
        try:
            claim_res = self._http_post(f"/swap/reverse/{swap_id}/claim", {"preimage": preimage.hex()})
            rec["status"] = "completed"
            rec["updated_at"] = self._now_ts()
            self._record_swap(rec)
            return {
                "status": "completed",
                "swap_id": swap_id,
                "onchain_amount_sats": onchain_amount,
                "total_cost_sats": amount_sats - onchain_amount,
                "cost_ppm": rec["cost_ppm"],
                "lockup_txid": rec.get("lockup_txid"),
                "claim_result": claim_res,
            }
        except Exception as e:
            rec["status"] = "claim_failed"
            rec["error"] = str(e)
            rec["updated_at"] = self._now_ts()
            self._record_swap(rec)
            return {
                "error": f"cooperative claim failed: {e}",
                "swap_id": swap_id,
                "note": "Funds locked on-chain. Manual claim may be required.",
                "preimage": preimage.hex(),
                "claim_privkey": claim_priv,
                "claim_pubkey": claim_pub,
            }

    def _wait_for_lockup(self, swap_id: str, timeout: int = 600, interval: int = 10) -> Dict[str, Any]:
        start = time.time()
        while time.time() - start < timeout:
            try:
                status = self._http_get(f"/swap/status?id={swap_id}")
                st = status.get("status", "")
                if st in ("transaction.mempool", "transaction.confirmed"):
                    try:
                        tx = self._http_get(f"/swap/reverse/{swap_id}/transaction")
                        return {"lockup_txid": tx.get("id"), "status": st}
                    except Exception:
                        return {"lockup_txid": None, "status": st}
                if st == "swap.expired":
                    return {"error": "swap expired"}
                if st.startswith("transaction.failed") or st.startswith("swap.error"):
                    return {"error": st}
            except Exception as e:
                self._log(f"Status poll error: {e}", level="warn")

            time.sleep(interval)

        return {"error": "timeout waiting for lockup"}

    def status(self, swap_id: str) -> Dict[str, Any]:
        local = self._get_swap(swap_id)
        try:
            remote = self._http_get(f"/swap/status?id={swap_id}")
        except Exception as e:
            remote = {"error": str(e)}
        return {"local": local, "boltz": remote}

    def history(self, limit: int = 20) -> Dict[str, Any]:
        swaps = self._list_swaps(limit)
        completed = [s for s in swaps if s.get("status") == "completed"]
        total_sent = sum(s.get("invoice_amount_sats", 0) for s in completed)
        total_received = sum(s.get("onchain_amount_sats", 0) for s in completed)
        total_cost = sum(s.get("total_cost_sats", 0) for s in completed)
        return {
            "swaps": swaps,
            "totals": {
                "count": len(swaps),
                "completed": len(completed),
                "total_sent": total_sent,
                "total_received": total_received,
                "total_cost": total_cost,
                "avg_cost_ppm": int(total_cost * 1_000_000 / total_sent) if total_sent else 0,
            }
        }
