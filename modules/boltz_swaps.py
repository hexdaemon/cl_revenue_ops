"""
Boltz swap module for cl-revenue-ops.

Implements:
- Loop-out (reverse swap): Lightning -> on-chain BTC
- Loop-in (submarine swap): on-chain BTC -> Lightning

Tracks costs and swap state in SQLite.
"""

import os
import time
import json
import hashlib
import secrets
import subprocess
import tempfile
from typing import Dict, Any, Optional, Tuple, List

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
                swap_type TEXT NOT NULL DEFAULT 'loop_out',
                target_channel_id TEXT,
                target_peer_id TEXT,
                bolt11_invoice TEXT,
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
        # Keep existing installations forward-compatible with new columns.
        self._ensure_columns(conn, {
            "swap_type": "TEXT NOT NULL DEFAULT 'loop_out'",
            "target_channel_id": "TEXT",
            "target_peer_id": "TEXT",
            "bolt11_invoice": "TEXT",
        })
        conn.execute("CREATE INDEX IF NOT EXISTS idx_boltz_swaps_status ON boltz_swaps(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_boltz_swaps_created ON boltz_swaps(created_at)")

    def _ensure_columns(self, conn, columns: Dict[str, str]) -> None:
        rows = conn.execute("PRAGMA table_info(boltz_swaps)").fetchall()
        existing = set()
        for row in rows:
            if isinstance(row, dict):
                existing.add(row.get("name"))
            else:
                # sqlite3.Row is indexable; name is column #1 in PRAGMA output.
                existing.add(row[1])

        for name, ddl in columns.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE boltz_swaps ADD COLUMN {name} {ddl}")

    def _record_swap(self, rec: Dict[str, Any]):
        conn = self.db._get_connection()
        fields = [
            "id", "created_at", "updated_at", "node_id", "swap_type",
            "target_channel_id", "target_peer_id", "bolt11_invoice", "invoice_amount_sats",
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

    def _generate_secp256k1_keypair(self, pubkey_format: str = "xonly") -> Tuple[str, str]:
        """
        Generate a secp256k1 keypair using OpenSSL.
        Returns (privkey_hex, pubkey_hex).

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

        if len(pub_bytes) < 65 or pub_bytes[0] != 0x04:
            raise RuntimeError("Failed to parse pubkey from OpenSSL")

        x_coord = pub_bytes[1:33]
        y_coord = pub_bytes[33:65]
        if pubkey_format == "xonly":
            pub_hex = x_coord.hex()
        elif pubkey_format == "compressed":
            prefix = b"\x02" if (y_coord[-1] % 2 == 0) else b"\x03"
            pub_hex = (prefix + x_coord).hex()
        else:
            raise ValueError(f"Unsupported pubkey_format: {pubkey_format}")

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

    def _get_submarine_limits(self) -> Dict[str, Any]:
        pairs = self._http_get("/swap/submarine")
        btc_pair = pairs.get("BTC", {}).get("BTC", {})
        if not btc_pair:
            return {"error": "BTC/BTC submarine pair not available"}
        return {
            "limits": btc_pair.get("limits", {}),
            "pair_hash": btc_pair.get("hash", "")
        }

    def _list_peerchannels(self, peer_id: Optional[str] = None) -> List[Dict[str, Any]]:
        try:
            if peer_id:
                res = self.rpc.listpeerchannels(id=peer_id)
            else:
                res = self.rpc.listpeerchannels()
            return res.get("channels", [])
        except Exception:
            return []

    def _channel_matches(self, channel: Dict[str, Any], channel_id: str) -> bool:
        target = str(channel_id).strip()
        vals = {
            str(channel.get("short_channel_id") or ""),
            str(channel.get("channel_id") or ""),
        }
        return target in vals

    def _channel_is_routable(self, channel: Dict[str, Any]) -> bool:
        state = str(channel.get("state", "")).upper()
        return (
            bool(channel.get("short_channel_id"))
            and "ONCHAIN" not in state
            and "CLOSED" not in state
        )

    def _collect_invoice_hints(
        self,
        channel_id: Optional[str] = None,
        peer_id: Optional[str] = None
    ) -> Dict[str, Any]:
        if channel_id and peer_id:
            return {"error": "Provide either channel_id or peer_id, not both"}

        if channel_id:
            channels = self._list_peerchannels()
            matches = [c for c in channels if self._channel_matches(c, channel_id)]
            if not matches:
                return {"error": f"channel_id not found: {channel_id}"}
            hints = [c.get("short_channel_id") for c in matches if self._channel_is_routable(c)]
            if not hints:
                return {"error": f"channel_id has no routable short_channel_id: {channel_id}"}
            return {"hints": hints}

        if peer_id:
            channels = self._list_peerchannels(peer_id)
            if not channels:
                return {"error": f"peer_id not found or has no channels: {peer_id}"}
            hints = [c.get("short_channel_id") for c in channels if self._channel_is_routable(c)]
            if not hints:
                return {"error": f"peer_id has no routable short_channel_id channels: {peer_id}"}
            return {"hints": hints}

        return {"hints": []}

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
            "swap_type": "loop_out",
            "target_channel_id": None,
            "target_peer_id": None,
            "bolt11_invoice": invoice,
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

    def loop_in(
        self,
        amount_sats: int,
        channel_id: Optional[str] = None,
        peer_id: Optional[str] = None
    ) -> Dict[str, Any]:
        if amount_sats < 25000:
            return {"error": "amount_sats must be >= 25,000"}

        hint_res = self._collect_invoice_hints(channel_id=channel_id, peer_id=peer_id)
        if hint_res.get("error"):
            return hint_res
        hints = hint_res.get("hints", [])

        limits_info = self._get_submarine_limits()
        if limits_info.get("error"):
            return limits_info

        limits = limits_info.get("limits", {})
        if amount_sats < limits.get("minimal", 25000):
            return {"error": f"amount_sats below minimum {limits.get('minimal')}"}
        if amount_sats > limits.get("maximal", 25000000):
            return {"error": f"amount_sats above maximum {limits.get('maximal')}"}

        label = f"boltz-loop-in-{self._now_ts()}-{secrets.token_hex(4)}"
        description = "cl-revenue-ops loop-in"
        invoice_kwargs: Dict[str, Any] = {
            "amount_msat": f"{amount_sats * 1000}msat",
            "label": label,
            "description": description,
            "expiry": 3600,
        }
        if hints:
            invoice_kwargs["exposeprivatechannels"] = hints

        try:
            invoice_res = self.rpc.invoice(**invoice_kwargs)
        except Exception:
            # Older CLN versions may prefer integer amount_msat.
            invoice_kwargs["amount_msat"] = amount_sats * 1000
            invoice_res = self.rpc.invoice(**invoice_kwargs)

        bolt11 = invoice_res.get("bolt11")
        if not bolt11:
            return {"error": f"invoice returned unexpected response: {invoice_res}"}

        payment_hash = (
            invoice_res.get("payment_hash")
            or invoice_res.get("paymentHash")
            or hashlib.sha256(bolt11.encode()).hexdigest()
        )

        refund_priv, refund_pub = self._generate_secp256k1_keypair(pubkey_format="compressed")
        payload: Dict[str, Any] = {
            "from": "BTC",
            "to": "BTC",
            "invoice": bolt11,
            "refundPublicKey": refund_pub,
        }
        pair_hash = limits_info.get("pair_hash")
        if pair_hash:
            payload["pairHash"] = pair_hash

        swap = self._http_post("/swap/submarine", payload)
        if not swap.get("id"):
            return {"error": f"Boltz loop-in creation failed: {swap}"}

        swap_id = swap["id"]
        expected_onchain = int(swap.get("expectedAmount", amount_sats))
        timeout_block = int(swap.get("timeoutBlockHeight", 0))
        funding_address = swap.get("address") or swap.get("lockupAddress")
        status = swap.get("status", "created")
        total_cost = max(0, expected_onchain - amount_sats)

        rec = {
            "id": swap_id,
            "created_at": self._now_ts(),
            "updated_at": self._now_ts(),
            "node_id": self._get_node_id(),
            "swap_type": "loop_in",
            "target_channel_id": channel_id,
            "target_peer_id": peer_id,
            "bolt11_invoice": bolt11,
            "invoice_amount_sats": amount_sats,
            "onchain_amount_sats": expected_onchain,
            "boltz_fee_pct": 0.0,
            "boltz_fee_sats": 0,
            "miner_fee_lockup_sats": 0,
            "miner_fee_claim_sats": 0,
            "total_cost_sats": total_cost,
            "cost_ppm": int(total_cost * 1_000_000 / amount_sats) if amount_sats else 0,
            "status": status,
            "preimage_hash": payment_hash,
            "preimage": None,
            "claim_privkey": refund_priv,
            "claim_pubkey": refund_pub,
            "address": funding_address,
            "timeout_block": timeout_block,
            "lockup_txid": None,
            "claim_txid": None,
            "error": None,
        }
        self._record_swap(rec)

        return {
            "status": "awaiting_onchain_funding",
            "swap_id": swap_id,
            "boltz_status": status,
            "amount_sats": amount_sats,
            "expected_onchain_sats": expected_onchain,
            "funding_address": funding_address,
            "bip21": swap.get("bip21"),
            "channel_id": channel_id,
            "peer_id": peer_id,
            "invoice_label": label,
        }

    def _is_failed_status(self, status: str) -> bool:
        st = (status or "").lower()
        return (
            st.startswith("transaction.failed")
            or st.startswith("swap.error")
            or st.startswith("invoice.failed")
            or st == "swap.expired"
            or st == "failed"
        )

    def _is_completed_status(self, status: str, swap_type: str) -> bool:
        st = (status or "").lower()
        if swap_type == "loop_in":
            return (
                st in ("completed", "invoice.paid", "invoice.settled", "transaction.claimed")
                or st.startswith("invoice.paid")
                or st.startswith("transaction.claimed")
            )
        return st == "completed"

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

        if local and remote.get("status"):
            remote_status = remote.get("status")
            if remote_status and remote_status != local.get("status"):
                local["status"] = remote_status
                local["updated_at"] = self._now_ts()
                if self._is_failed_status(remote_status):
                    local["error"] = remote_status
                self._record_swap(local)

        return {"local": local, "boltz": remote}

    def history(self, limit: int = 20) -> Dict[str, Any]:
        swaps = self._list_swaps(limit)
        loop_out_completed = [
            s for s in swaps
            if self._is_completed_status(s.get("status", ""), s.get("swap_type", "loop_out") or "loop_out")
            and (s.get("swap_type") or "loop_out") != "loop_in"
        ]
        loop_in_completed = [
            s for s in swaps
            if self._is_completed_status(s.get("status", ""), s.get("swap_type", "loop_out") or "loop_out")
            and (s.get("swap_type") or "loop_out") == "loop_in"
        ]

        total_sent = sum(s.get("invoice_amount_sats", 0) for s in loop_out_completed)
        total_received = sum(s.get("onchain_amount_sats", 0) for s in loop_out_completed)
        total_cost = sum(s.get("total_cost_sats", 0) for s in loop_out_completed)
        return {
            "swaps": swaps,
            "totals": {
                "count": len(swaps),
                "completed": len(loop_out_completed) + len(loop_in_completed),
                "total_sent": total_sent,
                "total_received": total_received,
                "total_cost": total_cost,
                "avg_cost_ppm": int(total_cost * 1_000_000 / total_sent) if total_sent else 0,
                "loop_in_completed": len(loop_in_completed),
                "loop_in_lightning_received_sats": sum(s.get("invoice_amount_sats", 0) for s in loop_in_completed),
                "loop_in_onchain_sent_sats": sum(s.get("onchain_amount_sats", 0) for s in loop_in_completed),
            }
        }
