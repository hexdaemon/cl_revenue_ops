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
from urllib.parse import urlparse
from typing import Dict, Any, Optional, Tuple, List

try:
    import urllib.request as _urlreq
    import urllib.error as _urlerr
except Exception:
    _urlreq = None

DEFAULT_BOLTZ_API = os.environ.get("BOLTZ_API", "https://api.boltz.exchange/v2")


class BoltzSwapManager:
    DEFAULT_LOOP_IN_MAX_SATS = 10_000_000
    DEFAULT_LOOP_IN_DAILY_CAP_SATS = 25_000_000
    DEFAULT_LOOP_IN_MIN_CONF = 1

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

    def _cfg_bool(self, key: str, default: bool) -> bool:
        value: Any = default
        if isinstance(self.config, dict):
            value = self.config.get(key, default)
        else:
            value = getattr(self.config, key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)

    def _cfg_int(self, key: str, default: int) -> int:
        value: Any = default
        if isinstance(self.config, dict):
            value = self.config.get(key, default)
        else:
            value = getattr(self.config, key, default)
        try:
            return int(value)
        except Exception:
            return default

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
                error TEXT,
                destination_validated INTEGER NOT NULL DEFAULT 0,
                destination_validation_note TEXT,
                auto_funding_status TEXT,
                auto_funding_txid TEXT,
                auto_funding_error TEXT,
                auto_funding_amount_sats INTEGER,
                auto_funding_min_conf INTEGER,
                auto_funding_updated_at INTEGER
            )
        """)
        # Keep existing installations forward-compatible with new columns.
        self._ensure_columns(conn, {
            "swap_type": "TEXT NOT NULL DEFAULT 'loop_out'",
            "target_channel_id": "TEXT",
            "target_peer_id": "TEXT",
            "bolt11_invoice": "TEXT",
            "destination_validated": "INTEGER NOT NULL DEFAULT 0",
            "destination_validation_note": "TEXT",
            "auto_funding_status": "TEXT",
            "auto_funding_txid": "TEXT",
            "auto_funding_error": "TEXT",
            "auto_funding_amount_sats": "INTEGER",
            "auto_funding_min_conf": "INTEGER",
            "auto_funding_updated_at": "INTEGER",
        })
        conn.execute("CREATE INDEX IF NOT EXISTS idx_boltz_swaps_status ON boltz_swaps(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_boltz_swaps_created ON boltz_swaps(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_boltz_swaps_auto_funding_status ON boltz_swaps(auto_funding_status)")

        # Audit trail for operational and safety decisions.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS boltz_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                swap_id TEXT,
                event_type TEXT NOT NULL,
                level TEXT NOT NULL DEFAULT 'info',
                message TEXT NOT NULL,
                details_json TEXT,
                created_at INTEGER NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_boltz_audit_swap_created ON boltz_audit_log(swap_id, created_at)")

        # Ledger of loop-in auto-funding transactions and blocked attempts.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS boltz_funding_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                swap_id TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                status TEXT NOT NULL,
                txid TEXT,
                min_conf INTEGER,
                destination TEXT,
                note TEXT,
                created_at INTEGER NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_boltz_ledger_created ON boltz_funding_ledger(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_boltz_ledger_swap ON boltz_funding_ledger(swap_id, created_at)")

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
            "error", "destination_validated", "destination_validation_note",
            "auto_funding_status", "auto_funding_txid", "auto_funding_error",
            "auto_funding_amount_sats", "auto_funding_min_conf", "auto_funding_updated_at",
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

    def _record_audit_event(
        self,
        event_type: str,
        message: str,
        swap_id: Optional[str] = None,
        level: str = "info",
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        conn = self.db._get_connection()
        now = self._now_ts()
        details_json = None
        if details is not None:
            try:
                details_json = json.dumps(details, sort_keys=True, separators=(",", ":"))
            except Exception:
                details_json = json.dumps({"details": str(details)})
        conn.execute(
            """
            INSERT INTO boltz_audit_log
            (swap_id, event_type, level, message, details_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (swap_id, event_type, level, message, details_json, now)
        )
        self._log(f"{event_type}: {message} (swap_id={swap_id})", level=level)

    def _record_funding_ledger(
        self,
        swap_id: str,
        amount_sats: int,
        status: str,
        txid: Optional[str] = None,
        min_conf: Optional[int] = None,
        destination: Optional[str] = None,
        note: Optional[str] = None,
    ) -> None:
        conn = self.db._get_connection()
        conn.execute(
            """
            INSERT INTO boltz_funding_ledger
            (swap_id, amount_sats, status, txid, min_conf, destination, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (swap_id, int(amount_sats), status, txid, min_conf, destination, note, self._now_ts())
        )

    def _get_recent_swap_audit_events(self, swap_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        conn = self.db._get_connection()
        rows = conn.execute(
            """
            SELECT id, swap_id, event_type, level, message, details_json, created_at
            FROM boltz_audit_log
            WHERE swap_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (swap_id, int(limit))
        ).fetchall()
        events = []
        for row in rows:
            ev = dict(row)
            if ev.get("details_json"):
                try:
                    ev["details"] = json.loads(ev["details_json"])
                except Exception:
                    ev["details"] = {"raw": ev["details_json"]}
            else:
                ev["details"] = None
            events.append(ev)
        return events

    def _get_latest_ledger_entry(self, swap_id: str) -> Optional[Dict[str, Any]]:
        conn = self.db._get_connection()
        row = conn.execute(
            """
            SELECT *
            FROM boltz_funding_ledger
            WHERE swap_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (swap_id,)
        ).fetchone()
        return dict(row) if row else None

    def _get_daily_loop_in_funded_sats(self) -> int:
        conn = self.db._get_connection()
        since = self._now_ts() - 86400
        row = conn.execute(
            """
            SELECT COALESCE(SUM(amount_sats), 0) AS total
            FROM boltz_funding_ledger
            WHERE created_at >= ? AND status = 'broadcast'
            """,
            (since,)
        ).fetchone()
        return int(row["total"]) if row and row["total"] is not None else 0

    def _get_loop_in_ledger_totals(self) -> Dict[str, int]:
        conn = self.db._get_connection()
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS count,
                COALESCE(SUM(amount_sats), 0) AS total
            FROM boltz_funding_ledger
            WHERE status = 'broadcast'
            """
        ).fetchone()
        return {
            "count": int(row["count"]) if row else 0,
            "total_sats": int(row["total"]) if row and row["total"] is not None else 0,
        }

    def _parse_bip21_address(self, bip21: Optional[str]) -> Optional[str]:
        if not bip21 or not isinstance(bip21, str):
            return None
        if not bip21.lower().startswith("bitcoin:"):
            return None
        try:
            parsed = urlparse(bip21)
            return parsed.path or None
        except Exception:
            return None

    def _validate_boltz_funding_destination(
        self,
        swap: Dict[str, Any],
        funding_address: Optional[str]
    ) -> Tuple[bool, str]:
        if not funding_address:
            return False, "Boltz swap missing funding address"

        addr = swap.get("address")
        lockup = swap.get("lockupAddress")
        candidates = [v for v in (addr, lockup) if isinstance(v, str) and v.strip()]
        if not candidates:
            return False, "Boltz swap did not return an address/lockupAddress"

        unique = {c.strip() for c in candidates}
        if len(unique) > 1:
            return False, "Boltz response address mismatch (address vs lockupAddress)"
        if funding_address.strip() not in unique:
            return False, "Funding address is not the Boltz-provided destination"

        bip21_addr = self._parse_bip21_address(swap.get("bip21"))
        if bip21_addr and bip21_addr.strip() != funding_address.strip():
            return False, "Boltz bip21 address mismatch"

        return True, "validated against Boltz swap response"

    def _auto_funding_runtime_status(self) -> Dict[str, Any]:
        enabled = self._cfg_bool("revenue_boltz_auto", True)
        per_swap_cap = self._cfg_int("boltz_loop_in_max_sats", self.DEFAULT_LOOP_IN_MAX_SATS)
        daily_cap = self._cfg_int("boltz_loop_in_daily_cap_sats", self.DEFAULT_LOOP_IN_DAILY_CAP_SATS)
        daily_funded = self._get_daily_loop_in_funded_sats()
        return {
            "enabled": enabled,
            "per_swap_cap_sats": per_swap_cap,
            "daily_cap_sats": daily_cap,
            "daily_funded_sats": daily_funded,
            "daily_remaining_sats": max(0, daily_cap - daily_funded),
            "min_confirmations": self._cfg_int("boltz_loop_in_min_confirmations", self.DEFAULT_LOOP_IN_MIN_CONF),
        }

    def _build_swap_auto_funding_view(self, swap: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not swap:
            return {"runtime": self._auto_funding_runtime_status(), "swap": None}
        return {
            "runtime": self._auto_funding_runtime_status(),
            "swap": {
                "destination_validated": bool(swap.get("destination_validated")),
                "destination_validation_note": swap.get("destination_validation_note"),
                "status": swap.get("auto_funding_status"),
                "txid": swap.get("auto_funding_txid"),
                "error": swap.get("auto_funding_error"),
                "amount_sats": swap.get("auto_funding_amount_sats"),
                "min_conf": swap.get("auto_funding_min_conf"),
                "updated_at": swap.get("auto_funding_updated_at"),
                "ledger_latest": self._get_latest_ledger_entry(swap.get("id")),
            },
        }

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
            "pair_hash": btc_pair.get("hash", ""),
            "auto_funding": self._auto_funding_runtime_status(),
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

    def _auto_fund_loop_in_swap(self, rec: Dict[str, Any]) -> Dict[str, Any]:
        swap_id = rec.get("id")
        amount_sats = int(rec.get("onchain_amount_sats") or 0)
        destination = rec.get("address")
        min_conf = max(1, self._cfg_int("boltz_loop_in_min_confirmations", self.DEFAULT_LOOP_IN_MIN_CONF))
        rec["auto_funding_min_conf"] = min_conf
        rec["auto_funding_amount_sats"] = amount_sats
        rec["auto_funding_updated_at"] = self._now_ts()

        if not self._cfg_bool("revenue_boltz_auto", True):
            rec["auto_funding_status"] = "disabled"
            rec["auto_funding_error"] = "automatic funding disabled by revenue_boltz_auto"
            self._record_swap(rec)
            self._record_audit_event(
                "loop_in_auto_funding_disabled",
                "Auto-funding skipped by kill switch",
                swap_id=swap_id,
                details={"amount_sats": amount_sats}
            )
            self._record_funding_ledger(
                swap_id=swap_id,
                amount_sats=amount_sats,
                status="disabled",
                min_conf=min_conf,
                destination=destination,
                note="kill-switch revenue_boltz_auto=false"
            )
            return {"funded": False, "status": "disabled", "reason": rec["auto_funding_error"]}

        per_swap_cap = self._cfg_int("boltz_loop_in_max_sats", self.DEFAULT_LOOP_IN_MAX_SATS)
        if amount_sats > per_swap_cap:
            rec["auto_funding_status"] = "blocked_per_swap_cap"
            rec["auto_funding_error"] = f"amount {amount_sats} exceeds per-swap cap {per_swap_cap}"
            self._record_swap(rec)
            self._record_audit_event(
                "loop_in_auto_funding_blocked",
                "Per-swap cap exceeded",
                swap_id=swap_id,
                level="warn",
                details={"amount_sats": amount_sats, "per_swap_cap_sats": per_swap_cap}
            )
            self._record_funding_ledger(
                swap_id=swap_id,
                amount_sats=amount_sats,
                status="blocked_per_swap_cap",
                min_conf=min_conf,
                destination=destination,
                note=rec["auto_funding_error"]
            )
            return {"funded": False, "status": "blocked_per_swap_cap", "reason": rec["auto_funding_error"]}

        daily_cap = self._cfg_int("boltz_loop_in_daily_cap_sats", self.DEFAULT_LOOP_IN_DAILY_CAP_SATS)
        daily_total = self._get_daily_loop_in_funded_sats()
        if daily_total + amount_sats > daily_cap:
            rec["auto_funding_status"] = "blocked_daily_cap"
            rec["auto_funding_error"] = (
                f"daily cap exceeded: {daily_total} + {amount_sats} > {daily_cap}"
            )
            self._record_swap(rec)
            self._record_audit_event(
                "loop_in_auto_funding_blocked",
                "Daily cap exceeded",
                swap_id=swap_id,
                level="warn",
                details={
                    "daily_funded_sats": daily_total,
                    "attempt_amount_sats": amount_sats,
                    "daily_cap_sats": daily_cap
                }
            )
            self._record_funding_ledger(
                swap_id=swap_id,
                amount_sats=amount_sats,
                status="blocked_daily_cap",
                min_conf=min_conf,
                destination=destination,
                note=rec["auto_funding_error"]
            )
            return {"funded": False, "status": "blocked_daily_cap", "reason": rec["auto_funding_error"]}

        if self._cfg_bool("dry_run", False):
            rec["auto_funding_status"] = "dry_run"
            rec["auto_funding_error"] = "dry_run=true (not broadcasting on-chain funding tx)"
            self._record_swap(rec)
            self._record_audit_event(
                "loop_in_auto_funding_dry_run",
                "Dry run mode prevented on-chain funding",
                swap_id=swap_id,
                details={"amount_sats": amount_sats, "min_conf": min_conf}
            )
            self._record_funding_ledger(
                swap_id=swap_id,
                amount_sats=amount_sats,
                status="dry_run",
                min_conf=min_conf,
                destination=destination,
                note=rec["auto_funding_error"]
            )
            return {"funded": False, "status": "dry_run", "reason": rec["auto_funding_error"]}

        withdraw_result: Dict[str, Any]
        try:
            try:
                withdraw_result = self.rpc.withdraw(
                    destination=destination,
                    satoshi=amount_sats,
                    minconf=min_conf
                )
            except Exception:
                # Backward compatibility for CLN variants accepting sat strings.
                withdraw_result = self.rpc.withdraw(
                    destination=destination,
                    satoshi=f"{amount_sats}sat",
                    minconf=min_conf
                )
        except Exception as e:
            rec["auto_funding_status"] = "withdraw_failed"
            rec["auto_funding_error"] = str(e)
            rec["auto_funding_updated_at"] = self._now_ts()
            self._record_swap(rec)
            self._record_funding_ledger(
                swap_id=swap_id,
                amount_sats=amount_sats,
                status="withdraw_failed",
                min_conf=min_conf,
                destination=destination,
                note=str(e)
            )
            self._record_audit_event(
                "loop_in_auto_funding_failed",
                "CLN wallet withdraw failed",
                swap_id=swap_id,
                level="warn",
                details={"error": str(e), "amount_sats": amount_sats, "min_conf": min_conf}
            )
            return {"funded": False, "status": "withdraw_failed", "reason": str(e)}

        txid = (
            withdraw_result.get("txid")
            or withdraw_result.get("txid_hex")
            or withdraw_result.get("id")
        )
        rec["auto_funding_status"] = "broadcast"
        rec["auto_funding_txid"] = txid
        rec["auto_funding_error"] = None
        rec["auto_funding_updated_at"] = self._now_ts()
        self._record_swap(rec)
        self._record_funding_ledger(
            swap_id=swap_id,
            amount_sats=amount_sats,
            status="broadcast",
            txid=txid,
            min_conf=min_conf,
            destination=destination,
            note="CLN withdraw broadcast"
        )
        self._record_audit_event(
            "loop_in_auto_funding_broadcast",
            "Auto-funded loop-in with CLN wallet",
            swap_id=swap_id,
            details={"txid": txid, "amount_sats": amount_sats, "min_conf": min_conf}
        )
        return {"funded": True, "status": "broadcast", "txid": txid, "withdraw_result": withdraw_result}

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
            return {"dry_run": True, "quote": quote, "auto_funding": self._auto_funding_runtime_status()}

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
            "destination_validated": 0,
            "destination_validation_note": "not_applicable_loop_out",
            "auto_funding_status": "not_applicable",
            "auto_funding_txid": None,
            "auto_funding_error": None,
            "auto_funding_amount_sats": None,
            "auto_funding_min_conf": None,
            "auto_funding_updated_at": None,
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
            return {
                "error": f"payment failed: {e}",
                "swap_id": swap_id,
                "auto_funding": self._auto_funding_runtime_status()
            }

        # Wait for lockup
        lockup = self._wait_for_lockup(swap_id)
        if lockup.get("error"):
            rec["status"] = "timeout_lockup"
            rec["error"] = lockup.get("error")
            rec["updated_at"] = self._now_ts()
            self._record_swap(rec)
            return {
                "error": lockup.get("error"),
                "swap_id": swap_id,
                "auto_funding": self._auto_funding_runtime_status()
            }
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
                "auto_funding": self._auto_funding_runtime_status(),
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
                "auto_funding": self._auto_funding_runtime_status(),
            }

    def loop_in(
        self,
        amount_sats: int,
        channel_id: Optional[str] = None,
        peer_id: Optional[str] = None
    ) -> Dict[str, Any]:
        runtime_auto = self._auto_funding_runtime_status()
        if amount_sats < 25000:
            return {"error": "amount_sats must be >= 25,000", "auto_funding": runtime_auto}

        per_swap_cap = self._cfg_int("boltz_loop_in_max_sats", self.DEFAULT_LOOP_IN_MAX_SATS)
        if amount_sats > per_swap_cap:
            return {
                "error": f"amount_sats exceeds configured per-swap cap ({per_swap_cap})",
                "auto_funding": runtime_auto
            }

        hint_res = self._collect_invoice_hints(channel_id=channel_id, peer_id=peer_id)
        if hint_res.get("error"):
            hint_res["auto_funding"] = runtime_auto
            return hint_res
        hints = hint_res.get("hints", [])

        limits_info = self._get_submarine_limits()
        if limits_info.get("error"):
            limits_info["auto_funding"] = runtime_auto
            return limits_info

        limits = limits_info.get("limits", {})
        if amount_sats < limits.get("minimal", 25000):
            return {"error": f"amount_sats below minimum {limits.get('minimal')}", "auto_funding": runtime_auto}
        if amount_sats > limits.get("maximal", 25000000):
            return {"error": f"amount_sats above maximum {limits.get('maximal')}", "auto_funding": runtime_auto}

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
            return {"error": f"invoice returned unexpected response: {invoice_res}", "auto_funding": runtime_auto}

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
            return {"error": f"Boltz loop-in creation failed: {swap}", "auto_funding": runtime_auto}

        swap_id = swap["id"]
        expected_onchain = int(swap.get("expectedAmount", amount_sats))
        timeout_block = int(swap.get("timeoutBlockHeight", 0))
        funding_address = swap.get("address") or swap.get("lockupAddress")
        status = swap.get("status", "created")
        total_cost = max(0, expected_onchain - amount_sats)
        destination_ok, destination_note = self._validate_boltz_funding_destination(swap, funding_address)

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
            "destination_validated": 1 if destination_ok else 0,
            "destination_validation_note": destination_note,
            "auto_funding_status": "pending",
            "auto_funding_txid": None,
            "auto_funding_error": None,
            "auto_funding_amount_sats": expected_onchain,
            "auto_funding_min_conf": self._cfg_int(
                "boltz_loop_in_min_confirmations",
                self.DEFAULT_LOOP_IN_MIN_CONF
            ),
            "auto_funding_updated_at": self._now_ts(),
        }
        self._record_swap(rec)
        self._record_audit_event(
            "loop_in_created",
            "Loop-in swap created; preparing auto-funding",
            swap_id=swap_id,
            details={
                "invoice_amount_sats": amount_sats,
                "expected_onchain_sats": expected_onchain,
                "channel_id": channel_id,
                "peer_id": peer_id
            }
        )

        auto_result: Dict[str, Any]
        if not destination_ok:
            rec["auto_funding_status"] = "destination_invalid"
            rec["auto_funding_error"] = destination_note
            rec["error"] = destination_note
            rec["auto_funding_updated_at"] = self._now_ts()
            self._record_swap(rec)
            self._record_audit_event(
                "loop_in_destination_invalid",
                "Boltz destination validation failed",
                swap_id=swap_id,
                level="warn",
                details={"validation_note": destination_note, "funding_address": funding_address}
            )
            self._record_funding_ledger(
                swap_id=swap_id,
                amount_sats=expected_onchain,
                status="destination_invalid",
                min_conf=rec["auto_funding_min_conf"],
                destination=funding_address,
                note=destination_note
            )
            auto_result = {
                "funded": False,
                "status": "destination_invalid",
                "reason": destination_note,
            }
        else:
            try:
                auto_result = self._auto_fund_loop_in_swap(rec)
            except Exception as e:
                rec["auto_funding_status"] = "withdraw_failed"
                rec["auto_funding_error"] = str(e)
                rec["error"] = str(e)
                rec["auto_funding_updated_at"] = self._now_ts()
                self._record_swap(rec)
                self._record_funding_ledger(
                    swap_id=swap_id,
                    amount_sats=expected_onchain,
                    status="withdraw_failed",
                    min_conf=rec.get("auto_funding_min_conf"),
                    destination=funding_address,
                    note=str(e)
                )
                self._record_audit_event(
                    "loop_in_auto_funding_failed",
                    "CLN wallet auto-funding failed",
                    swap_id=swap_id,
                    level="warn",
                    details={"error": str(e)}
                )
                auto_result = {"funded": False, "status": "withdraw_failed", "reason": str(e)}

        rec = self._get_swap(swap_id) or rec

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
            "auto_funding": {
                **self._build_swap_auto_funding_view(rec),
                "result": auto_result,
            },
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

        return {
            "local": local,
            "boltz": remote,
            "auto_funding": self._build_swap_auto_funding_view(local),
            "audit_events": self._get_recent_swap_audit_events(swap_id, limit=10),
        }

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
        ledger_totals = self._get_loop_in_ledger_totals()
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
                "loop_in_auto_funding_tx_count": ledger_totals["count"],
                "loop_in_auto_funded_sats": ledger_totals["total_sats"],
            },
            "auto_funding": self._auto_funding_runtime_status(),
        }
