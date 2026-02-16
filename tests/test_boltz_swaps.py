import sqlite3
from types import SimpleNamespace
from unittest.mock import MagicMock

from modules.boltz_swaps import BoltzSwapManager


class _TestDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row

    def _get_connection(self):
        return self.conn


def _make_manager(config=None):
    db = _TestDB()
    plugin = MagicMock()
    plugin.rpc = MagicMock()
    plugin.rpc.getinfo.return_value = {"id": "02" + "1" * 64}
    if config is None:
        config = {"revenue_boltz_auto": False}
    manager = BoltzSwapManager(db, plugin, config=config)
    return manager, plugin


def test_loop_in_records_swap_with_channel_hint():
    manager, plugin = _make_manager()
    rpc = plugin.rpc

    rpc.listpeerchannels.return_value = {
        "channels": [{"short_channel_id": "123x1x0", "channel_id": "1", "state": "CHANNELD_NORMAL"}]
    }
    rpc.invoice.return_value = {
        "bolt11": "lnbc1testinvoice",
        "payment_hash": "ab" * 32,
    }

    manager._http_get = MagicMock(return_value={
        "BTC": {"BTC": {"limits": {"minimal": 25000, "maximal": 25000000}, "hash": "pairhash"}}
    })
    manager._http_post = MagicMock(return_value={
        "id": "swap-loop-in-1",
        "status": "swap.created",
        "address": "bc1qloopinaddress",
        "expectedAmount": 101000,
        "timeoutBlockHeight": 1234,
    })
    manager._generate_secp256k1_keypair = MagicMock(return_value=("11" * 32, "02" + "22" * 32))

    res = manager.loop_in(100000, channel_id="123x1x0")

    assert res["swap_id"] == "swap-loop-in-1"
    assert res["status"] == "awaiting_onchain_funding"
    assert res["boltz_status"] == "swap.created"

    row = manager.db._get_connection().execute(
        "SELECT * FROM boltz_swaps WHERE id = ?",
        ("swap-loop-in-1",)
    ).fetchone()
    assert row is not None
    assert row["swap_type"] == "loop_in"
    assert row["target_channel_id"] == "123x1x0"
    assert row["target_peer_id"] is None
    assert row["status"] == "swap.created"

    kwargs = rpc.invoice.call_args.kwargs
    assert kwargs["exposeprivatechannels"] == ["123x1x0"]


def test_loop_in_resolves_peer_to_channel_hints():
    manager, plugin = _make_manager()
    rpc = plugin.rpc

    rpc.listpeerchannels.return_value = {
        "channels": [
            {"short_channel_id": "900x2x1", "state": "CHANNELD_NORMAL"},
            {"short_channel_id": None, "state": "CHANNELD_NORMAL"},
            {"short_channel_id": "901x2x1", "state": "ONCHAIN"},
        ]
    }
    rpc.invoice.return_value = {
        "bolt11": "lnbc1testinvoice2",
        "payment_hash": "cd" * 32,
    }

    manager._http_get = MagicMock(return_value={
        "BTC": {"BTC": {"limits": {"minimal": 25000, "maximal": 25000000}, "hash": "pairhash"}}
    })
    manager._http_post = MagicMock(return_value={
        "id": "swap-loop-in-2",
        "status": "swap.created",
        "address": "bc1qloopinaddress2",
        "expectedAmount": 202000,
        "timeoutBlockHeight": 5678,
    })
    manager._generate_secp256k1_keypair = MagicMock(return_value=("33" * 32, "03" + "44" * 32))

    res = manager.loop_in(200000, peer_id="02" + "f" * 64)

    assert res["swap_id"] == "swap-loop-in-2"

    kwargs = rpc.invoice.call_args.kwargs
    assert kwargs["exposeprivatechannels"] == ["900x2x1"]


def test_loop_in_rejects_channel_and_peer_together():
    manager, _ = _make_manager()
    res = manager.loop_in(100000, channel_id="1x1x1", peer_id="02" + "a" * 64)
    assert "error" in res


def test_loop_in_auto_funds_with_cln_wallet_and_updates_ledger():
    cfg = SimpleNamespace(
        revenue_boltz_auto=True,
        boltz_loop_in_max_sats=10_000_000,
        boltz_loop_in_daily_cap_sats=25_000_000,
        boltz_loop_in_min_confirmations=2,
        dry_run=False,
    )
    manager, plugin = _make_manager(config=cfg)
    rpc = plugin.rpc

    rpc.listpeerchannels.return_value = {
        "channels": [{"short_channel_id": "123x1x0", "channel_id": "1", "state": "CHANNELD_NORMAL"}]
    }
    rpc.invoice.return_value = {"bolt11": "lnbc1fundtest", "payment_hash": "11" * 32}
    rpc.withdraw.return_value = {"txid": "aa" * 32}

    manager._http_get = MagicMock(return_value={
        "BTC": {"BTC": {"limits": {"minimal": 25000, "maximal": 25000000}, "hash": "pairhash"}}
    })
    manager._http_post = MagicMock(return_value={
        "id": "swap-loop-in-auto-1",
        "status": "swap.created",
        "address": "bc1qautofunddest",
        "lockupAddress": "bc1qautofunddest",
        "bip21": "bitcoin:bc1qautofunddest?amount=0.001005",
        "expectedAmount": 100500,
        "timeoutBlockHeight": 1234,
    })
    manager._generate_secp256k1_keypair = MagicMock(return_value=("11" * 32, "02" + "22" * 32))

    res = manager.loop_in(100000, channel_id="123x1x0")

    assert res["swap_id"] == "swap-loop-in-auto-1"
    assert res["auto_funding"]["result"]["status"] == "broadcast"
    kwargs = rpc.withdraw.call_args.kwargs
    assert kwargs["destination"] == "bc1qautofunddest"
    assert kwargs["satoshi"] == 100500
    assert kwargs["minconf"] == 2

    row = manager.db._get_connection().execute(
        "SELECT auto_funding_status, auto_funding_txid, destination_validated FROM boltz_swaps WHERE id = ?",
        ("swap-loop-in-auto-1",)
    ).fetchone()
    assert row is not None
    assert row["auto_funding_status"] == "broadcast"
    assert row["auto_funding_txid"] == "aa" * 32
    assert row["destination_validated"] == 1

    ledger = manager.db._get_connection().execute(
        "SELECT status, amount_sats, txid FROM boltz_funding_ledger WHERE swap_id = ? ORDER BY id DESC LIMIT 1",
        ("swap-loop-in-auto-1",)
    ).fetchone()
    assert ledger is not None
    assert ledger["status"] == "broadcast"
    assert ledger["amount_sats"] == 100500
    assert ledger["txid"] == "aa" * 32


def test_loop_in_enforces_configured_per_swap_cap():
    cfg = SimpleNamespace(
        revenue_boltz_auto=True,
        boltz_loop_in_max_sats=100000,
        boltz_loop_in_daily_cap_sats=25_000_000,
        boltz_loop_in_min_confirmations=1,
        dry_run=False,
    )
    manager, plugin = _make_manager(config=cfg)

    res = manager.loop_in(200000, channel_id="123x1x0")

    assert "error" in res
    assert "per-swap cap" in res["error"]
    plugin.rpc.invoice.assert_not_called()
    plugin.rpc.withdraw.assert_not_called()


def test_loop_in_blocks_funding_when_destination_validation_fails():
    cfg = SimpleNamespace(
        revenue_boltz_auto=True,
        boltz_loop_in_max_sats=10_000_000,
        boltz_loop_in_daily_cap_sats=25_000_000,
        boltz_loop_in_min_confirmations=1,
        dry_run=False,
    )
    manager, plugin = _make_manager(config=cfg)
    rpc = plugin.rpc

    rpc.listpeerchannels.return_value = {
        "channels": [{"short_channel_id": "123x1x0", "channel_id": "1", "state": "CHANNELD_NORMAL"}]
    }
    rpc.invoice.return_value = {"bolt11": "lnbc1destcheck", "payment_hash": "22" * 32}

    manager._http_get = MagicMock(return_value={
        "BTC": {"BTC": {"limits": {"minimal": 25000, "maximal": 25000000}, "hash": "pairhash"}}
    })
    manager._http_post = MagicMock(return_value={
        "id": "swap-loop-in-dest-bad",
        "status": "swap.created",
        "address": "bc1qone",
        "lockupAddress": "bc1qtwo",
        "expectedAmount": 100000,
        "timeoutBlockHeight": 1234,
    })
    manager._generate_secp256k1_keypair = MagicMock(return_value=("11" * 32, "02" + "22" * 32))

    res = manager.loop_in(100000, channel_id="123x1x0")

    assert res["auto_funding"]["result"]["status"] == "destination_invalid"
    rpc.withdraw.assert_not_called()
    row = manager.db._get_connection().execute(
        "SELECT destination_validated, auto_funding_status FROM boltz_swaps WHERE id = ?",
        ("swap-loop-in-dest-bad",)
    ).fetchone()
    assert row is not None
    assert row["destination_validated"] == 0
    assert row["auto_funding_status"] == "destination_invalid"


def test_loop_in_blocks_on_daily_cap_after_previous_funding():
    cfg = SimpleNamespace(
        revenue_boltz_auto=True,
        boltz_loop_in_max_sats=10_000_000,
        boltz_loop_in_daily_cap_sats=150000,
        boltz_loop_in_min_confirmations=1,
        dry_run=False,
    )
    manager, plugin = _make_manager(config=cfg)
    rpc = plugin.rpc

    rpc.listpeerchannels.return_value = {
        "channels": [{"short_channel_id": "123x1x0", "channel_id": "1", "state": "CHANNELD_NORMAL"}]
    }
    rpc.invoice.side_effect = [
        {"bolt11": "lnbc1first", "payment_hash": "33" * 32},
        {"bolt11": "lnbc1second", "payment_hash": "44" * 32},
    ]
    rpc.withdraw.return_value = {"txid": "bb" * 32}

    manager._http_get = MagicMock(return_value={
        "BTC": {"BTC": {"limits": {"minimal": 25000, "maximal": 25000000}, "hash": "pairhash"}}
    })
    manager._http_post = MagicMock(side_effect=[
        {
            "id": "swap-loop-in-cap-1",
            "status": "swap.created",
            "address": "bc1qcapfund",
            "lockupAddress": "bc1qcapfund",
            "expectedAmount": 100000,
            "timeoutBlockHeight": 1000,
        },
        {
            "id": "swap-loop-in-cap-2",
            "status": "swap.created",
            "address": "bc1qcapfund",
            "lockupAddress": "bc1qcapfund",
            "expectedAmount": 60000,
            "timeoutBlockHeight": 1001,
        }
    ])
    manager._generate_secp256k1_keypair = MagicMock(return_value=("11" * 32, "02" + "22" * 32))

    first = manager.loop_in(100000, channel_id="123x1x0")
    second = manager.loop_in(60000, channel_id="123x1x0")

    assert first["auto_funding"]["result"]["status"] == "broadcast"
    assert second["auto_funding"]["result"]["status"] == "blocked_daily_cap"
    assert rpc.withdraw.call_count == 1

    rows = manager.db._get_connection().execute(
        "SELECT status FROM boltz_funding_ledger WHERE swap_id = ? ORDER BY id DESC",
        ("swap-loop-in-cap-2",)
    ).fetchall()
    assert rows
    assert rows[0]["status"] == "blocked_daily_cap"
