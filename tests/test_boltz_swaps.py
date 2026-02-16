import sqlite3
from unittest.mock import MagicMock

from modules.boltz_swaps import BoltzSwapManager


class _TestDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row

    def _get_connection(self):
        return self.conn


def _make_manager():
    db = _TestDB()
    plugin = MagicMock()
    plugin.rpc = MagicMock()
    plugin.rpc.getinfo.return_value = {"id": "02" + "1" * 64}
    manager = BoltzSwapManager(db, plugin, config={})
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
