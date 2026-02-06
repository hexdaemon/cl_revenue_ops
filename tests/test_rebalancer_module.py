import time
from unittest.mock import MagicMock


def _candidate(
    *,
    source_candidates=None,
    to_channel="222x333x0",
    primary_source_peer_id="02" + "a" * 64,
    to_peer_id="02" + "b" * 64,
    amount_sats=50000,
):
    from modules.rebalancer import RebalanceCandidate

    if source_candidates is None:
        source_candidates = ["111x222x0"]

    amt_msat = amount_sats * 1000
    return RebalanceCandidate(
        source_candidates=source_candidates,
        to_channel=to_channel,
        primary_source_peer_id=primary_source_peer_id,
        to_peer_id=to_peer_id,
        amount_sats=amount_sats,
        amount_msat=amt_msat,
        outbound_fee_ppm=1000,
        inbound_fee_ppm=100,
        source_fee_ppm=100,
        weighted_opp_cost_ppm=100,
        spread_ppm=800,
        max_budget_sats=10,
        max_budget_msat=10_000,
        max_fee_ppm=2000,
        expected_profit_sats=1,
        liquidity_ratio=0.1,
        dest_flow_state="balanced",
        dest_turnover_rate=0.0,
        source_turnover_rate=0.0,
    )


class TestExecuteRebalanceBudgetReservationLifecycle:
    def test_execute_rebalance_dry_run_does_not_reserve_budget_and_clears_pending(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import EVRebalancer

        cfg = Config(dry_run=True, enable_proportional_budget=False)
        clboss = MagicMock()
        clboss.ensure_unmanaged_for_channel = MagicMock(return_value=True)

        r = EVRebalancer(mock_plugin, cfg, mock_database, clboss)
        r.job_manager.start_job = MagicMock(return_value={"success": True})

        mock_database.record_rebalance = MagicMock(return_value=123)
        mock_database.update_rebalance_result = MagicMock()
        mock_database.reserve_budget = MagicMock(return_value=(True, 9999))

        cand = _candidate()
        res = r.execute_rebalance(cand)

        assert res["success"] is True
        mock_database.reserve_budget.assert_not_called()
        assert cand.to_channel not in r._pending

    def test_execute_rebalance_releases_budget_on_start_job_failure(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import EVRebalancer

        cfg = Config(dry_run=False, enable_proportional_budget=False)
        clboss = MagicMock()
        clboss.ensure_unmanaged_for_channel = MagicMock(return_value=True)

        r = EVRebalancer(mock_plugin, cfg, mock_database, clboss)
        r.job_manager.start_job = MagicMock(return_value={"success": False, "error": "boom"})

        mock_database.record_rebalance = MagicMock(return_value=456)
        mock_database.update_rebalance_result = MagicMock()
        mock_database.reserve_budget = MagicMock(return_value=(True, 9999))
        mock_database.release_budget_reservation = MagicMock(return_value=True)

        cand = _candidate()
        res = r.execute_rebalance(cand, enforce_budget=True)

        assert res["success"] is False
        mock_database.reserve_budget.assert_called_once()
        mock_database.release_budget_reservation.assert_called_once_with(456)


class TestMultiSourceClbossUnmanage:
    def test_execute_rebalance_unmanages_each_source_with_its_peer_id(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import EVRebalancer

        cfg = Config(dry_run=True, enable_proportional_budget=False)
        clboss = MagicMock()
        clboss.ensure_unmanaged_for_channel = MagicMock(return_value=True)

        r = EVRebalancer(mock_plugin, cfg, mock_database, clboss)

        mock_database.record_rebalance = MagicMock(return_value=1)
        mock_database.update_rebalance_result = MagicMock()

        cand = _candidate(source_candidates=["111x222x0", "333x444x0"])
        cand.source_candidate_peer_ids = ["02" + "c" * 64, "02" + "d" * 64]

        r.execute_rebalance(cand)

        # First two calls are for the sources; last is for destination.
        assert clboss.ensure_unmanaged_for_channel.call_count >= 3
        src_calls = clboss.ensure_unmanaged_for_channel.call_args_list[:2]
        assert src_calls[0][0][0] == "111x222x0"
        assert src_calls[0][0][1] == cand.source_candidate_peer_ids[0]
        assert src_calls[1][0][0] == "333x444x0"
        assert src_calls[1][0][1] == cand.source_candidate_peer_ids[1]


class TestLastHopFeeUnits:
    def test_get_last_hop_fee_converts_base_fee_to_ppm(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import EVRebalancer

        cfg = Config(dry_run=True)
        clboss = MagicMock()
        r = EVRebalancer(mock_plugin, cfg, mock_database, clboss)

        peer_id = "02" + "e" * 64
        our_id = "02" + "f" * 64

        mock_plugin.rpc.getinfo.return_value = {"id": our_id}
        mock_plugin.rpc.listchannels.return_value = {
            "channels": [
                {
                    "destination": our_id,
                    "fee_per_millionth": 100,
                    "base_fee_millisatoshi": 1000,  # 1 sat
                }
            ]
        }

        # At 100k sats (100,000,000 msat), a 1 sat base fee ~= 10 ppm.
        ppm = r._get_last_hop_fee(peer_id, amount_msat=100_000_000)
        assert ppm == 110


class TestManualRebalanceBudgetBypass:
    def test_manual_rebalance_does_not_reserve_budget(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import EVRebalancer

        cfg = Config(dry_run=False, enable_proportional_budget=False)
        clboss = MagicMock()
        clboss.ensure_unmanaged_for_channel = MagicMock(return_value=True)

        r = EVRebalancer(mock_plugin, cfg, mock_database, clboss)
        r._check_capital_controls = MagicMock(return_value=True)
        r._estimate_inbound_fee = MagicMock(return_value=0)
        r._get_channels_with_balances = MagicMock(return_value={
            "111x222x0": {"peer_id": "02" + "a" * 64, "fee_ppm": 10, "spendable_sats": 1_000_000, "capacity": 2_000_000},
            "222x333x0": {"peer_id": "02" + "b" * 64, "fee_ppm": 20, "spendable_sats": 1000, "capacity": 2_000_000},
        })

        r.job_manager.start_job = MagicMock(return_value={"success": False, "error": "boom"})

        mock_database.record_rebalance = MagicMock(return_value=999)
        mock_database.update_rebalance_result = MagicMock()
        mock_database.reserve_budget = MagicMock(return_value=(True, 9999))

        r.manual_rebalance("111x222x0", "222x333x0", 50_000, max_fee_sats=10, force=True)
        mock_database.reserve_budget.assert_not_called()


class TestJobMonitorPrefersSlingStats:
    def test_monitor_jobs_treats_success_count_as_success_even_if_balance_delta_zero(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager, ActiveJob, JobStatus

        cfg = Config(dry_run=False, enable_proportional_budget=False)
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        target_scid = "123x456x0"
        candidate = _candidate(to_channel=target_scid, amount_sats=50_000)
        candidate.max_budget_msat = 100_000
        candidate.max_budget_sats = 100
        candidate.expected_profit_sats = 0

        job = ActiveJob(
            scid=target_scid,
            scid_normalized=target_scid,
            source_candidates=["111x222x0"],
            start_time=int(time.time()),
            candidate=candidate,
            rebalance_id=1,
            target_amount_sats=50_000,
            initial_local_sats=100,
            max_fee_ppm=2000,
            status=JobStatus.RUNNING,
        )
        jm._active_jobs[target_scid] = job

        # Balance delta is zero.
        mock_plugin.rpc.listfunds.return_value = {
            "channels": [
                {"short_channel_id": target_scid, "our_amount_msat": 100_000},
            ]
        }

        jm._get_sling_stats = MagicMock(return_value={
            target_scid: {
                "scid": target_scid,
                "success_count": 1,
                "fee_total_msat": 1000,
            }
        })

        summary = jm.monitor_jobs()
        assert summary["completed"] == 1
        assert jm.active_job_count == 0
