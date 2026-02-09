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


# =============================================================================
# Sling Integration Enhancement Tests
# =============================================================================


class TestParallelJobsParameter:
    """Change 1: Verify paralleljobs appears in sling-job RPC params."""

    def test_paralleljobs_passed_to_sling(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config(sling_parallel_jobs=3)
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        mock_plugin.rpc.listfunds.return_value = {
            "channels": [{"short_channel_id": "222x333x0", "our_amount_msat": 0}]
        }

        cand = _candidate()
        jm.start_job(cand, rebalance_id=1)

        # The first rpc.call should be sling-job
        sling_job_call = mock_plugin.rpc.call.call_args_list[0]
        assert sling_job_call[0][0] == "sling-job"
        params = sling_job_call[0][1]
        assert params["paralleljobs"] == 3


class TestFlowAwareDepletion:
    """Change 2: Verify depleteuptopercent varies by flow state."""

    def _start_job_with_flow(self, mock_plugin, mock_database, flow_state):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config(
            sling_deplete_pct_sink=0.10,
            sling_deplete_pct_source=0.35,
            sling_deplete_pct_balanced=0.20,
        )
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        mock_plugin.rpc.listfunds.return_value = {
            "channels": [{"short_channel_id": "222x333x0", "our_amount_msat": 0}]
        }

        cand = _candidate()
        cand.dest_flow_state = flow_state
        jm.start_job(cand, rebalance_id=1)

        sling_job_call = mock_plugin.rpc.call.call_args_list[0]
        return sling_job_call[0][1]

    def test_sink_depletion(self, mock_plugin, mock_database):
        params = self._start_job_with_flow(mock_plugin, mock_database, "sink")
        assert params["depleteuptopercent"] == 0.10

    def test_source_depletion(self, mock_plugin, mock_database):
        params = self._start_job_with_flow(mock_plugin, mock_database, "source")
        assert params["depleteuptopercent"] == 0.35

    def test_balanced_depletion(self, mock_plugin, mock_database):
        params = self._start_job_with_flow(mock_plugin, mock_database, "balanced")
        assert params["depleteuptopercent"] == 0.20


class TestPinnedStatsSchema:
    """Change 3: Verify _extract_success_amount_sats / _extract_success_count
    work with the successes_in_time_window nested format and fall back to
    legacy flat keys."""

    def test_success_amount_from_nested_schema(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        stats = {
            "successes_in_time_window": {
                "total_amount_sats": 500000,
                "total_rebalances": 3,
                "total_spent_sats": 50,
            }
        }
        assert jm._extract_success_amount_sats(stats) == 500000

    def test_success_amount_fallback_to_msat(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        stats = {"success_total_msat": 500000000}
        assert jm._extract_success_amount_sats(stats) == 500000

    def test_success_amount_fallback_to_sats(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        stats = {"success_total_sats": 250000}
        assert jm._extract_success_amount_sats(stats) == 250000

    def test_success_amount_empty_stats(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        assert jm._extract_success_amount_sats({}) is None
        assert jm._extract_success_amount_sats(None) is None

    def test_success_count_from_nested_schema(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        stats = {"successes_in_time_window": {"total_rebalances": 5}}
        assert jm._extract_success_count(stats) == 5

    def test_success_count_fallback(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        stats = {"success_count": 2}
        assert jm._extract_success_count(stats) == 2


class TestPerJobStats:
    """Change 3a: Verify _get_sling_stats calls per-scid stats for active jobs."""

    def test_per_scid_stats_preferred(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager, ActiveJob, JobStatus

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        scid = "123x456x0"
        cand = _candidate(to_channel=scid)
        job = ActiveJob(
            scid=scid,
            scid_normalized=scid,
            source_candidates=["111x222x0"],
            start_time=int(time.time()),
            candidate=cand,
            rebalance_id=1,
            target_amount_sats=50000,
            initial_local_sats=0,
            max_fee_ppm=2000,
            status=JobStatus.RUNNING,
        )
        jm._active_jobs[scid] = job

        # Mock per-scid call to return detailed stats
        per_scid_result = {
            "successes_in_time_window": {"total_amount_sats": 100000, "total_rebalances": 2},
            "failures_in_time_window": {"total_rebalances": 1},
        }
        mock_plugin.rpc.call.return_value = per_scid_result

        stats = jm._get_sling_stats()
        assert scid in stats
        assert stats[scid]["successes_in_time_window"]["total_amount_sats"] == 100000

        # Verify per-scid call was made with scid param
        mock_plugin.rpc.call.assert_called_with("sling-stats", {"scid": scid, "json": True})


class TestPushDirection:
    """Change 4: Verify direction='push' is passed to sling-job and target inverts."""

    def test_push_direction_passed_to_sling(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config(sling_target_balanced=0.50)
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        mock_plugin.rpc.listfunds.return_value = {
            "channels": [{"short_channel_id": "222x333x0", "our_amount_msat": 0}]
        }

        cand = _candidate()
        cand.direction = "push"
        jm.start_job(cand, rebalance_id=1)

        sling_job_call = mock_plugin.rpc.call.call_args_list[0]
        params = sling_job_call[0][1]
        assert params["direction"] == "push"

    def test_push_direction_inverts_target(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config(sling_target_balanced=0.50)
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        mock_plugin.rpc.listfunds.return_value = {
            "channels": [{"short_channel_id": "222x333x0", "our_amount_msat": 0}]
        }

        cand = _candidate()
        cand.direction = "push"
        cand.dest_flow_state = "balanced"
        jm.start_job(cand, rebalance_id=1)

        sling_job_call = mock_plugin.rpc.call.call_args_list[0]
        params = sling_job_call[0][1]
        # Push: target = 1.0 - balanced(0.50) = 0.50
        assert params["target"] == 0.50

    def test_push_direction_source_flow_inverts(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config(sling_target_source=0.65)
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        mock_plugin.rpc.listfunds.return_value = {
            "channels": [{"short_channel_id": "222x333x0", "our_amount_msat": 0}]
        }

        cand = _candidate()
        cand.direction = "push"
        cand.dest_flow_state = "source"
        jm.start_job(cand, rebalance_id=1)

        sling_job_call = mock_plugin.rpc.call.call_args_list[0]
        params = sling_job_call[0][1]
        # Push + source: target = 1.0 - 0.65 = 0.35
        assert params["target"] == 0.35

    def test_pull_direction_default(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        mock_plugin.rpc.listfunds.return_value = {
            "channels": [{"short_channel_id": "222x333x0", "our_amount_msat": 0}]
        }

        cand = _candidate()
        # No direction set — defaults to "pull"
        jm.start_job(cand, rebalance_id=1)

        sling_job_call = mock_plugin.rpc.call.call_args_list[0]
        params = sling_job_call[0][1]
        assert params["direction"] == "pull"

    def test_direction_in_to_dict(self):
        cand = _candidate()
        assert cand.to_dict()["direction"] == "pull"
        cand.direction = "push"
        assert cand.to_dict()["direction"] == "push"


class TestSlingOnce:
    """Change 5: Verify execute_once calls sling-once RPC with correct params."""

    def test_execute_once_success(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        mock_plugin.rpc.call.return_value = {"status": "ok"}

        result = jm.execute_once(
            scid="123x456x0",
            direction="pull",
            amount=100000,
            maxppm=500,
            onceamount=200000,
        )

        assert result["success"] is True
        mock_plugin.rpc.call.assert_called_once_with("sling-once", {
            "scid": "123x456x0",
            "direction": "pull",
            "amount": 100000,
            "maxppm": 500,
            "onceamount": 200000,
        })

    def test_execute_once_with_candidates(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        mock_plugin.rpc.call.return_value = {"status": "ok"}

        result = jm.execute_once(
            scid="123:456:0",
            direction="push",
            amount=50000,
            maxppm=300,
            candidates=["111:222:0", "333:444:0"],
            outppm=200,
        )

        assert result["success"] is True
        call_args = mock_plugin.rpc.call.call_args[0][1]
        assert call_args["scid"] == "123x456x0"
        assert call_args["direction"] == "push"
        assert call_args["candidates"] == ["111x222x0", "333x444x0"]
        assert call_args["outppm"] == 200

    def test_execute_once_rounds_up_onceamount(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        mock_plugin.rpc.call.return_value = {"status": "ok"}

        # 150000 is not a multiple of 100000 → should round up to 200000
        jm.execute_once(
            scid="123x456x0", direction="pull",
            amount=100000, maxppm=500, onceamount=150000,
        )
        call_args = mock_plugin.rpc.call.call_args[0][1]
        assert call_args["onceamount"] == 200000

    def test_execute_once_rpc_error(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        mock_plugin.rpc.call.side_effect = Exception("connection failed")

        result = jm.execute_once(
            scid="123x456x0", direction="pull",
            amount=100000, maxppm=500,
        )

        assert result["success"] is False
        assert "connection failed" in result["error"]


class TestExtractFailureCount:
    """Change 10a: Verify _extract_failure_count works with nested and fallback."""

    def test_failure_count_from_nested_schema(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        stats = {"failures_in_time_window": {"total_rebalances": 7}}
        assert jm._extract_failure_count(stats) == 7

    def test_failure_count_fallback_consecutive(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        stats = {"consecutive_failures": 4}
        assert jm._extract_failure_count(stats) == 4

    def test_failure_count_empty(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        assert jm._extract_failure_count({}) == 0
        assert jm._extract_failure_count(None) == 0


class TestExtractFeePpm:
    """Change 10b: Verify _extract_fee_ppm extracts feeppm_weighted_avg."""

    def test_fee_ppm_from_nested_schema(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        stats = {
            "successes_in_time_window": {
                "total_amount_sats": 500000,
                "feeppm_weighted_avg": 120,
            }
        }
        assert jm._extract_fee_ppm(stats) == 120

    def test_fee_ppm_none_if_missing(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        assert jm._extract_fee_ppm({}) is None
        assert jm._extract_fee_ppm({"successes_in_time_window": {}}) is None


class TestFeeSatsFromTotalSpent:
    """Change 10c: Verify _handle_job_success uses total_spent_sats when
    other fee fields are missing."""

    def test_fee_from_total_spent_sats(self, mock_plugin, mock_database):
        from modules.config import Config
        from modules.rebalancer import JobManager, ActiveJob, JobStatus

        cfg = Config()
        jm = JobManager(mock_plugin, cfg, mock_database, hive_bridge=None)

        scid = "999x888x0"
        cand = _candidate(to_channel=scid)
        cand.max_budget_sats = 100
        cand.expected_profit_sats = 10

        job = ActiveJob(
            scid=scid,
            scid_normalized=scid,
            source_candidates=["111x222x0"],
            start_time=int(time.time()),
            candidate=cand,
            rebalance_id=42,
            target_amount_sats=50000,
            initial_local_sats=0,
            max_fee_ppm=2000,
            status=JobStatus.RUNNING,
        )
        jm._active_jobs[scid] = job

        mock_database.update_rebalance_result = MagicMock()
        mock_database.reset_failure_count = MagicMock()
        mock_database.record_rebalance_cost = MagicMock()
        mock_database.mark_budget_spent = MagicMock()

        stats = {
            "successes_in_time_window": {
                "total_amount_sats": 50000,
                "total_spent_sats": 25,
            }
        }

        jm._handle_job_success(job, 50000, stats)

        # Verify fee_sats=25 was used in record_rebalance_cost
        mock_database.record_rebalance_cost.assert_called_once()
        call_kwargs = mock_database.record_rebalance_cost.call_args
        assert call_kwargs[1]["cost_sats"] == 25 or call_kwargs[0][2] == 25
