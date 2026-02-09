"""
Tests for profitability_analyzer — effective rebalance cost.
"""

import os
import sys
import pytest
from unittest.mock import MagicMock

# Mock pyln.client before importing modules
mock_pyln = MagicMock()
mock_pyln.Plugin = MagicMock
mock_pyln.RpcError = Exception
sys.modules['pyln'] = mock_pyln
sys.modules['pyln.client'] = mock_pyln

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.profitability_analyzer import (
    ChannelCosts, ChannelRevenue, ChannelProfitability, ProfitabilityClass
)


def _make_profitability(
    rebalance_cost_sats=1000,
    effective_rebalance_cost_sats=0,
    fees_earned_sats=2000,
    sourced_fee_contribution_sats=0,
):
    """Build a ChannelProfitability with specified cost/revenue."""
    costs = ChannelCosts(
        channel_id="111x222x0",
        peer_id="02" + "a" * 64,
        open_cost_sats=500,
        rebalance_cost_sats=rebalance_cost_sats,
        effective_rebalance_cost_sats=effective_rebalance_cost_sats,
    )
    revenue = ChannelRevenue(
        channel_id="111x222x0",
        fees_earned_sats=fees_earned_sats,
        volume_routed_sats=1_000_000,
        forward_count=100,
        sourced_fee_contribution_sats=sourced_fee_contribution_sats,
    )
    return ChannelProfitability(
        channel_id="111x222x0",
        peer_id="02" + "a" * 64,
        capacity_sats=2_000_000,
        costs=costs,
        revenue=revenue,
        net_profit_sats=fees_earned_sats - costs.total_cost_sats,
        roi_percent=10.0,
        classification=ProfitabilityClass.PROFITABLE,
        cost_per_sat_routed=0.001,
        fee_per_sat_routed=0.002,
        days_open=30,
        last_routed=None,
    )


class TestEffectiveCost:
    """Test effective_rebalance_cost_sats in marginal_roi."""

    def test_marginal_roi_uses_effective_cost(self):
        """When effective > raw, marginal_roi should be lower."""
        # Raw cost = 1000, effective = 2000 (50% success rate)
        prof = _make_profitability(
            rebalance_cost_sats=1000,
            effective_rebalance_cost_sats=2000,
            fees_earned_sats=3000,
        )

        # marginal_roi = (3000 - 2000) / max(1, 2000) = 0.5
        assert abs(prof.marginal_roi - 0.5) < 0.01

        # Compare with raw cost version
        prof_raw = _make_profitability(
            rebalance_cost_sats=1000,
            effective_rebalance_cost_sats=0,  # fallback to raw
            fees_earned_sats=3000,
        )
        # marginal_roi = (3000 - 1000) / max(1, 1000) = 2.0
        assert abs(prof_raw.marginal_roi - 2.0) < 0.01

        # Effective cost version has lower ROI
        assert prof.marginal_roi < prof_raw.marginal_roi

    def test_effective_cost_fallback(self):
        """When effective_rebalance_cost_sats=0, falls back to raw."""
        prof = _make_profitability(
            rebalance_cost_sats=500,
            effective_rebalance_cost_sats=0,
            fees_earned_sats=1500,
        )

        # Should use raw cost: (1500 - 500) / max(1, 500) = 2.0
        assert abs(prof.marginal_roi - 2.0) < 0.01

    def test_no_costs_returns_one_if_earning(self):
        """With zero costs but earning, marginal_roi = 1.0."""
        prof = _make_profitability(
            rebalance_cost_sats=0,
            effective_rebalance_cost_sats=0,
            fees_earned_sats=1000,
        )
        assert prof.marginal_roi == 1.0

    def test_bleeder_classification_includes_effective_cost(self):
        """BleederClassification has effective_rebalance_cost_30d field."""
        from modules.profitability_analyzer import BleederClassification

        bc = BleederClassification(
            channel_id="111x222x0",
            peer_id="02" + "a" * 64,
            classification="hard",
            reason="test",
            rebalance_cost_30d=1000,
            revenue_30d=400,
            net_profit_30d=-600,
            net_profit_7d=-100,
            recommended_action="disable_rebalance",
            effective_rebalance_cost_30d=2000,
        )

        d = bc.to_dict()
        assert d["effective_rebalance_cost_30d"] == 2000
        assert d["rebalance_cost_30d"] == 1000
