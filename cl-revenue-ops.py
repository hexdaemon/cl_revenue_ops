#!/usr/bin/env python3
"""
cl-revenue-ops: A Revenue Operations Plugin for Core Lightning

This plugin acts as a "Revenue Operations" layer that sits on top of the clboss 
automated manager. While clboss handles channel creation and node reliability,
this plugin overrides clboss for fee setting and rebalancing decisions to 
maximize profitability based on economic principles rather than heuristics.

MANAGER-OVERRIDE PATTERN:
-------------------------
Before changing any channel state, this plugin checks if the peer is managed 
by clboss. If it is, we issue the `clboss-unmanage` command for that specific 
peer and tag (e.g., lnfee) to prevent clboss from reverting our changes.

Author: Lightning Goats Team
License: MIT
"""

import os
import sys
import time
import json
import random
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from pathlib import Path

from pyln.client import Plugin, RpcError

# Import our modules
from modules.flow_analysis import FlowAnalyzer, ChannelState
from modules.fee_controller import PIDFeeController
from modules.rebalancer import EVRebalancer
from modules.clboss_manager import ClbossManager
from modules.config import Config
from modules.database import Database
from modules.profitability_analyzer import ChannelProfitabilityAnalyzer
from modules.capacity_planner import CapacityPlanner
from modules.metrics import PrometheusExporter, MetricNames, METRIC_HELP

# Initialize the plugin
plugin = Plugin()

# Global instances (initialized in init)
flow_analyzer: Optional[FlowAnalyzer] = None
fee_controller: Optional[PIDFeeController] = None
rebalancer: Optional[EVRebalancer] = None
clboss_manager: Optional[ClbossManager] = None
database: Optional[Database] = None
config: Optional[Config] = None
profitability_analyzer: Optional[ChannelProfitabilityAnalyzer] = None
capacity_planner: Optional[CapacityPlanner] = None
metrics_exporter: Optional[PrometheusExporter] = None

# SCID to Peer ID cache for reputation tracking
_scid_to_peer_cache: Dict[str, str] = {}


# =============================================================================
# PLUGIN OPTIONS
# =============================================================================

plugin.add_option(name='revenue-ops-db-path', default='~/.lightning/revenue_ops.db', description='Path to the SQLite database')
plugin.add_option(name='revenue-ops-flow-interval', default='3600', description='Interval for flow analysis')
plugin.add_option(name='revenue-ops-fee-interval', default='1800', description='Interval for fee adjustments')
plugin.add_option(name='revenue-ops-rebalance-interval', default='900', description='Interval for rebalance checks')
plugin.add_option(name='revenue-ops-target-flow', default='100000', description='Target daily flow in sats')
plugin.add_option(name='revenue-ops-min-fee-ppm', default='10', description='Min fee in PPM')
plugin.add_option(name='revenue-ops-max-fee-ppm', default='5000', description='Max fee in PPM')
plugin.add_option(name='revenue-ops-rebalance-min-profit', default='10', description='Min profit for rebalance')
plugin.add_option(name='revenue-ops-pid-kp', default='0.5', description='PID Kp')
plugin.add_option(name='revenue-ops-pid-ki', default='0.1', description='PID Ki')
plugin.add_option(name='revenue-ops-pid-kd', default='0.05', description='PID Kd')
plugin.add_option(name='revenue-ops-flow-window-days', default='7', description='Days to analyze flow')
plugin.add_option(name='revenue-ops-clboss-enabled', default='true', description='Interact with clboss')
plugin.add_option(name='revenue-ops-rebalancer', default='sling', description='Rebalancer plugin')
plugin.add_option(name='revenue-ops-daily-budget-sats', default='5000', description='Max rebalance fees/24h')
plugin.add_option(name='revenue-ops-min-wallet-reserve', default='1000000', description='Min reserve sats')
plugin.add_option(name='revenue-ops-dry-run', default='false', description='Dry run mode')
plugin.add_option(name='revenue-ops-htlc-congestion-threshold', default='0.8', description='Congestion threshold')
plugin.add_option(name='revenue-ops-enable-reputation', default='true', description='Weight volume by reputation')
plugin.add_option(name='revenue-ops-reputation-decay', default='0.98', description='Reputation decay factor')
plugin.add_option(name='revenue-ops-enable-prometheus', default='false', description='Enable Prometheus')
plugin.add_option(name='revenue-ops-prometheus-port', default='9800', description='Prometheus port')
plugin.add_option(name='revenue-ops-enable-kelly', default='false', description='Enable Kelly Criterion')
plugin.add_option(name='revenue-ops-kelly-fraction', default='0.5', description='Kelly multiplier')


# =============================================================================
# INITIALIZATION
# =============================================================================

@plugin.init()
def init(options: Dict[str, Any], configuration: Dict[str, Any], plugin: Plugin, **kwargs):
    global flow_analyzer, fee_controller, rebalancer, clboss_manager, database, config, profitability_analyzer, capacity_planner, metrics_exporter
    
    plugin.log("Initializing cl-revenue-ops plugin (Safety-Hardened Startup)...")
    
    # Build configuration
    config = Config(
        db_path=os.path.expanduser(options['revenue-ops-db-path']),
        flow_interval=int(options['revenue-ops-flow-interval']),
        fee_interval=int(options['revenue-ops-fee-interval']),
        rebalance_interval=int(options['revenue-ops-rebalance-interval']),
        target_flow=int(options['revenue-ops-target-flow']),
        min_fee_ppm=int(options['revenue-ops-min-fee-ppm']),
        max_fee_ppm=int(options['revenue-ops-max-fee-ppm']),
        rebalance_min_profit=int(options['revenue-ops-rebalance-min-profit']),
        pid_kp=float(options['revenue-ops-pid-kp']),
        pid_ki=float(options['revenue-ops-pid-ki']),
        pid_kd=float(options['revenue-ops-pid-kd']),
        flow_window_days=int(options['revenue-ops-flow-window-days']),
        clboss_enabled=options['revenue-ops-clboss-enabled'].lower() == 'true',
        rebalancer_plugin=options['revenue-ops-rebalancer'],
        daily_budget_sats=int(options['revenue-ops-daily-budget-sats']),
        min_wallet_reserve=int(options['revenue-ops-min-wallet-reserve']),
        dry_run=options['revenue-ops-dry-run'].lower() == 'true',
        htlc_congestion_threshold=float(options['revenue-ops-htlc-congestion-threshold']),
        enable_reputation=options['revenue-ops-enable-reputation'].lower() == 'true',
        reputation_decay=float(options['revenue-ops-reputation-decay']),
        enable_prometheus=options['revenue-ops-enable-prometheus'].lower() == 'true',
        prometheus_port=int(options['revenue-ops-prometheus-port']),
        enable_kelly=options['revenue-ops-enable-kelly'].lower() == 'true',
        kelly_fraction=float(options['revenue-ops-kelly-fraction'])
    )
    
    # 1. Dependency checks (Minimal RPC)
    try:
        active_plugins = [p.get("name", "").lower() for p in plugin.rpc.plugin("list").get("plugins", [])]
        config.sling_available = any("sling" in name for name in active_plugins)
    except Exception as e:
        plugin.log(f"Initial plugin check failed (non-fatal): {e}", level='debug')
        config.sling_available = True

    # 2. Database Initialization
    database = Database(config.db_path, plugin)
    database.initialize()
    
    # CRASH FIX: We have removed the listpeers snapshot from init. 
    # Calling listpeers while lightningd is replaying blocks causes SIG11.
    plugin.log("Startup snapshot deferred to background thread to prevent daemon race condition.")
    
    # 3. Initialize Analysis Modules
    if config.enable_prometheus:
        metrics_exporter = PrometheusExporter(port=config.prometheus_port, plugin=plugin)
        metrics_exporter.start_server()
    
    clboss_manager = ClbossManager(plugin, config)
    profitability_analyzer = ChannelProfitabilityAnalyzer(plugin, config, database, metrics_exporter)
    flow_analyzer = FlowAnalyzer(plugin, config, database)
    capacity_planner = CapacityPlanner(plugin, config, profitability_analyzer, flow_analyzer)
    fee_controller = PIDFeeController(plugin, config, database, clboss_manager, profitability_analyzer, metrics_exporter)
    rebalancer = EVRebalancer(plugin, config, database, clboss_manager, metrics_exporter)
    rebalancer.set_profitability_analyzer(profitability_analyzer)
    
    # 4. Define Background Loops
    def flow_analysis_loop():
        time.sleep(30) # Delay first run
        while True:
            try:
                run_flow_analysis()
                if database:
                    database.cleanup_old_data(days_to_keep=max(8, config.flow_window_days + 1))
                if metrics_exporter:
                    update_peer_reputation_metrics()
            except Exception as e:
                plugin.log(f"Error in flow analysis: {e}", level='error')
            time.sleep(config.flow_interval + random.randint(-60, 60))

    def fee_adjustment_loop():
        time.sleep(90) # Delay first run
        while True:
            try:
                run_fee_adjustment()
            except Exception as e:
                plugin.log(f"Error in fee adjustment: {e}", level='error')
            time.sleep(config.fee_interval + random.randint(-60, 60))

    def rebalance_check_loop():
        if not config.sling_available: return
        time.sleep(150) # Delay first run
        while True:
            try:
                run_rebalance_check()
            except Exception as e:
                plugin.log(f"Error in rebalance check: {e}", level='error')
            time.sleep(config.rebalance_interval + random.randint(-60, 60))

    def snapshot_peers_delayed():
        """Delayed snapshot to allow lightningd to finish block replaying."""
        delay = 120 # Wait 2 minutes for stability
        plugin.log(f"Background snapshot: waiting {delay}s for daemon stability...")
        time.sleep(delay)
        try:
            peers = plugin.rpc.listpeers()
            count = 0
            for peer in peers.get("peers", []):
                if peer.get("connected", False):
                    database.record_connection_event(peer["id"], "snapshot")
                    count += 1
            plugin.log(f"Stability Snapshot: Recorded {count} connected peers.")
        except Exception as e:
            plugin.log(f"Error in delayed snapshot: {e}", level='warn')

    # 5. Start threads
    threading.Thread(target=flow_analysis_loop, daemon=True, name="flow-analysis").start()
    threading.Thread(target=fee_adjustment_loop, daemon=True, name="fee-adjustment").start()
    threading.Thread(target=rebalance_check_loop, daemon=True, name="rebalance-check").start()
    threading.Thread(target=snapshot_peers_delayed, daemon=True, name="startup-snapshot").start()
    
    plugin.log("cl-revenue-ops initialized successfully!")
    return None


# =============================================================================
# CORE LOGIC FUNCTIONS
# =============================================================================

def run_flow_analysis():
    if not flow_analyzer: return
    try:
        flow_analyzer.analyze_all_channels()
        if database and config and config.enable_reputation:
            database.decay_reputation(config.reputation_decay)
    except Exception as e:
        plugin.log(f"Flow analysis failed: {e}", level='error')

def run_fee_adjustment():
    if not fee_controller: return
    try:
        fee_controller.adjust_all_fees()
    except Exception as e:
        plugin.log(f"Fee adjustment failed: {e}", level='error')

def run_rebalance_check():
    if not rebalancer: return
    try:
        candidates = rebalancer.find_rebalance_candidates()
        for candidate in candidates:
            rebalancer.execute_rebalance(candidate)
    except Exception as e:
        plugin.log(f"Rebalance check failed: {e}", level='error')

def update_peer_reputation_metrics():
    if not database or not metrics_exporter: return
    try:
        for rep in database.get_all_peer_reputations():
            peer_id = rep.get('peer_id', '')
            if not peer_id: continue
            labels = {"peer_id": peer_id}
            metrics_exporter.set_gauge(MetricNames.PEER_REPUTATION_SCORE, rep.get('score', 1.0), labels)
            metrics_exporter.set_gauge(MetricNames.PEER_SUCCESS_COUNT, rep.get('successes', 0), labels)
            metrics_exporter.set_gauge(MetricNames.PEER_FAILURE_COUNT, rep.get('failures', 0), labels)
    except Exception as e:
        plugin.log(f"Error updating peer metrics: {e}", level='warn')


# =============================================================================
# RPC METHODS
# =============================================================================

@plugin.method("revenue-status")
def revenue_status(plugin: Plugin):
    if not database: return {"error": "Plugin not fully initialized"}
    return {
        "status": "running",
        "config": {"target_flow": config.target_flow, "dry_run": config.dry_run},
        "channel_states": database.get_all_channel_states(),
        "recent_fee_changes": database.get_recent_fee_changes(limit=10),
        "recent_rebalances": database.get_recent_rebalances(limit=10)
    }

@plugin.method("revenue-analyze")
def revenue_analyze(plugin: Plugin, channel_id: Optional[str] = None):
    if not flow_analyzer: return {"error": "Plugin not initialized"}
    if channel_id:
        result = flow_analyzer.analyze_channel(channel_id)
        return {"channel": channel_id, "analysis": result.to_dict() if result else None}
    run_flow_analysis()
    return {"status": "Flow analysis triggered"}

@plugin.method("revenue-capacity-report")
def revenue_capacity_report(plugin: Plugin):
    if not capacity_planner: raise RpcError("revenue-capacity-report", {}, "Not initialized")
    return capacity_planner.generate_report()

@plugin.method("revenue-set-fee")
def revenue_set_fee(plugin: Plugin, channel_id: str, fee_ppm: int):
    if not fee_controller: return {"error": "Not initialized"}
    return fee_controller.set_channel_fee(channel_id, fee_ppm, manual=True)

@plugin.method("revenue-rebalance")
def revenue_rebalance(plugin: Plugin, from_channel: str, to_channel: str, amount_sats: int, max_fee_sats: Optional[int] = None):
    if not rebalancer: return {"error": "Not initialized"}
    return rebalancer.manual_rebalance(from_channel, to_channel, amount_sats, max_fee_sats)

@plugin.method("revenue-profitability")
def revenue_profitability(plugin: Plugin, channel_id: Optional[str] = None):
    if not profitability_analyzer: return {"error": "Not initialized"}
    if channel_id:
        res = profitability_analyzer.analyze_channel(channel_id)
        return res.to_dict() if res else {"error": "No data"}
    return profitability_analyzer.get_summary()

@plugin.method("revenue-history")
def revenue_history(plugin: Plugin):
    if not profitability_analyzer: return {"error": "Not initialized"}
    return profitability_analyzer.get_lifetime_report()

@plugin.method("revenue-remanage")
def revenue_remanage(plugin: Plugin, peer_id: str, tag: Optional[str] = None):
    if not clboss_manager: return {"error": "Not initialized"}
    return clboss_manager.remanage(peer_id, tag)

@plugin.method("revenue-clboss-status")
def revenue_clboss_status(plugin: Plugin):
    if not clboss_manager: return {"error": "Not initialized"}
    return clboss_manager.get_unmanaged_status()


# =============================================================================
# HOOKS & SUBSCRIPTIONS
# =============================================================================

def _resolve_scid_to_peer(scid: str) -> Optional[str]:
    global _scid_to_peer_cache
    if scid in _scid_to_peer_cache: return _scid_to_peer_cache[scid]
    try:
        for channel in plugin.rpc.listpeerchannels().get("channels", []):
            cid = channel.get("short_channel_id") or channel.get("channel_id")
            if cid: _scid_to_peer_cache[cid] = channel["peer_id"]
        return _scid_to_peer_cache.get(scid)
    except: return None

@plugin.subscribe("forward_event")
def on_forward_event(forward_event: Dict, plugin: Plugin, **kwargs):
    if not database: return
    status = forward_event.get("status")
    in_channel = (forward_event.get("in_channel") or "").replace(':', 'x')
    if in_channel:
        peer_id = _resolve_scid_to_peer(in_channel)
        if peer_id: database.update_peer_reputation(peer_id, is_success=(status == "settled"))
    
    if status == "settled":
        out_channel = (forward_event.get("out_channel") or "").replace(':', 'x')
        def pmsat(v): return int(v.millisatoshis) if hasattr(v, 'millisatoshis') else int(str(v).replace('msat', ''))
        database.record_forward(
            in_channel, out_channel, 
            pmsat(forward_event.get("in_msatoshi", 0)),
            pmsat(forward_event.get("out_msatoshi", 0)),
            pmsat(forward_event.get("fee_msatoshi", 0)),
            forward_event.get("resolved_time", 0) - forward_event.get("received_time", 0)
        )

@plugin.subscribe("connect")
def on_peer_connect(plugin: Plugin, **kwargs):
    if not database: return
    # Safer extraction to avoid SIG11 during concurrent RPCs
    peer_id = kwargs.get('id') or (kwargs.get('connect', {}) if isinstance(kwargs.get('connect'), dict) else {}).get('id')
    if peer_id: database.record_connection_event(peer_id, "connected")

@plugin.subscribe("disconnect")
def on_peer_disconnect(plugin: Plugin, **kwargs):
    if not database: return
    peer_id = kwargs.get('id') or (kwargs.get('disconnect', {}) if isinstance(kwargs.get('disconnect'), dict) else {}).get('id')
    if peer_id: database.record_connection_event(peer_id, "disconnected")

if __name__ == "__main__":
    plugin.run()