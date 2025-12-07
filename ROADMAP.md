# cl-revenue-ops Roadmap: Enterprise Grade

This document outlines the development path to move `cl-revenue-ops` from a "Power User" tool to an "Enterprise Grade" routing engine suitable for managing high-liquidity nodes.

## Phase 1: Capital Safety & Controls (Completed)
*Objective: Prevent the algorithm from over-spending on fees or exhausting operating capital during high-volatility periods.*

- [x] **Global Daily Budgeting**: Implement a hard cap on total rebalancing fees paid per 24-hour rolling window.
- [x] **Wallet Reserve Protection**: Suspend all operations if on-chain or off-chain liquid funds drop below a safe reserve threshold.

## Phase 2: Observability (Completed)
*Objective: "You cannot manage what you cannot measure." Provide real-time visualization of algorithmic decisions.*

- [x] **Prometheus Metrics Exporter**: Expose a local HTTP endpoint to output time-series data for fees, revenue, ROI, and rebalancing.

## Phase 3: Traffic Intelligence (Completed)
*Objective: Optimize for quality liquidity and filter out noise/spam.*

- [x] **HTLC Slot Awareness**: Mark channels with >80% slot usage as `CONGESTED` and skip rebalancing.
- [x] **Reputation Tracking**: Track HTLC failure rates per peer in database.
- [x] **Reputation-Weighted Fees**: Discount volume from peers with high failure rates (spam/probing) in the Hill Climbing algorithm.

## Phase 4: Stability & Scaling (Next)
*Objective: Reduce network noise and handle high throughput.*

- [ ] **Deadband Hysteresis**:
    - Detect "Market Calm" (low revenue variance).
    - Enter "Sleep Mode" for stable channels to reduce gossip noise (`channel_update` spam).
- [ ] **Async Job Queue**:
    - Refactor `rebalancer.py` to decouple decision-making from execution.
    - Allow concurrent rebalancing attempts (if supported by the underlying rebalancer plugin).

---
*Roadmap updated: December 07, 2025*