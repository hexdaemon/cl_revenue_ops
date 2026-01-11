# Hive Simulation Suite Test Report

**Date:** 2026-01-11 (Expanded Network v2)
**Network:** Polar Network 1 (regtest) - 15 nodes (53% LND)
**Duration:** Extended natural simulation (multiple traffic phases)

---

## Executive Summary

Extended simulation testing with **expanded 15-node network** shows:

1. **Hive coordination active** - All 3 hive nodes (alice, bob, carol) coordinating
2. **Zero inter-hive fees** - cl-revenue-ops sets 0 ppm between hive members
3. **Natural fee management** - HIVE strategy applied automatically to hive peers
4. **Hive dominance** - Hive nodes routed **77%** of all network forwards
5. **Realistic LND ratio** - 8 LND nodes (53%) with diverse charge-lnd configs
6. **Fee strategy comparison** - High-fee LND nodes earn more per forward but route less

---

## Natural Simulation Results

### Fee Configuration (Set by Plugins)

| Node Type | Fee Manager | Inter-Hive | External Channels |
|-----------|-------------|:----------:|------------------:|
| Hive (alice, bob, carol) | cl-revenue-ops | 0 ppm | 10-60 ppm (DYNAMIC) |
| CLN External (dave, erin, pat, oscar) | CLBOSS | N/A | 500 ppm |
| LND Competitive (lnd1) | charge-lnd | N/A | 10-350 ppm |
| LND Aggressive (lnd2) | charge-lnd | N/A | 100-1000 ppm |
| LND Conservative (judy) | charge-lnd | N/A | 200-400 ppm |
| LND Balanced (kathy) | charge-lnd | N/A | 75-500 ppm |
| LND Dynamic (lucy) | charge-lnd | N/A | 5-2000 ppm |
| LND Whale (mike) | charge-lnd | N/A | 1-100 ppm |
| LND Sniper (quincy) | charge-lnd | N/A | 1-1500 ppm |
| LND Lazy (niaj) | charge-lnd | N/A | 75-300 ppm |

### Profitability Comparison (Expanded Network)

| Node | Type | Implementation | Forwards | Total Fees | Fee/Forward |
|------|------|----------------|----------|------------|-------------|
| alice | Hive | CLN | 438 | 96 sats | 0.22 sats |
| bob | Hive | CLN | 340 | 81 sats | 0.24 sats |
| carol | Hive | CLN | 13 | 0.5 sats | 0.03 sats |
| dave | External | CLN | 83 | 57 sats | 0.69 sats |
| erin | External | CLN | 103 | 24 sats | 0.23 sats |
| pat | External | CLN | 0 | 0 sats | - |
| oscar | External | CLN | 0 | 0 sats | - |
| lnd1 | External | LND | 30 | 28 sats | 0.93 sats |
| lnd2 | External | LND | 19 | 201 sats | **10.58 sats** |
| judy | External | LND | 0 | 0 sats | - |
| kathy | External | LND | 0 | 0 sats | - |
| lucy | External | LND | 0 | 0 sats | - |
| mike | External | LND | 0 | 0 sats | - |
| quincy | External | LND | 0 | 0 sats | - |
| niaj | External | LND | 0 | 0 sats | - |

**Summary by Node Type:**
| Type | Nodes | Total Forwards | Total Fees | Avg Fee/Forward |
|------|-------|----------------|------------|-----------------|
| Hive (CLN) | 3 | 791 | 178 sats | 0.22 sats |
| External (CLN) | 4 | 186 | 81 sats | 0.44 sats |
| External (LND) | 8 | 49 | 230 sats | **4.69 sats** |

**Key Findings:**
1. Hive nodes routed **77%** of all forwards (791 of 1026 total)
2. LND nodes earned **highest per-forward fees** (4.69 sats avg) but only 5% of volume
3. lnd2's aggressive fee strategy (100-1000 ppm) earned 201 sats on only 19 forwards
4. New LND nodes (judy, kathy, lucy, mike, quincy, niaj) not on primary routing paths yet
5. Zero inter-hive fees enable efficient internal routing without fee loss

### Plugin/Tool Status

| Node | Implementation | cl-revenue-ops | cl-hive | Fee Manager |
|------|----------------|:--------------:|:-------:|:-----------:|
| alice | CLN v25.12 | v1.5.0 | v0.1.0-dev | CLBOSS v0.15.1 |
| bob | CLN v25.12 | v1.5.0 | v0.1.0-dev | CLBOSS v0.15.1 |
| carol | CLN v25.12 | v1.5.0 | v0.1.0-dev | CLBOSS v0.15.1 |
| dave | CLN v25.12 | - | - | CLBOSS v0.15.1 |
| erin | CLN v25.12 | - | - | CLBOSS v0.15.1 |
| pat | CLN v25.12 | - | - | CLBOSS v0.15.1 |
| oscar | CLN v25.12 | - | - | CLBOSS v0.15.1 |
| lnd1 | LND v0.20.0 | - | - | charge-lnd (Competitive) |
| lnd2 | LND v0.20.0 | - | - | charge-lnd (Aggressive) |
| judy | LND v0.20.0 | - | - | charge-lnd (Conservative) |
| kathy | LND v0.20.0 | - | - | charge-lnd (Balanced) |
| lucy | LND v0.20.0 | - | - | charge-lnd (Dynamic) |
| mike | LND v0.20.0 | - | - | charge-lnd (Whale) |
| quincy | LND v0.20.0 | - | - | charge-lnd (Sniper) |
| niaj | LND v0.20.0 | - | - | charge-lnd (Lazy) |

---

## Detailed Test Results

### Hive Coordination (cl-hive)

| Node | Status | Tier | Members Seen |
|------|--------|------|--------------|
| alice | active | admin | 3 (alice, bob, carol) |
| bob | active | admin | 3 (alice, bob, carol) |
| carol | active | member | 3 (alice, bob, carol) |

**Observations:**
- Hive governance mode: autonomous
- Carol promoted from neophyte to member tier
- All nodes share the same hive ID (hive_a337541fde61c25e)
- HIVE fee policy (0 ppm) applied to all inter-hive channels

### cl-revenue-ops Fee Policies

| Node | Peer | Strategy | Result |
|------|------|----------|--------|
| alice | bob | HIVE | 0 ppm on 243x1x0 |
| alice | carol | HIVE | 0 ppm on 414x1x0 |
| bob | alice | HIVE | 0 ppm on 243x1x0 |
| bob | carol | HIVE | 0 ppm on 255x1x0 |
| carol | alice | HIVE | 0 ppm on 414x1x0 |
| carol | bob | HIVE | 0 ppm on 255x1x0 |

**Non-hive peers use DYNAMIC strategy** - fees adjusted by HillClimb algorithm based on liquidity and flow.

### LND Fee Management (charge-lnd)

All 8 LND nodes use charge-lnd for dynamic fee adjustment based on channel balance ratios. Each node has a unique fee policy to simulate real-world variation.

**lnd1 Configuration (Competitive):**
| Policy | Balance Range | Base Fee | Fee PPM |
|--------|---------------|----------|---------|
| depleted | < 15% local | 500 msat | 350 ppm |
| balanced | 15-85% local | 250 msat | 30-150 ppm |
| saturated | > 85% local | 0 msat | 10 ppm |

**lnd2 Configuration (Aggressive):**
| Policy | Balance Range | Base Fee | Fee PPM |
|--------|---------------|----------|---------|
| depleted | < 25% local | 5000 msat | 1000 ppm |
| balanced | 25-75% local | 1000 msat | 200-600 ppm |
| saturated | > 75% local | 500 msat | 100 ppm |

**judy Configuration (Conservative):**
| Policy | Balance Range | Base Fee | Fee PPM |
|--------|---------------|----------|---------|
| depleted | < 20% local | 1000 msat | 400 ppm |
| balanced | 20-80% local | 500 msat | 200-300 ppm |
| saturated | > 80% local | 250 msat | 200 ppm |

**kathy Configuration (Balanced):**
| Policy | Balance Range | Base Fee | Fee PPM |
|--------|---------------|----------|---------|
| depleted | < 20% local | 1000 msat | 500 ppm |
| balanced | 20-80% local | 500 msat | 75-300 ppm |
| saturated | > 80% local | 100 msat | 75 ppm |

**lucy Configuration (Dynamic):**
| Policy | Balance Range | Base Fee | Fee PPM |
|--------|---------------|----------|---------|
| depleted | < 10% local | 10000 msat | 2000 ppm |
| balanced | 10-90% local | 500 msat | 50-1000 ppm |
| saturated | > 90% local | 0 msat | 5 ppm |

**mike Configuration (Whale):**
| Policy | Balance Range | Base Fee | Fee PPM |
|--------|---------------|----------|---------|
| depleted | < 30% local | 100 msat | 100 ppm |
| balanced | 30-70% local | 50 msat | 25-50 ppm |
| saturated | > 70% local | 0 msat | 1 ppm |

**quincy Configuration (Sniper):**
| Policy | Balance Range | Base Fee | Fee PPM |
|--------|---------------|----------|---------|
| depleted | < 5% local | 2000 msat | 1500 ppm |
| low | 5-25% local | 1000 msat | 500-750 ppm |
| balanced | 25-75% local | 500 msat | 100-300 ppm |
| high | 75-95% local | 100 msat | 25-75 ppm |
| saturated | > 95% local | 0 msat | 1 ppm |

**niaj Configuration (Lazy):**
| Policy | Balance Range | Base Fee | Fee PPM |
|--------|---------------|----------|---------|
| depleted | < 30% local | 1000 msat | 300 ppm |
| balanced | 30-70% local | 500 msat | 150 ppm |
| saturated | > 70% local | 250 msat | 75 ppm |

### Channel Topology (Expanded 15-Node Network)

```
HIVE NODES (3)                     EXTERNAL CLN (4)              LND NODES (8)
┌─────────────┐                   ┌─────────────┐              ┌─────────────┐
│   alice     │                   │    dave     │              │    lnd1     │
│  10 channels│◄─────────────────►│  11 channels│◄────────────►│  9 channels │
│  (0ppm hive)│                   │  (500ppm)   │              │ (Competitive)│
└─────────────┘                   └─────────────┘              └─────────────┘
       │                                │                             │
       │                                │                             │
┌─────────────┐                   ┌─────────────┐              ┌─────────────┐
│    bob      │                   │    erin     │              │    lnd2     │
│  8 channels │◄─────────────────►│  8 channels │◄────────────►│  7 channels │
│  (0ppm hive)│                   │  (500ppm)   │              │ (Aggressive) │
└─────────────┘                   └─────────────┘              └─────────────┘
       │                                │                             │
       │                                │                             │
┌─────────────┐                   ┌─────────────┐              ┌─────────────┐
│   carol     │                   │    pat      │              │judy/kathy   │
│  7 channels │◄─────────────────►│  3 channels │◄────────────►│  4 channels │
│  (0ppm hive)│                   │  (500ppm)   │              │(Conserv/Bal)│
└─────────────┘                   └─────────────┘              └─────────────┘
                                        │                             │
                                  ┌─────────────┐              ┌─────────────┐
                                  │   oscar     │              │lucy/mike    │
                                  │  3 channels │              │quincy/niaj  │
                                  │  (500ppm)   │              │  2-3 chans  │
                                  └─────────────┘              └─────────────┘
```

**Network Statistics:**
- Total nodes: 15 (7 CLN, 8 LND = 53% LND)
- Total active channels: ~75
- Hive internal routing: 0 ppm
- External CLN fees: 500 ppm (CLBOSS default)
- LND fees: 1-2000 ppm (charge-lnd dynamic)

---

## New Commands Added

### Hive-Specific Commands
| Command | Description |
|---------|-------------|
| `hive-test <mins>` | Full hive system test (all phases) |
| `hive-coordination` | Test cl-hive channel coordination |
| `hive-competition <mins>` | Test hive vs non-hive routing competition |
| `hive-fees` | Test hive fee coordination |
| `hive-rebalance` | Test cl-revenue-ops rebalancing |

### Setup Commands
| Command | Description |
|---------|-------------|
| `setup-channels` | Setup bidirectional channel topology |
| `pre-balance` | Balance channels via circular payments |

---

## Hive System Test Results

### Phase 1: Pre-test Setup
- Detected 7 unbalanced channels (< 20% or > 80% local)
- Automated channel balancing via circular payments
- Successfully pushed liquidity to external nodes

### Phase 2: Hive Coordination (cl-hive)
| Node | Is Member | Hive Size | Pending Intents |
|------|-----------|-----------|-----------------|
| alice | Yes | 4 | 0 |
| bob | Yes | 4 | 0 |
| carol | Yes | 4 | 0 |

**Observations:**
- cl-hive running on all hive nodes
- Hive has 4 members
- Intent system operational

### Phase 3: Fee Management (cl-revenue-ops)

**Policy Settings:**
- alice: 2 policies (dynamic + hive strategy)
- bob: 0 policies (using defaults)
- carol: 0 policies (using defaults)

**Flow State Detection:**
| Node | Channel | State | Flow Ratio |
|------|---------|-------|------------|
| alice | 243x1x0 | balanced | 0.0 |
| alice | 314x1x0 | sink | -0.6 |
| alice | 406x1x0 | source | 0.6 |
| bob | 243x1x0 | balanced | -0.11 |
| bob | 255x1x0 | sink | -0.6 |
| bob | 406x2x0 | balanced | 0.22 |
| carol | 255x1x0 | source | 0.6 |
| carol | 277x1x0 | sink | -0.6 |

### Phase 4: Competition Test

| Metric | Value |
|--------|-------|
| Total Payments | 78 |
| Routed via Hive | 0 (0%) |
| Routed via External | 78 (100%) |

**Analysis:** Current topology doesn't place hive nodes on the path between dave and erin. Need to add channels to make hive nodes routing intermediaries.

### Phase 5: Rebalancing (cl-revenue-ops)

| Node | Source Channel | Sink Channel | Result |
|------|----------------|--------------|--------|
| alice | 314x1x0 (93%) | 243x1x0 (13%) | Async job started |
| bob | 243x1x0 (86%) | 406x3x0 (0%) | Async job started |
| carol | 277x1x0 (100%) | 255x1x0 (0%) | Async job started |

**Success:** All 3 rebalance jobs started successfully using cl-revenue-ops (not CLBOSS).

### Phase 6: Performance Analysis

**Channel Efficiency (Turnover):**
| Node | Channel | Velocity | Turnover |
|------|---------|----------|----------|
| alice | 243x1x0 | 0.03 | 0 |
| bob | 406x2x0 | 0.0 | 0.53 |
| bob | 243x1x0 | -0.30 | 0.27 |

---

## Current Channel Topology

```
HIVE NODES                         EXTERNAL NODES
┌─────────────┐                   ┌──────────────┐
│   alice     │                   │    dave      │
│ ├─ 314x1x0 → lnd1              │ ├─ 277x1x0 ← carol
│ ├─ 243x1x0 ↔ bob               │ ├─ 406x1x0 → alice
│ └─ 406x1x0 ← dave              │ ├─ 406x2x0 → bob
└─────────────┘                   │ └─ 289x1x0 → erin
                                  └──────────────┘
┌─────────────┐                   ┌──────────────┐
│    bob      │                   │    erin      │
│ ├─ 243x1x0 ↔ alice             │ ├─ 289x1x0 ← dave
│ ├─ 255x1x0 → carol             │ └─ 406x3x0 → bob
│ ├─ 406x2x0 ← dave              └──────────────┘
│ └─ 406x3x0 ← erin
└─────────────┘

┌─────────────┐
│   carol     │
│ ├─ 255x1x0 ← bob
│ └─ 277x1x0 → dave
└─────────────┘
```

---

## Recommendations for Future Testing

### Network Composition ✅ ACHIEVED
- ~~Add more LND nodes~~ - Network now has 8 LND (53%), matching real-world ~55%
- ~~Vary charge-lnd configs~~ - 8 unique fee strategies implemented

### Improving LND Routing Participation
1. **New LND nodes not routing** - judy, kathy, lucy, mike, quincy, niaj have 0 forwards
2. Need to position LND nodes on primary routing paths between payment endpoints
3. Consider opening channels from LND nodes directly to both payment sources and destinations
4. Run longer simulations to allow gossip propagation and pathfinding adaptation

### For Rebalancing Testing
1. Run longer tests to observe rebalance completion
2. Monitor `revenue-status` to see rebalance effects
3. Add periodic rebalance triggers

### For Fee Strategy Analysis
1. lnd2's aggressive strategy (100-1000 ppm) earned 10.58 sats/forward - highest in network
2. Compare total revenue vs per-forward revenue strategies over longer periods
3. Test how hive's low-fee strategy affects channel flow balance
4. Run charge-lnd periodically on LND nodes to adapt fees dynamically

---

## Files Modified

| File | Changes |
|------|---------|
| `simulate.sh` | Fixed metrics, added hive tests, added pre-balance |

---

## Usage Examples

```bash
# Full hive system test (15 minutes)
./simulate.sh hive-test 15 1

# Setup and balance channels
./simulate.sh setup-channels 1
./simulate.sh pre-balance 1

# Individual hive tests
./simulate.sh hive-coordination 1
./simulate.sh hive-competition 10 1
./simulate.sh hive-fees 1
./simulate.sh hive-rebalance 1

# View results
./simulate.sh report 1
```

---

*Report generated by cl-revenue-ops simulation suite v1.3*
*Last updated: 2026-01-11 - Expanded to 15-node network with 8 LND nodes (53%)*
