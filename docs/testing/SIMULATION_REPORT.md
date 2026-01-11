# Hive Simulation Suite Test Report

**Date:** 2026-01-11 (Comprehensive Test v3)
**Network:** Polar Network 1 (regtest) - 15 nodes (53% LND)
**Duration:** 30-minute comprehensive simulation (4 traffic phases)

---

## Executive Summary

**30-minute comprehensive simulation** with optimized fee strategy shows:

1. **Hive dominance confirmed** - Hive nodes routed **74%** of all network forwards (1,017 of 1,373)
2. **Optimized fee strategy** - 0 ppm inter-hive, 75 ppm minimum for external channels
3. **2x revenue improvement** - Hive earned 356 sats (vs ~178 sats with 10 ppm floor)
4. **Volume vs margin tradeoff** - Hive prioritizes volume; LND nodes prioritize per-forward profit
5. **Realistic competition** - 8 LND nodes (53%) with diverse charge-lnd fee strategies
6. **lnd2 highest margin** - Aggressive fees (100-1000 ppm) earned 10.57 sats/forward

---

## 30-Minute Comprehensive Test Results

### Fee Configuration (Optimized)

| Node Type | Fee Manager | Inter-Hive | External Channels |
|-----------|-------------|:----------:|------------------:|
| Hive (alice, bob, carol) | cl-revenue-ops | **0 ppm** | **75+ ppm** (DYNAMIC) |
| CLN External (dave, erin, pat, oscar) | CLBOSS | N/A | 500 ppm |
| LND Competitive (lnd1) | charge-lnd | N/A | 10-350 ppm |
| LND Aggressive (lnd2) | charge-lnd | N/A | 100-1000 ppm |
| LND Conservative (judy) | charge-lnd | N/A | 200-400 ppm |
| LND Balanced (kathy) | charge-lnd | N/A | 75-500 ppm |
| LND Dynamic (lucy) | charge-lnd | N/A | 5-2000 ppm |
| LND Whale (mike) | charge-lnd | N/A | 1-100 ppm |
| LND Sniper (quincy) | charge-lnd | N/A | 1-1500 ppm |
| LND Lazy (niaj) | charge-lnd | N/A | 75-300 ppm |

### Profitability Comparison (30-Minute Test)

| Node | Type | Implementation | Forwards | Total Fees | Fee/Forward |
|------|------|----------------|----------|------------|-------------|
| alice | Hive | CLN | 635 | 268 sats | 0.42 sats |
| bob | Hive | CLN | 366 | 87 sats | 0.24 sats |
| carol | Hive | CLN | 16 | 0 sats | 0 sats |
| dave | External | CLN | 179 | 460 sats | **2.56 sats** |
| erin | External | CLN | 106 | 25 sats | 0.24 sats |
| pat | External | CLN | 0 | 0 sats | - |
| oscar | External | CLN | 0 | 0 sats | - |
| lnd1 | External | LND | 31 | 28 sats | 0.90 sats |
| lnd2 | External | LND | 19 | 201 sats | **10.57 sats** |
| judy | External | LND | 0 | 0 sats | - |
| kathy | External | LND | 0 | 0 sats | - |
| lucy | External | LND | 0 | 0 sats | - |
| mike | External | LND | 0 | 0 sats | - |
| quincy | External | LND | 0 | 0 sats | - |
| niaj | External | LND | 21 | 20 sats | 0.95 sats |

**Summary by Node Type:**
| Type | Nodes | Total Forwards | Total Fees | % Traffic | Avg Fee/Forward |
|------|-------|----------------|------------|-----------|-----------------|
| Hive (CLN) | 3 | 1,017 | 356 sats | **74.0%** | 0.35 sats |
| External (CLN) | 4 | 285 | 485 sats | 20.7% | 1.70 sats |
| External (LND) | 8 | 71 | 251 sats | 5.2% | **3.54 sats** |

**Key Findings:**
1. Hive nodes routed **74%** of all forwards (1,017 of 1,373 total)
2. **2x revenue improvement** - 356 sats with 75 ppm floor vs ~178 sats with 10 ppm floor
3. lnd2's aggressive fee strategy (100-1000 ppm) earned **10.57 sats/forward** - highest margin
4. dave earned highest total fees (460 sats) due to 500 ppm CLBOSS default + high volume
5. niaj (Lazy config) started routing - 21 forwards at 0.95 sats/forward
6. Zero inter-hive fees enable efficient internal routing, boosting volume

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

### Completed ✅
- ~~Add more LND nodes~~ - Network now has 8 LND (53%), matching real-world ~55%
- ~~Vary charge-lnd configs~~ - 8 unique fee strategies implemented
- ~~Optimize hive fee strategy~~ - 0 ppm inter-hive, 75 ppm min external
- ~~Run comprehensive test~~ - 30-minute test with 1,373 forwards completed

### Improving LND Routing Participation
1. **Most new LND nodes not routing** - judy, kathy, lucy, mike, quincy still at 0 forwards
2. **niaj started routing** - 21 forwards shows Lazy config (75-300 ppm) is competitive
3. Need to position remaining LND nodes on primary routing paths
4. Consider opening channels from LND nodes directly to payment sources/destinations

### Fee Strategy Insights
1. **Volume strategy (hive):** 74% of traffic, 356 sats total, 0.35 sats/forward
2. **Margin strategy (lnd2):** 1.4% of traffic, 201 sats total, 10.57 sats/forward
3. **Balanced approach (dave):** 13% of traffic, 460 sats total, 2.56 sats/forward
4. Optimal strategy depends on channel position and liquidity management goals

### For Production Deployment
1. Monitor channel balance drift with high-volume routing
2. Consider automatic rebalancing triggers when liquidity becomes unbalanced
3. Test fee adjustments in response to sustained one-directional flow
4. Evaluate if 75 ppm floor is optimal or should be adjusted based on competition

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

*Report generated by cl-revenue-ops simulation suite v1.4*
*Last updated: 2026-01-11 - 30-minute comprehensive test with optimized fee strategy (0/75 ppm)*
