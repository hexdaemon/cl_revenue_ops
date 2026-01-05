# Phase 9.3 Spec: The Guard (Economics & Governance)

| Field | Value |
|-------|-------|
| **Focus** | Membership Lifecycle, Incentives, and Ecological Limits |
| **Status** | **APPROVED** |

---

## 1. Internal Economics: The Two-Tier System

To prevent "Free Riders" and ensure value accretion, The Hive utilizes a tiered membership structure. Access to the "Zero-Fee" pool is earned, not given.

### 1.1 Neophyte (Probationary Status)
**Role:** Revenue Source & Auditioning Candidate.
*   **Fees:** **Discounted** (e.g., 50% of Public Rate). They pay to access Hive liquidity but get a better deal than the public.
*   **Rebalancing:** **Pull Only.** Can request funds (paying the discounted fee) but does not receive proactive "Push" injections.
*   **Data Access:** **Read-Only.** Receives topology data (where to open channels) but is excluded from high-value "Alpha" strategy gossip.
*   **Duration:** Minimum 30-day evaluation period.

### 1.2 Full Member (Vested Partner)
**Role:** Owner & Operator.
*   **Fees:** **Zero (0 PPM)** or Floor (10 PPM). Frictionless internal movement.
*   **Rebalancing:** **Push & Pull.** Eligible for automated inventory load balancing.
*   **Data Access:** **Read-Write.** Broadcasts strategies, votes on bans, receives "Alpha" immediately.
*   **Governance:** Holds signing power for new member promotion.

---

## 2. The Promotion Protocol: "Proof of Utility"

Transitioning from Neophyte to Member is an **Algorithmic Consensus** process, not a human vote. A Neophyte requests promotion via `HIVE_PROMOTION_REQUEST`. Existing Members run a local audit:

### 2.1 The Value-Add Equation
A Member signs a `VOUCH` message only if the Neophyte satisfies **ALL** criteria:

1.  **Reliability:** Uptime > 99.5% over the 30-day probation. Zero "Toxic" incidents (no dust attacks, no jams).
2.  **Contribution Ratio:** Ratio > 1.0. The Neophyte must have routed *more* volume for the Hive than they consumed from it.
3.  **Topological Uniqueness (The Kicker):**
    *   Does the Neophyte connect to a peer the Hive *doesn't* already have?
    *   **YES:** High Value (Expansion) -> **PROMOTE**.
    *   **NO:** Redundant (Cannibalization) -> **REJECT** (Remain Neophyte).

### 2.2 Consensus Threshold
Once a Neophyte collects `VOUCH` signatures from **51%** of the active fleet (or a fixed quoru
