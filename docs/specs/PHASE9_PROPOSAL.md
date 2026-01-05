# Phase 9 Proposal: "The Hive"
**Distributed Fleet Coordination & Virtual Centrality**

| Field | Value |
|-------|-------|
| **Target Version** | v2.0.0 |
| **Previous Concept** | Liquidity Dividend System (LDS) |
| **New Concept** | **The Hive** (PKI-Based Fleet Coordination) |
| **Objective** | Transform isolated nodes into a unified, high-frequency routing cartel using cryptographic coordination. |

---

## 1. Executive Summary

The **Liquidity Dividend System (LDS)** proposed a "Managed Fund" model where investors deposited capital into a central node. While economically potent, it carried unacceptable regulatory risks (custody) and technical fragility (solvency deadlocks).

**"The Hive"** is the strategic pivot. Instead of physical centralization, we achieve **Virtual Centrality**.

The Hive is a protocol that allows independent nodes—whether owned by one operator or a trusted consortium—to authenticate one another, coordinate fee strategies, and share liquidity without friction. It effectively turns a fleet of small nodes into a **Single Logical Entity** capable of out-competing larger, isolated hubs.

---

## 2. Strategic Pivot: Solving the LDS Pitfalls

The Hive was architected specifically to neutralize the risks identified in the LDS Red Team Audit while retaining the economic upside.

| Issue | The LDS Failure Mode | The Hive Solution |
| :--- | :--- | :--- |
| **Custody** | **High Risk.** Operator holds keys for LPs. Regulated as Money Transmission. | **Solved.** LPs run their own nodes/keys. The Hive is just a communication protocol between them. |
| **Liability** | **High.** If the central node is hacked, all LP funds are lost. | **Solved.** Funds are distributed. A hack on one node does not compromise the others. |
| **Solvency** | **Fragile.** "Runs on the bank" could lock up the central node. | **Robust.** There is no central bank. Nodes trade liquidity bilaterally. |
| **Regulation** | **Security.** "Investment contract" via pooled profits. | **Trade Agreement.** "Preferential Routing" between independent peers (Standard business practice). |

---

## 3. The Alpha Opportunities (Strategic Advantages)

By coordinating, The Hive unlocks yield strategies that are mathematically impossible for isolated nodes.

### 3.1 Zero-Cost Capital Teleportation
**The Problem:** Moving liquidity from Node A to Node B currently incurs market routing fees, eroding ROE.
**The Hive Solution:** Authenticated Fleet Members whitelist each other for **0-Fee Routing**.
*   **Impact:** Capital becomes "super-fluid." Liquidity can instantly move to whichever node has the highest demand without friction cost, effectively pooling the entire fleet's balance virtually.

### 3.2 Inventory Load Balancing ("Push" Rebalancing)
**The Problem:** Rebalancing is currently reactive ("Pull"). A node waits until it is empty to ask for funds, often missing payments in the interim.
**The Hive Solution:** Proactive "Push."
*   **Scenario:** Node A has excess idle liquidity. Node B is seeing high velocity.
*   **Action:** Node A proactively circular-routes funds to Node B *before* Node B runs dry.
*   **Result:** Zero downtime for high-demand channels.

### 3.3 The "Borg" Defense (Shared Intelligence)
**The Problem:** If Peer X attacks Node A (Dust flood, HTLC jamming), Node B is unaware and remains vulnerable.
**The Hive Solution:** Distributed Reputation Table.
*   **Action:** Node A broadcasts `PEER_BAN: [Pubkey_X]`.
*   **Result:** All Hive members pre-emptively blacklist Peer X via the `revenue-ignore` logic.

### 3.4 Dynamic Splicing Optimization
**The Problem:** Static channels trap capital.
**The Hive Solution:** The Hive enforces technical standards (e.g., Splicing support).
*   **Action:** The Capacity Planner identifies a "Winner" on Node A and a "Loser" on Node B.
*   **Execution:** The Hive orchestrates a Splice-Out on Node B, sends funds on-chain to Node A, and Splices-In on Node A.
*   **Result:** Automated capital reallocation across the fleet without closing channels completely.

---

## 4. Protocol Architecture: Signed Manifests (PKI)

To maintain security and quality control, The Hive uses a **Public Key Infrastructure (PKI)** system. Nodes do not just "join"; they must be invited and **Certified** to meet the fleet's standards.

### 4.1 The "Hive Ticket" (The Invitation)
An Admin Node generates a time-limited, cryptographically signed token authorizing entry.

**Command:** `lightning-cli revenue-hive-invite --valid-hours=24 --req-splice --req-version=1.4`
*   *Output:* A signed blob containing constraints and the Admin's signature.

### 4.2 The Handshake & Certification Flow
When a **Candidate Node (A)** connects to an existing **Member Node (B)**:

1.  **Connection:** Node A connects via BOLT 8 (Encrypted Transport).
2.  **Discovery:** Node A sends `HIVE_HELLO` containing the **Hive Ticket**.
3.  **Challenge:** Node B sends `HIVE_CHALLENGE` with a random `nonce`.
4.  **Attestation (The Manifest):** Node A constructs a JSON Manifest proving it meets the criteria:
    ```json
    {
      "pubkey": "Node_A_Key",
      "version": "cl-revenue-ops v1.4.2",
      "features": ["splice", "dual-fund"],
      "nonce_reply": "signed_nonce_from_step_3"
    }
    ```
5.  **Proof:** Node A signs the Manifest and sends it to B.
6.  **Certification (The Audit):** Node B acts as the Gatekeeper:
    *   **Verify Ticket:** Is it signed by a trusted Admin? Is it expired?
    *   **Verify Identity:** Does the signature match Node A?
    *   **Verify Requirements:** Does Node A actually have Splicing enabled? Is it running the correct version?
7.  **Adoption:** If certified, Node B adds Node A to its local `fleet_nodes` database.

### 4.3 Active Capability Probing ("Trust but Verify")
To prevent spoofing (where a node *claims* to have Splicing but doesn't), the verifying node performs an **Active Probe**.
*   *Before* granting 0-fee status, Node B attempts a harmless technical negotiation (e.g., `splice_init`).
*   If the Candidate fails the probe, the Manifest is rejected as **Fraudulent**.

---

## 5. Governance & Defense

### 5.1 The "Immune System" (Global Blacklist)
The Hive maintains a shared `global_ignore_list`.
*   **Trigger:** Any node detects toxic behavior.
*   **Propagation:** A `HIVE_BAN_PEER` message is signed and broadcast.
*   **Consensus:** Nodes accept Ban messages *only* from authenticated, certified fleet members.

### 5.2 Granular Revocation
If a fleet member is compromised or behaves erratically:
*   **Action:** The Admin broadcasts `HIVE_REVOKE: [Pubkey_A]`.
*   **Result:** All nodes immediately strip "Friendly" status from Node A. Because we use PKI, we do not need to rotate shared passwords on the rest of the fleet.

---

## 6. Risk Mitigation (Red Team Analysis)

| Risk Class | Specific Threat | Mitigation Strategy |
| :--- | :--- | :--- |
| **Manifest Spoofing** | Attacker claims to run v1.4 but runs malicious code. | **Active Probing:** The verifier tests capabilities before acceptance. **Logic Bounds:** Local safety constraints (Floor Fees) always override Swarm suggestions. |
| **Replay Attack** | Attacker sniffs a valid handshake and replays it. | **Nonce Challenge:** The signature must include a random `nonce` generated by the Verifier, valid only for that specific millisecond. |
| **Ticket Theft** | Attacker steals an unused Invite Ticket. | **Short Expiry:** Tickets expire quickly (e.g., 1 hour). **One-Time Use:** Admins can enforce single-use nonces. |
| **Compromised Member** | A member node turns malicious. | **Revocation List:** Admins can cryptographically ban specific pubkeys from the fleet instantly. |

---

**Recommendation:**
Phase 9 "The Hive" represents the mature, enterprise-grade evolution of the project. It solves the legal/custodial blockers of LDS while providing a robust framework for managing high-value node fleets. **Approve for development post-v1.4.**
