# cl-revenue-ops: Liquidity Dividend System (LDS)
**Technical Specification & Implementation Roadmap v1.0**

## 1. Project Vision
The LDS transforms a Core Lightning (CLN) node into a **Community-Funded Market Maker**. It allows third-party contributors to provide "passive capital" to the node’s global liquidity pool. In exchange, they receive a pro-rata share of the **Realized Net Profit**, calculated after all rebalancing and operational expenses are deducted.

---

## 2. Core Economic Philosophy: The Unified Pool
Unlike "Channel Leasing" where a user’s risk is tied to a specific peer, the LDS utilizes a **Unified Risk-Averaged Pool**.

*   **Socialized Operational Risk:** User capital is allocated dynamically via `clboss` and `sling`. Rebalancing costs and channel opening fees are deducted from the **Global Pool Profit**.
*   **Decoupled Allocation:** The Node Operator (and the `cl-revenue-ops` algorithms) maintains full control over where capital is deployed.
*   **Protection against "Bad Decisions":** Because profit/loss is averaged across the entire node, users are not penalized if one specific channel fails, provided the node's overall strategy remains profitable.

### The Dividend Formula
$$Dividend_i = \left( Net\_Profit \times \frac{User\_TWAB_i}{Node\_Total\_Capital} \right) \times Lock\_Rebate\_Factor$$

*   **Net Profit:** `Fees_Earned - (Rebalance_Costs + Amortized_Open_Costs)`.
*   **User TWAB:** Time-Weighted Average Balance over the last 72 hours.
*   **Lock Rebate Factor:** The percentage of the Node's Management Fee returned to the user.

---

## 3. Dynamic Multipliers: The "Management Fee Rebate" Model
To eliminate "Node Bleed" (paying out more than earned), multipliers are funded exclusively by rebating the **Node Management Fee (Carry)**.

**System Default:** The Node Operator takes a **20% Carry** on all net profits.

| Lock Duration | Node Carry | User Payout | Multiplier Label |
| :--- | :--- | :--- | :--- |
| **0 Days (Liquid)** | 20% | 80% of Pro-rata | 0.8x |
| **30 Days** | 10% | 90% of Pro-rata | 0.9x |
| **90 Days** | 0% | 100% of Pro-rata | 1.0x |
| **180 Days** | 0% | 100% of Pro-rata | 1.0x (Cap) |

*By capping payouts at 100% of realized profit, the node operator ensures the principal is never touched to pay interest.*

---

## 4. Adversarial Protection (Red Team Audit)

| Attack Vector | Description | Technical Mitigation |
| :--- | :--- | :--- |
| **Yield Sniping** | Depositing 1 BTC just before the payout loop to capture "Whale" routing profits. | **72h TWAB:** Dividends are based on the average balance over 3 days. Flash-deposits earn near-zero yield. |
| **The Bank Run** | Users withdraw 100% of capital during an L1 fee spike or force-close storm. | **Liquidity Buffer:** LNbits restricts instant withdrawals to 10% of total pool. Excess is queued for 24h rebalancing. |
| **Virtual Inflation** | Logic bug pays out "Virtual sats" in LNbits that don't exist in CLN channels. | **Master Audit:** System halts if `Virtual Liabilities > 85% of Physical Local Balance`. |
| **Decision Decay** | Node operator over-spends on rebalancing, making net profit negative. | **High Water Mark (HWM):** Losses are carried forward. No dividends are paid until all past OpEx is recovered. |

---

## 5. Technical Architecture

### Layer 1: LNbits "Vault" Extension (The Ledger)
*   **Hard-Lock Enforcement:** A dedicated extension that flags wallets as `LDS_INVESTOR`. It hooks into LNbits payment logic to block any `OUT` transaction if `now < lock_expiry`.
*   **Investor Dashboard:** Displays "Current Yield," "Active Multiplier," and "Time until Unlock."
*   **TWAB Engine:** Hourly snapshots of investor balances stored in a local SQLite table.

### Layer 2: `cl-revenue-ops` LDS Driver (The Strategist)
*   **Solvency Engine:** Queries CLN `listfunds` and compares physical "Local Balance" against the LNbits `Total_Liabilities`.
*   **Payout Orchestrator:** Calculates the daily delta in `lifetime_net_profit`. If positive, it issues an internal transfer from the Node Master Wallet to the LDS Master Wallet.

---

## 6. Implementation Roadmap (AI Prompts)

### Phase 1: The Solvency & TWAB Driver
> "Update `modules/database.py` to support LDS tracking. Create an `lds_snapshots` table to record wallet balances every hour. Implement `get_72h_twab(wallet_id)`. Then, in `cl-revenue-ops.py`, create a `verify_solvency()` function that aborts the payout loop if total virtual liabilities exceed 85% of the physical local balance found in CLN `listfunds`."

### Phase 2: The LNbits Extension (Spend Guard)
> "Build an LNbits extension called 'LDS Vault'. Create a setting to mark a wallet as 'LOCKED'. Implement a middleware hook in FastAPI to intercept `POST /api/v1/payments`. If the source wallet is LOCKED and the `lock_expiry` hasn't passed, return a 403 error: 'Capital is currently deployed in routing channels and is time-locked'."

### Phase 3: The Profit Distribution Loop
> "Implement the `distribute_dividends` loop in `cl-revenue-ops.py`. It should: 1. Calculate Net Profit since the last successful payout. 2. For each investor wallet, calculate their TWAB-based share. 3. Apply the MFR (Management Fee Rebate) based on their lock tier. 4. Use the LNbits API to credit the user's wallet. 5. Update a `global_high_water_mark` in the DB to ensure losses are recovered before the next payout."

---

## 7. Team Review: Points for Refinement
1.  **Exit Strategy:** Should we offer an "Emergency Exit" button that allows users to break a lock for a 5% "Slashing Penalty"? This penalty would stay in the node's reserve.
2.  **Pool Density:** Do we limit the pool size? (e.g., "The node will not accept more than 2 BTC total in contributions").
3.  **Operator Guarantee:** Does the operator guarantee the return of principal in the event of a massive force-close that exceeds the reserve fund? (Recommendation: Terms of Service should state that users share in the risk of on-chain enforcement loss).