# BigQuery & DataFusion Superpowers: Feature Ranking & Implementation Plan

This plan evaluates and ranks advanced analytical capabilities inspired by **Google BigQuery** (product & AI superpowers) and **Apache DataFusion** (high-performance in-memory SQL superpowers). It provides a clear, ranked roadmap for transforming **DoubleDecker** from a basic query builder into an enterprise-grade, guided analytics workbench for smaller businesses.

---

## User Review Required

> [!IMPORTANT]
> **Choose Your Priority Tier**: Please review the ranked feature tiers below and let me know which tier (or combination of individual features) you would like to implement first!

---

## Ranked Feature Catalog

We have evaluated 9 major features across three priority tiers based on **Business Impact**, **User Value**, and **Implementation Ease**.

### 🏆 Tier 1: Immediate High-Impact Business Wins (Recommended First Step)
*These features require 0 backend changes or new heavy libraries, but make the query builder feel 10x more capable for daily business reporting.*

| Rank | Feature Name | Source | Description | Why It Matters | Implementation Effort |
| :---: | :--- | :---: | :--- | :--- | :---: |
| **#1** | **Date & Time Bucketing** | *DataFusion* | Add `DATE_TRUNC` transforms to bucket timestamps by Day, Week, Month, Quarter, or Year. | **#1 chart requirement.** Businesses analyze revenue by Month or Quarter, never by exact timestamp. | **Low** (UI dropdown + simple transform rule) |
| **#2** | **Count Distinct (`COUNT DISTINCT`)** | *DataFusion* | Add `Count Distinct` to Group By aggregations. | Essential for e-commerce & SaaS (*"How many unique customers bought X?"* vs total items). | **Low** (Add to `AggFunc` enum & preparers) |
| **#3** | **Expanded Business Filter Operators** | *BigQuery / SQL* | Add `Is Empty / Null`, `Is Not Empty`, `One Of (In List)`, and `Between`. | Eliminates dirty data and simplifies multi-value filtering without tedious `OR` chains. | **Low** (Add to `FilterOp` enum & UI inputs) |

---

### 🚀 Tier 2: Advanced Analytical Engine Superpowers
*These features unlock analytical depth normally reserved for enterprise data scientists, brought to business users via simple UI toggles.*

| Rank | Feature Name | Source | Description | Why It Matters | Implementation Effort |
| :---: | :--- | :---: | :--- | :--- | :---: |
| **#4** | **Pivot Mode (2D Cross-Tabulation)** | *BigQuery* | Turn a 1D Group By result into an Excel-style 2D matrix (e.g., Months on X, Regions as columns). | Business users are trained on Excel pivots. This bridges the gap between SQL and spreadsheets. | **Medium** (Frontend matrix pivot helper & UI toggle) |
| **#5** | **Running Totals & Growth % (Window Functions)** | *DataFusion* | Add `Cumulative Sum` and `Period-over-Period Growth %` (`LAG/LEAD`) to Transform/Aggregation. | Unlocks "Year-to-Date Revenue" charts and "MoM Churn Rate" without writing complex SQL. | **Medium** (Add window logic to dataAggregation / engine) |
| **#6** | **Conditional Categorization (CASE WHEN / IF)** | *SQL* | Let users build custom buckets (*If amount > $1000 then 'VIP', else 'Standard'*). | Enables customer segmentation and custom reporting tiers directly in the UI. | **Medium** (New UI builder in TransformModal) |

---

### 🧠 Tier 3: BigQuery Product & AI Superpowers
*These features elevate the product experience with intelligence, automation, and confidence-boosting guidance.*

| Rank | Feature Name | Source | Description | Why It Matters | Implementation Effort |
| :---: | :--- | :---: | :--- | :--- | :---: |
| **#7** | **Instant Query Impact Preview ("Explain")** | *BigQuery* | Show a live badge: *"Will summarize 150k rows into ~12 categories in <0.1s"* before running. | Removes "query anxiety" for non-technical users by showing expected size and speed. | **Medium** (Calculate cardinality metrics in UI state) |
| **#8** | **One-Click AI Trend Forecasting** | *BigQuery ML* | Add a **"✨ Add Forecast (Next 3 Periods)"** toggle on Line & Bar charts. | Gives small business owners instant future visibility without hiring a data scientist. | **Medium** (Frontend linear regression / moving average) |
| **#9** | **Smart Data Quality Profiling Warnings** | *BigQuery* | Auto-flag data anomalies: *"⚠️ Column 'email' has 14% missing values"* when selected. | Guides users toward clean data before they build broken charts. | **High** (Requires background profiling pass on dataset) |

---

## Open Questions

> [!IMPORTANT]
> **Which Tier should we implement now?**
> 1. **Option A (Recommended):** Start with **Tier 1 (#1 Date Bucketing, #2 Count Distinct, and #3 Expanded Filters)** to instantly level up our core reporting power.
> 2. **Option B:** Jump straight into **Tier 2 (#4 Pivot Mode & #5 Running Totals)** for advanced spreadsheet-like power.
> 3. **Option C:** Implement **Tier 3 (#7 Query Impact Preview & #8 AI Trend Forecasting)** to wow users with BigQuery-style intelligence.

---

## Proposed Changes (For Recommended Tier 1)

If we proceed with **Tier 1**, we will modify the following components:

### Query Builder UI & Types

#### [MODIFY] [FilterModal.tsx](file:///c:/Users/User/Documents/doubledecker-FE/src/components/QueryBuilder/FilterModal.tsx)
- Expand `FilterOp` type to include `"IsNull" | "IsNotNull" | "In" | "Between"`.
- Update operator selection dropdown with human-readable labels (`"Is empty"`, `"Is not empty"`, `"Is one of"`, `"Is between"`).
- Render specialized input fields (e.g., dual inputs for `Between`, tag-input for `In`, hide value input for `IsNull`).

#### [MODIFY] [AggregationModal.tsx](file:///c:/Users/User/Documents/doubledecker-FE/src/components/QueryBuilder/AggregationModal.tsx)
- Expand `AggFunc` type to include `"CountDistinct" | "Median"`.
- Add `"Count Distinct (Unique Values)"` and `"Median (Middle Value)"` to the UI dropdown.

#### [MODIFY] [TransformModal.tsx](file:///c:/Users/User/Documents/doubledecker-FE/src/components/QueryBuilder/TransformModal.tsx)
- Expand `TransformOp` type to include `"DateTruncYear" | "DateTruncMonth" | "DateTruncWeek" | "DateTruncDay"`.
- Add a new "Date & Time Bucketing" category in the transformation selector so users can easily group timestamps.

#### [MODIFY] [OperationsSidebar.tsx](file:///c:/Users/User/Documents/doubledecker-FE/src/components/QueryBuilder/OperationsSidebar.tsx)
- Update `operatorLabels` and `transformLabels` dictionaries to display friendly badges for the new operators in the sidebar cards.

---

### Data Processing Engine

#### [MODIFY] [dataAggregation.ts](file:///c:/Users/User/Documents/doubledecker-FE/src/utils/dataAggregation.ts)
- Update aggregation engine to calculate `Set.size` when `CountDistinct` is requested.
- Update transform execution logic to slice/format date strings when date bucketing transforms are encountered.

---

## Verification Plan

### Automated Tests
- Run unit tests to verify new operators and aggregations:
  ```bash
  npm run test
  ```
- Add test assertions for:
  1. `CountDistinct` returning exact unique counts on datasets with duplicates.
  2. Date bucketing correctly truncating ISO timestamps to Month/Year strings.
  3. `IsNull` and `Between` filters correctly filtering rows in `dataAggregation.ts`.

### Manual Verification
1. Open Query Builder in `npm run dev`.
2. Apply a **Date Bucket** transform on a date column -> group by the new bucketed column -> verify monthly/quarterly summaries.
3. Apply a **Count Distinct** aggregation on customer IDs -> verify unique counts are displayed.
