# Data In Motion — Building the Future of Intelligent Cloud Storage

This repository contains our hackathon project for the NetApp "Data In Motion" challenge. The goal is to design an intelligent data storage solution that improves placement, performance, and cost management for hybrid and multicloud environments.

## Challenge Overview

Organizations that rely on hybrid and multicloud deployments face performance, cost, and regulatory challenges as data moves between workloads, storage tiers, and locations. The objective of the hackathon is to build a solution that continuously assesses datasets, learns from usage patterns, and recommends the optimal storage target—whether cloud, on-premises, hot, warm, or cold tiers—while also ensuring security and compliance.

## Solution Requirements

Your solution should:

1. **Assess Dataset Needs** – Automatically determine when a dataset should reside in hot, warm, or cold storage.
2. **Learn Continuously** – Capture usage patterns, cost characteristics, and compliance requirements in real time and update policies accordingly.
3. **Optimize Costs** – Select storage tiers that balance performance and budget, both for past activity and future forecasts.
4. **Enhance Security and Compliance** – Protect sensitive data, understand data location requirements, and surface remediation steps for issues.
5. **Predict Failures** – Monitor storage health and leverage AI-driven insights to recommend preventative actions, backup plans, and disaster recovery strategies.
6. **Use the Provided Dataset** – Analyze the NetApp dataset containing telemetry, utilization, and anomaly records to build data placement recommendations.
7. **Showcase Your Work** – Present your architecture, demonstrate the solution, and document the approach.

## Expected Deliverables

Participants must provide:

- Slides that introduce the team, solution domain knowledge, architecture, and future roadmap.
- A working demo or video walkthrough.
- Source code in this repository.
- Models or notebooks for machine learning components.

## Judging Criteria

Projects will be evaluated on the following dimensions:

| Criterion | Weight |
| --- | --- |
| Business Impact & Relevance | 20% |
| Innovation | 20% |
| Technical Complexity | 20% |
| Completion & Clarity of Thought | 20% |
| Alignment to NetApp's Domain | 10% |
| Presentation | 10% |

## Why It Matters

In the era of exponential data growth, organizations must keep data available, compliant, performant, and affordable across diverse infrastructure. "Data In Motion" challenges participants to break the trade-offs between cost and performance by building intelligent, automated storage decisions with cloud APIs, AI-driven insights, and proactive remediation capabilities.


## Predictive Insights Roadmap

To implement proactive storage placement recommendations, consider the following model families and key parameters:

### 1. Time-Series Forecasting
Use historical access counts and telemetry per file or bucket to forecast future demand.
- **Prophet**: Models seasonality and trend with minimal tuning. Key parameters: `changepoint_prior_scale` (trend flexibility), `seasonality_mode` (`additive` vs `multiplicative`), custom seasonalities for weekday/hour cycles.
- **ARIMA/SARIMA**: Classical statistical model that handles autoregressive and moving-average behavior. Tune `(p, d, q)` orders and seasonal `(P, D, Q, s)` terms; use `auto_arima` to search combinations.
- **LSTM/Temporal Fusion Transformer**: Deep learning approaches for multivariate time-series with exogenous features (latency, cost). Key hyperparameters include number of layers, hidden size, dropout rate, attention heads, and lookback window length.

### 2. Predictive Classification
Predict whether an object should move tiers within a horizon based on features like access frequency, response-time SLA, storage cost, compliance flags, and anomaly scores.
- **Gradient Boosted Trees (XGBoost/LightGBM/CatBoost)**: Strong tabular learners. Important parameters: `max_depth`, `n_estimators`, `learning_rate`, `subsample`, `colsample_bytree`, class weighting for imbalanced data.
- **Logistic Regression with Elastic Net**: Provides explainable coefficients for policy transparency. Tune `C` (inverse regularization strength) and `l1_ratio` to balance sparsity and smoothness.
- **Random Forest**: Baseline ensemble capturing non-linear interactions. Adjust `n_estimators`, `max_features`, `max_depth`, and `min_samples_split` to control overfitting.

### 3. Anomaly & Hotspot Detection
Detect sudden spikes or anomalous patterns that justify immediate moves.
- **EWMA/Shewhart Control Charts**: Lightweight statistical monitors using rolling mean (`window_size`) and control limit multipliers (`k`).
- **Isolation Forest**: Tree-based anomaly detector for high-dimensional telemetry. Tune `n_estimators`, `max_samples`, and `contamination` to match expected anomaly rate.
- **DBSCAN/OPTICS Clustering**: Identify dense clusters of high activity across devices or regions. Key parameters: `eps` (distance threshold) and `min_samples` (cluster density).

### Operational Considerations
- Maintain feature pipelines that aggregate per-object usage over configurable windows (e.g., 1h, 6h, 24h).
- Track model confidence thresholds to trigger automated vs. advisory moves (`probability_cutoff`, `forecast_margin`).
- Retrain on a cadence aligned with data drift (daily/weekly) and capture model metrics (precision/recall, MAPE) for dashboards.
- Store model artifacts and metadata for reproducibility, enabling rollback of underperforming versions.

## Consistency & Availability Safeguards

To meet the "Ensure Data Consistency and Availability" requirement the platform now layers distributed-systems guardrails on top of the predictive tiering workflows:

- **Optimistic concurrency with versioning** – every dataset document carries a monotonically increasing `version`. Updates use compare-and-set semantics, retrying automatically when a conflicting write is detected and logging a `sync_conflict` event that also feeds the ML features.
- **Replica-aware sync manager** – the API boots a `ConsistencyManager` that tracks remote replica endpoints (configurable via `REPLICA_ENDPOINTS`), pushes metadata diffs, and records the outcome in a dedicated `sync_queue` collection. Pending items can be reviewed at `/consistency/status` or replayed with `/consistency/resync`.
- **Graceful degradation on network failure** – if Kafka, storage, or replica calls fail, the manager captures an `availability_error`, increments reliability features (e.g. `network_failures_last_hour`), and queues the change for later replay instead of dropping it.
- **Seed-time alignment** – startup seeding stamps every document with a `sync_state` snapshot so new environments begin in a known-good state, and the manager reconciles any unfinished replications as part of bootstrapping.

These enhancements keep tiering recommendations, migrations, and telemetry-derived features aligned across distributed deployments even when the network is unreliable.

## High-Volume Simulation & Streaming Telemetry

To showcase the predictive tiering loop end-to-end the API now includes a synthetic workload engine that can be toggled entirely in software. The simulator continuously sends rich read/write events, posts device telemetry to the stream API, and triggers opportunistic migrations so that the dashboards show non-zero Kafka throughput even on a laptop with no external dependencies. Key capabilities include:

- **Background load generator** – the API bootstraps a daemon thread at startup (`ENABLE_SYNTHETIC_LOAD=0` disables it) that writes bursty access patterns, latency spikes, and replica-conflict flags into MongoDB while mirroring the events into the Kafka-compatible stream API. The Streamlit dashboard therefore visualizes live charts without waiting for an external producer.
- **On-demand burst endpoint** – call `POST /simulate/burst` with optional `events`, `file_ids`, or `include_moves` flags to drive thousands of synthetic reads/writes or migrations immediately. This is handy for demos or to sanity-check the ML model after tweaking features.
- **Streaming metrics API** – `GET /streaming/metrics` now exposes throughput-per-minute, active device counts, and recent events directly from MongoDB so the UI can still plot activity when the Kafka emulator is offline.
- **Auto-refreshing dashboard** – the Streamlit mission control page injects a 15-second meta refresh so judges always see up-to-date throughput, predictions, and alerts with zero manual clicks.

Together these additions satisfy the request to simulate large data transfers, verify ML-driven tiering under heavy load, and keep Kafka telemetry visibly flowing throughout the hackathon demo.

