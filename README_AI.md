# IOWarp vs Mofka Benchmarking Project

## Overview & Purpose

The primary objective of this project is to create a robust benchmarking suite to compare the streaming and I/O performance of **Mofka** (a distributed event-streaming and messaging framework based on Mochi) against **IOWarp Core** (specifically its Context Transfer Engine and high-performance Context Runtime). 

Given that both frameworks target high-performance computing (HPC) environments, this benchmarking effort evaluates critical streaming metrics such as:
- **Throughput** (MB/s and Events/sec)
- **Latency** for data ingestion and retrieval
- **Scalability** across multiple threads and nodes
- **Resource utilization** during high-volume streaming workflows

By packaging these tests, developers and researchers can easily orchestrate comparative runs and generate visualized performance reports to identify the optimal framework for various data-intensive scenarios.

## Repository Structure

Relevant components for this specific benchmarking effort are organized as follows:

### 1. Standalone Benchmarks
`test/mofka_benchmark/`
This directory holds the core raw benchmark scripts that interact directly with Mofka's API (specifically its Python client API).
- `server.sh`, `env.sh`, `config.json`: Environment setup and server orchestration for Mofka.
- `producer.py` / `producer.sh`: Benchmarks data ingestion rates, pushing configurable batches of data and metadata to Mofka topics.
- `consumer.py` / `consumer.sh`: Benchmarks data retrieval and consumption rates from Mofka topics.

### 2. Jarvis Orchestration Modules (To Be Built)
`jarvis_iowarp/jarvis_iowarp/mofka_bench/`
This is the target directory for the Mofka Jarvis package. Jarvis (jarvis-cd) is used to deploy distributed systems, execute coordinated tasks, and collect metrics seamlessly across clusters. The upcoming Jarvis package for Mofka should wrap the standalone benchmark scripts, implementing components like `_get_stat` (to parse throughput and latency metrics from the scripts' output) and `_plot` (to automatically generate performance comparison figures).

### 3. Performance Pipelines (To Be Built)
`jarvis_iowarp/pipelines/performance/`
This is the target location for orchestrated pipeline definition scripts (YAML). These pipeline scripts will define the end-to-end testing workflow:
1. Deploying the Mofka server.
2. Running the Mofka producer/consumer tasks under varying configurations.
3. Gathering statistics and plotting results.
They will be executable using `jarvis ppl run yaml [script_path]`.

### 4. IOWarp Core Components
`/workspace/context-*` directories
These directories contain the core C++ codebase for IOWarp (e.g., Context Runtime, Context Transfer Engine, Context Assimilation Engine). While the focus here is benchmarking Mofka, IOWarp benchmarks (like `wrp_cte_bench` and `wrp_run_thrpt_benchmark`) already exist in these folders to serve as the baseline comparison.

## AI Task Instructions (Context for Claude)

You are tasked with building the automated benchmarking orchestration for Mofka via Jarvis.

**Key Requirements for Your Task:**
1. **Build the Mofka Jarvis Package** in `jarvis_iowarp/jarvis_iowarp/mofka_bench/`.
   - Wrap the logic from `test/mofka_benchmark/` (such as starting the server, running producers/consumers).
   - Implement `_get_stat` to extract throughput and event metrics from the standard output of `producer.py` and `consumer.py`.
   - Implement `_plot` to logically visualize these metrics (e.g., Throughput vs. Number of Threads/Event Size).
2. **Create the Performance Pipeline** in `jarvis_iowarp/pipelines/performance/`.
   - Provide a declarative YAML test script that executes the Jarvis package.
   - Verify that this pipeline can be executed successfully using `jarvis ppl run yaml [script_path]`.
