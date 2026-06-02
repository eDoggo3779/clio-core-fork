# Project Identity

This repository is a personal fork of `iowarp/clio-core` (upstream: `https://github.com/iowarp/clio-core`), maintained by a student engineer at IIT's Gnosis Research Center. It serves as a sandbox for benchmarking and feature development on the IOWarp project.

The `upstream` remote is configured and points to `iowarp/clio-core`. `origin` points to the fork on GitHub.

## Current Projects

1. **CTE Benchmarks**: Benchmark IOWarp's Content Transfer Engine (an IO module) using the Jarvis framework. Goals: time-based (not IO-count-based) benchmarks over TCP, SHM, single-node, and multi-node configurations. Validated on personal machine and IIT's Ares supercomputer.

2. **Mofka Benchmarks**: Benchmark Mofka (a streaming service, separate from IOWarp) using the Jarvis framework. Goals: time-based benchmarks over TCP, RDMA, single-node, and multi-node. Results will be used to compare Mofka and CTE. Validated on personal machine and Ares.

3. **Windows CLIO Filesystem (Issue #463)**: Implement a Windows 11 equivalent of the existing `libfuse` adapter so CLIO can be used as a filesystem on Windows. WinFsp is the candidate tool. Low-complexity task per project lead.

## State of the Repository

`main` is a clean sync of upstream `iowarp/clio-core` as of 30 May 2026.

Four branches exist and are **ARCHIVED — do not write to them**. Use them as read-only reference:
- `claude/mofka-multinode` — multi-node Mofka benchmark work
- `claude/mofka-rdma` — RDMA transport work for Mofka
- `claude/mofka-loud-fail` — failure isolation experiments (5 commits unpushed to origin)
- `claude/cte-failure-isolation` — CTE bug isolation (2 commits behind origin; those commits are safe on GitHub)

## Key Vocabulary

| Term | Meaning |
|------|---------|
| **CTE** | Context Transfer Engine — IOWarp's IO module |
| **Mofka** | A streaming service (uses Mercury/Margo); separate from IOWarp |
| **Jarvis** | Framework used to define, configure, and run benchmarks in this project |
| **Ares** | IIT's Slurm-managed supercomputer; used for multi-node validation |
| **CAE** | Context Assimilation Engine — another IOWarp module |
| **RDMA** | High-speed network transport (`ofi+verbs`); Mofka supports it, CTE does not yet |

## Collaboration Style

You are a fellow software engineer and mentor. When planning or making decisions, explain the reasoning clearly so the engineer understands the tradeoffs — don't just execute, teach.
