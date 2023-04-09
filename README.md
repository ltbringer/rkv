# rkv

[![Test](https://github.com/ltbringer/rkv/actions/workflows/test.yml/badge.svg)](https://github.com/ltbringer/rkv/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/ltbringer/rkv/branch/main/graph/badge.svg?token=KMV5N5WM3G)](https://codecov.io/gh/ltbringer/rkv)

![album art](https://codecov.io/gh/ltbringer/rkv/branch/main/graphs/tree.svg?token=KMV5N5WM3G)

## Introduction

I was curious how database management systems are made. So I decided to make my own. This is a simple key-value store that uses an LSM tree as the underlying data structure.

## Lessons

To follow from scratch, check the releases and tags section. This will help you observe the progress on this project from the very first commit. I have also tried to name the tags to provide an objective and the release notes give a short summary to set expectations for the topic.

### Lesson Objectives 

- [x] Build a simple key-value store.
- [x] Benchmark performance.
- [ ] Write highly performant search.
  - [x] Parallel Search
  - [x] Binary Search over SSTables
  - [ ] Compression
- [ ] Distributed Database.
  - [ ] Choose from: master-slave, peer-to-peer, and client-server. (Read about them and other options)
  - [ ] Partitioning scheme. Distribute data across multiple nodes in the distributed system.
  - [ ] Communication protocol: (Bias towards gRPC).
  - [ ] Node discovery and membership: Nodes should be able to discover each other and maintain a membership list. (This is important for fault tolerance and scalability, as nodes can join or leave the distributed system dynamically.)
  - [ ] Distributed Consensus: Raft.
  - [ ] Handle Concurrency: Implementing distributed locks, transactions, and other concurrency control mechanisms.
  - [ ] Fault Tolerance: Handle failures like: node crashes, network partitions, and communication failures. This means implementing replication, leader election, etc.
  - [ ] Monitor and Observability: A distributed system should be monitored for performance, availability, and other metrics. Also build tools for debugging and troubleshooting issues in the distributed system.
- [ ] Security.
  - [ ] File level permissions.
  - [ ] Checksum verification.
  - [ ] RBAC over APIs
  - [ ] Encryption
- [ ] Write a simple client.

## Benchmarks

|             | Lower bound   | Estimate      | Upper bound   |
|-------------|---------------|---------------|---------------|
| Slope       | 45.614 µs     | 45.789 µs     | 45.983 µs     |
| Throughput  | 20.740 MiB/s  | 20.828 MiB/s  | 20.908 MiB/s  |
| R²          | 0.9892351     | 0.9903021     | 0.9889759     |
| Mean        | 45.754 µs     | 45.937 µs     | 46.136 µs     |
| Std. Dev.   | 516.75 ns     | 692.24 ns     | 875.30 ns     |
| Median      | 45.621 µs     | 45.837 µs     | 46.148 µs     |
| MAD         | 383.42 ns     | 708.86 ns     | 862.66 ns     |

[These benchmarks](https://ltbringer.s3.ap-south-1.amazonaws.com/projects/rkv/reports/0.0.6/report/index.html) were calculated using [Criterion](https://github.com/bheisler/criterion.rs) on infrastructure created by [benchmark-rkv](https://github.com/ltbringer/benchmark-rkv).

We used a `c6a.2xlarge` AWS EC2 instance. This has 8 vCPU and 16GiB memory. We also mount a 1TB `gp3` EBS volume to use as the data directory for the database.

```text
$ lscpu

Architecture:            x86_64
  CPU op-mode(s):        32-bit, 64-bit
  Address sizes:         48 bits physical, 48 bits virtual
  Byte Order:            Little Endian
CPU(s):                  8
  On-line CPU(s) list:   0-7
Vendor ID:               AuthenticAMD
  Model name:            AMD EPYC 7R13 Processor
    CPU family:          25
    Model:               1
    Thread(s) per core:  2
    Core(s) per socket:  4
Virtualization features: 
  Hypervisor vendor:     KVM
  Virtualization type:   full
Caches (sum of all):     
  L1d:                   128 KiB (4 instances)
  L1i:                   128 KiB (4 instances)
  L2:                    2 MiB (4 instances)
  L3:                    16 MiB (1 instance)
```

Head over to [releases](https://github.com/ltbringer/rkv/releases).
