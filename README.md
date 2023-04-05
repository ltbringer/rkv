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
- [ ] Security.
  - [ ] File level permissions.
  - [ ] Checksum verification.
  - [ ] RBAC over APIs
  - [ ] Encryption
- [ ] Communication between distributed nodes.
- [ ] Maintaining High Availability.
- [ ] Resolving partition failures.
- [ ] Write a simple client.

## Benchmarks

|            | Lower bound  | Estimate     | Upper bound  |
|------------|--------------|--------------|--------------|
| Slope      | 71.296 µs    | 71.700 µs    | 71.981 µs    |
| Throughput | 217.07 KiB/s | 217.92 KiB/s | 219.16 KiB/s |
| R²         | 0.9987940    | 0.9993731    | 0.9990916    |
| Mean       | 71.203 µs    | 71.434 µs    | 71.687 µs    |
| Std. Dev.  | 221.35 ns    | 415.43 ns    | 531.58 ns    |
| Median     | 71.094 µs    | 71.313 µs    | 71.751 µs    |
| MAD        | 35.104 ns    | 462.50 ns    | 716.93 ns    |

[These benchmarks](https://ltbringer.s3.ap-south-1.amazonaws.com/projects/rkv/reports/report/index.html) were calculated using [Criterion](https://github.com/bheisler/criterion.rs) on infrastructure created by [benchmark-rkv](https://github.com/ltbringer/benchmark-rkv).

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
