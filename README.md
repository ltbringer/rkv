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
- [ ] Benchmark performance.
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

1. [local-machine](https://htmlpreview.github.io/?https://github.com/ltbringer/rkv/blob/benchmarks/reports/store.get-for-1000000-keys-in-10-sstable(s)/report/index.html)

Head over to [releases](https://github.com/ltbringer/rkv/releases).