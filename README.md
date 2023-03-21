# rkv

[![Test](https://github.com/ltbringer/rkv/actions/workflows/test.yml/badge.svg)](https://github.com/ltbringer/rkv/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/ltbringer/rkv/branch/main/graph/badge.svg?token=KMV5N5WM3G)](https://codecov.io/gh/ltbringer/rkv)

## Coverage

![coverage](https://codecov.io/gh/ltbringer/rkv/branch/main/graphs/tree.svg?token=KMV5N5WM3G)

## Introduction

I was curious how database management systems are made. So I decided to make my own. This is a simple key-value store that uses an LSM tree as the underlying data structure. I have the following goals with this project:

- [x] Build a simple key-value store.
- [ ] Benchmarking the performance.
- [ ] Write highly performant search.
- [ ] Communication between distributed nodes.
- [ ] Maintaining High Availability.
- [ ] Resolving partition failures.
- [ ] Write a simple client.
