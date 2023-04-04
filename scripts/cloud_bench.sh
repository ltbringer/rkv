#!/bin/sh
S3_URI=$1
mkdir -p /tmp/bench-reports
cargo bench --target-dir /tmp/bench-reports
aws s3 cp /tmp/bench-reports $S3_URI --recursive
