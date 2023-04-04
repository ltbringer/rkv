#!/bin/sh
S3_URI=$1
cargo bench
mv ../target/criterion /tmp/criterion
aws s3 cp /tmp/criterion $S3_URI --recursive
