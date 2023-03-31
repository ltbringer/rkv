#!/bin/bash
S3_PATH=$1
cargo bench
aws s3 cp ./target/criterion $S3_PATH --recursive
