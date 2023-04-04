#!/bin/sh
S3_URI=$1
cargo bench
ls
aws s3 cp ./target/criterion $S3_URI --recursive
