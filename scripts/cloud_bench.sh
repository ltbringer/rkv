#!/bin/sh
S3_URI=$1
cargo bench
aws s3 cp ./target/criterion $S3_URI --recursive --acl public-read
