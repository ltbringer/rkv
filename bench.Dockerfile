FROM rust:alpine3.17
WORKDIR /usr/src/rkv
ARG N_KEYS
ARG S3_URI
ARG DAT_DIR
COPY . .
ENV N_KEYS=${N_KEYS}
ENV S3_URI=${S3_URI}
ENV DAT_DIR=${DAT_DIR}
CMD ["ash", "-c", "./scripts/cloud_bench.sh"]
