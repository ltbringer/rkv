FROM rust:alpine3.17
RUN apk add --no-cache musl-dev
WORKDIR /usr/src/rkv
ARG N_KEYS
ARG DAT_DIR
COPY . .
ENV N_KEYS=${N_KEYS}
ENV DAT_DIR=${DAT_DIR}
CMD ["ash", "-c", "./scripts/cloud_bench.sh"]
