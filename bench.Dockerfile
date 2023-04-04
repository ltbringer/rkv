FROM rust:alpine3.17
WORKDIR /usr/src/rkv

RUN apk add --no-cache \
    musl-dev \
    aws-cli \
    gnuplot

ARG N_KEYS
ARG DAT_DIR
ARG S3_URI

ENV N_KEYS=${N_KEYS}
ENV DAT_DIR=${DAT_DIR}
ENV S3_URI=${S3_URI}

COPY . .

CMD ["sh", "./scripts/cloud_bench.sh", $S3_URI]
