FROM rust:alpine3.17
WORKDIR /usr/src/rkv

RUN apk add --no-cache \
    musl-dev \
    aws-cli \
    gnuplot

ARG N_KEYS
ARG S3_URI
ARG KEY_LENGTH

ENV N_KEYS=${N_KEYS}
ENV S3_URI=${S3_URI}
ENV KEY_LENGTH=${KEY_LENGTH}

COPY . .

CMD ["ash", "-c", "./scripts/cloud_bench.sh \"$S3_URI\""]
