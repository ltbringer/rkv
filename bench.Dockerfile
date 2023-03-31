FROM rust:alpine3.17
WORKDIR /usr/src/rkv
ARG N_KEYS
COPY . .
ENV N_KEYS=${N_KEYS}
CMD ["cargo bench"]
