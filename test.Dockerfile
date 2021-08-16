# This dockerile might be helpful for running gorocksdb tests in docker
# Example:
#   > docker build --ssh default -f test.Dockerfile -t gorocksdb-test .
#   > docker run -it --rm -v $(pwd):/go/gorocksdb --workdir /go/gorocksdb gorocksdb-test go test -v
FROM golang:1.16.7-alpine3.14

ENV GO111MODULE="auto"

RUN apk add --no-cache openssh-client zlib-dev bzip2-dev lz4-dev snappy-dev zstd-dev gflags-dev
RUN apk add --no-cache build-base linux-headers git bash perl wget g++ automake

RUN mkdir -p ~/.ssh/ && ssh-keyscan github.com > ~/.ssh/known_hosts

RUN --mount=type=ssh git clone git@github.com:GetStream/rocksdb.git /rocksdb
RUN cd /rocksdb && \
    git checkout broadwell && \
    DISABLE_JEMALLOC=1 make static_lib -j5

RUN go get github.com/facebookgo/ensure && \
    go get github.com/stretchr/testify/assert

ENV CGO_CFLAGS="-I/rocksdb/include"
ENV CGO_LDFLAGS="-L/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"

CMD ["bash"]