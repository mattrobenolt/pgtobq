FROM golang:1.18

RUN mkdir -p /usr/src/pgtobq
WORKDIR /usr/src/pgtobq

COPY go.mod go.sum ./

RUN go mod download

COPY . ./

ENV PLATFORMS \
        linux \
        darwin \
        freebsd

CMD set -ex ; \
    for platform in $PLATFORMS; do \
        GOOS=${platform} GOARCH=amd64 go build -v -o bin/pgtobq-${platform}-amd64; \
    done ; \
    ls -l bin/
