FROM golang:1.10

ENV DEP_DOWNLOAD_URL https://github.com/golang/dep/releases/download/v0.4.1/dep-linux-amd64
ENV DEP_DOWNLOAD_SHA 31144e465e52ffbc0035248a10ddea61a09bf28b00784fd3fdd9882c8cbb2315

RUN set -ex; \
    wget -O dep "$DEP_DOWNLOAD_URL"; \
    echo "$DEP_DOWNLOAD_SHA  dep" | sha256sum -c -; \
    chmod +x dep ; \
    mv dep /usr/local/bin/; \
    dep version

RUN mkdir -p /go/src/app
WORKDIR /go/src/app

COPY Gopkg.toml Gopkg.lock ./

RUN dep ensure -vendor-only

COPY . ./

ENV PLATFORMS \
        linux/amd64 linux/386 \
        darwin/amd64 darwin/386 \
        freebsd/amd64 freebsd/386

CMD set -ex ; \
    for platform in $PLATFORMS; do \
        GOOS=${platform%/*} GOARCH=${platform##*/} go build -v -o bin/pgtobq-${platform%/*}-${platform##*/}; \
    done ; \
    ls -l bin/
