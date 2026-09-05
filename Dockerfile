# Container image for pdbmux, the merging PuppetDB proxy daemon. It ships only
# as a distroless static image; there is no OS package.
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION}" -o pdbmux .

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/pdbmux /usr/local/bin/pdbmux

EXPOSE 8080

ENTRYPOINT ["pdbmux", "serve"]
