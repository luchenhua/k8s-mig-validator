# --- Build stage ---
FROM golang:1.17 AS builder
WORKDIR /app
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.cn,https://goproxy.io,direct
# Copy the dependency definition
COPY go.mod .
COPY go.sum .
# Download dependencies
RUN go mod download
# Copy the source code
COPY . .
# Build for release
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build

# --- Final stage ---
FROM gcr.io/distroless/static-debian10:latest
COPY --from=builder /app/k8s-mig-validator /
ENTRYPOINT ["/k8s-mig-validator"]
EXPOSE 3000
LABEL Name=k8s-mig/k8s-mig-validator \
    Version=0.1
