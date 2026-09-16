FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /jobraft ./cmd/jobraft

FROM alpine:3.20
RUN adduser -D -H jobraft
USER jobraft
WORKDIR /data
COPY --from=build /jobraft /usr/local/bin/jobraft
ENV JOBRAFT_ADDR=:8080 JOBRAFT_STORE=/data/tasks.json
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/jobraft"]
