FROM rust:1.91-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/teste-1cpu-1gbram .
RUN mkdir -p /data
ENV DATABASE_URL=sqlite:///data/jobs.db
VOLUME ["/data"]
EXPOSE 3000
CMD ["./teste-1cpu-1gbram"]
