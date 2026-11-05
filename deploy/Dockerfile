FROM rust:1-slim AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates && rm -rf /var/lib/apt/lists/*
RUN rustup target add wasm32-unknown-unknown && cargo install wasm-bindgen-cli --version 0.2.130
COPY Cargo.toml Cargo.lock* ./
COPY backend ./backend
COPY frontend ./frontend
RUN cargo build --release -p downloadit-server
RUN cargo build --release -p downloadit-web --target wasm32-unknown-unknown
COPY web ./web
RUN wasm-bindgen --target web --out-dir web/pkg --out-name downloadit_web target/wasm32-unknown-unknown/release/downloadit_web.wasm

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl ffmpeg python3 && rm -rf /var/lib/apt/lists/* \
 && curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
 && chmod a+rx /usr/local/bin/yt-dlp \
 && useradd --system --create-home --home-dir /app downloadit \
 && mkdir -p /data/files /app/web && chown -R downloadit:downloadit /data /app
WORKDIR /app
COPY --from=build /src/target/release/downloadit-server /app/downloadit-server
COPY --from=build --chown=downloadit:downloadit /src/web /app/web
ENV ADDR=0.0.0.0:8080 DATABASE_PATH=/data/downloads.db DOWNLOAD_DIR=/data/files
EXPOSE 8080
USER downloadit
ENTRYPOINT ["/app/downloadit-server"]
