FROM ubuntu:24.04 AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    ninja-build \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cmake --preset release && cmake --build --preset release

FROM ubuntu:24.04 AS runtime
RUN useradd --system --create-home kvstore
USER kvstore
WORKDIR /app
COPY --from=build /src/build/release/kvstore /app/kvstore
COPY configs/kvstore.example.yaml /app/config.yaml
EXPOSE 6379 9090
ENTRYPOINT ["/app/kvstore"]
