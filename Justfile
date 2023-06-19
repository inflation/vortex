MAELSTROM_BIN := "/opt/apps/maelstrom/maelstrom"
TARGET_DIR := "target/debug"

build target:
    cargo build --bin {{target}}

echo: (build "echo")
    {{MAELSTROM_BIN}} test -w echo --bin {{TARGET_DIR}}/echo \
        --node-count 1 --time-limit 10

id: (build "unique-id")
    {{MAELSTROM_BIN}} test -w unique-ids --bin {{TARGET_DIR}}/unique-id \
        --time-limit 30 --rate 1000 --node-count 3 --availability total

br: (build "broadcast")
    RUST_LOG="vortex=debug,broadcast=debug" OTEL_SERVICE_NAME=broadcast \
    {{MAELSTROM_BIN}} test -w broadcast --bin {{TARGET_DIR}}/broadcast \
        --node-count 25 --time-limit 20 --rate 100 --latency 100

g: (build "g-counter")
    RUST_LOG="vortex=debug,'g-counter'=debug" OTEL_SERVICE_NAME=g-counter \
    {{MAELSTROM_BIN}} test -w g-counter --bin {{TARGET_DIR}}/g-counter \
        --node-count 3 --rate 100 --time-limit 20 --nemesis partition

k: (build "kafka")
    RUST_LOG="vortex=debug,kafka=debug" OTEL_SERVICE_NAME=kafka \
    {{MAELSTROM_BIN}} test -w kafka --bin {{TARGET_DIR}}/kafka \
        --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

t: (build "txn-rw-register")
    RUST_LOG="vortex=debug,'txn-rw-register'=debug" OTEL_SERVICE_NAME=txn-rw-register \
    {{MAELSTROM_BIN}} test -w txn-rw-register --bin {{TARGET_DIR}}/txn-rw-register \
        --node-count 1 --time-limit 20 --rate 1000 \
        --concurrency 2n --consistency-models read-uncommitted --availability total
