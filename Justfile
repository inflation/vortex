MAELSTROM_BIN := "/opt/apps/maelstrom/maelstrom"
TARGET_BIN := "target/debug/vortex"
TARGET_ID := "target/debug/unique-id"

build:
    cargo build

test: build
    {{MAELSTROM_BIN}} test -w echo --bin {{TARGET_BIN}} --node-count 1 --time-limit 10

test-id: build
    {{MAELSTROM_BIN}} test -w unique-ids --bin {{TARGET_ID}} --time-limit 30 --rate 1000 \
        --node-count 3 --availability total --nemesis partition

serve:
    {{MAELSTROM_BIN}} serve
