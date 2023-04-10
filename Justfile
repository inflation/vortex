MAELSTROM_BIN := "/opt/apps/maelstrom/maelstrom"
TARGET := "target/debug/vortex"

build:
    cargo build

test: build
    {{MAELSTROM_BIN}} test -w echo --bin {{TARGET}} --node-count 1 --time-limit 10

serve:
    {{MAELSTROM_BIN}} serve
