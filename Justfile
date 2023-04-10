MAELSTROM_BIN := "/opt/apps/maelstrom/maelstrom"
TARGET_ECHO := "target/debug/echo"
TARGET_ID := "target/debug/unique-id"
TARGET_BR := "target/debug/broadcast"

build:
    cargo build

test-echo: build
    {{MAELSTROM_BIN}} test -w echo --bin {{TARGET_ECHO}} --node-count 1 --time-limit 10

test-id: build
    {{MAELSTROM_BIN}} test -w unique-ids --bin {{TARGET_ID}} --time-limit 30 --rate 1000 \
        --node-count 3 --availability total --nemesis partition

test-br: build
    {{MAELSTROM_BIN}} test -w broadcast --bin {{TARGET_BR}} --node-count 1 --time-limit 20 \
        --rate 10


serve:
    {{MAELSTROM_BIN}} serve
