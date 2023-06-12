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
    {{MAELSTROM_BIN}} test -w broadcast --bin {{TARGET_DIR}}/broadcast \
        --node-count 25 --time-limit 20 --rate 100 --latency 100

g: (build "g-counter")
    {{MAELSTROM_BIN}} test -w g-counter --bin {{TARGET_DIR}}/g-counter \
        --node-count 3 --rate 100 --time-limit 20 --nemesis partition

serve:
    {{MAELSTROM_BIN}} serve
