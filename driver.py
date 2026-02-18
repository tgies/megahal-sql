#!/usr/bin/env python3
"""MegaHAL-SQL: An interactive chatbot driven entirely by SQL queries.

Usage:
    docker compose up -d --wait
    uv run python driver.py

The driver is a thin I/O shell. Both learning and generation are single SQL
statements -- THE LEARNING HORROR (tokenize -> intern -> forward+backward trie
writes via depth-unrolled writable CTEs) and THE HORROR (tokenize -> keywords
-> N candidates with bidirectional babble + evaluation -> best selection ->
sentence-case formatting). This Python is just for convenience.
"""

from pathlib import Path

import psycopg

PROJECT_ROOT = Path(__file__).parent
SCHEMA_DIR = PROJECT_ROOT / "schema"
DATA_DIR = PROJECT_ROOT / "data"
TRAINING_FILE = DATA_DIR / "megahal.trn"

DSN = "postgresql://megahal:megahal@localhost:5434/megahal"


def init_schema(conn) -> None:
    """Run schema DDL, seed data, and function definitions."""
    for sql_file in sorted(SCHEMA_DIR.glob("*.sql")):
        sql = sql_file.read_text().strip()
        if sql and not all(
            line.lstrip().startswith("--") or line.strip() == ""
            for line in sql.splitlines()
        ):
            conn.execute(sql)


def load_support_data(conn, data_dir: Path) -> None:
    """Load support/lookup data from MegaHAL data files via COPY FROM STDIN.
    This is a convenience for the try-it-quick path and it's not necessary to
    use Python for this.
    """
    word_tables = {
        "banned_words": "megahal.ban",
        "aux_words": "megahal.aux",
        "greeting_words": "megahal.grt",
    }
    for table, filename in word_tables.items():
        with conn.cursor().copy(f"COPY {table} (word) FROM STDIN") as copy:
            for line in (data_dir / filename).read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    copy.write_row((line,))

    with conn.cursor().copy(
        "COPY swap_pairs (from_word, to_word) FROM STDIN"
    ) as copy:
        for line in (data_dir / "megahal.swp").read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                parts = line.split()
                if len(parts) >= 2:
                    copy.write_row((parts[0], parts[1]))


def is_trained(conn) -> bool:
    """Check if the database already has trained trie data."""
    try:
        row = conn.execute(
            "SELECT count(*) FROM trie_nodes WHERE parent_id IS NOT NULL"
        ).fetchone()
        return row[0] > 0
    except Exception:
        return False


def main():
    print("Type 'quit', ^C, or ^D to exit.")
    print("Connecting to PostgreSQL...")
    conn = psycopg.connect(DSN, autocommit=False)

    try:
        print("Initializing schema...")
        init_schema(conn)

        if is_trained(conn):
            print("Database already trained, skipping training.")
        else:
            load_support_data(conn, DATA_DIR)
            print("Training from megahal.trn...")
            text = TRAINING_FILE.read_text()
            row = conn.execute(
                "SELECT * FROM megahal_learn(%s)", (text,)
            ).fetchone()
            print(f"Learned {row[2] or 0} sentences.")

        conn.commit()

        # Initial greeting -- pick a random greeting word, generate a reply
        row = conn.execute("SELECT megahal_greet()").fetchone()
        print(row[0])
        print()

        # REPL loop
        while True:
            try:
                user_input = input("> ")
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if user_input.strip().lower() in ("quit", "exit", "#quit"):
                break
            if not user_input.strip():
                continue

            # Learn from input, then generate a reply -- single SQL call
            row = conn.execute(
                "SELECT megahal_converse(%s)", (user_input,)
            ).fetchone()

            print(row[0])

            conn.commit()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
