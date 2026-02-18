def test_sentinel_symbols(db):
    rows = db.execute("SELECT id, word FROM symbols ORDER BY id").fetchall()
    assert rows[0] == (0, "<ERROR>")
    assert rows[1] == (1, "<FIN>")


def test_root_nodes_exist(db):
    rows = db.execute(
        "SELECT tree, symbol, parent_id FROM trie_nodes ORDER BY tree"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == "B"   # Backward root
    assert rows[0][1] == 0     # <ERROR> symbol
    assert rows[0][2] is None  # no parent
    assert rows[1][0] == "F"   # Forward root
    assert rows[1][1] == 0
    assert rows[1][2] is None


def test_config_order(db):
    (order,) = db.execute("SELECT value FROM config WHERE key = 'order'").fetchone()
    assert order == 5


def test_sequence_starts_at_2(db):
    (next_id,) = db.execute("SELECT nextval('symbols_id_seq')").fetchone()
    assert next_id == 2
