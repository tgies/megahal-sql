"""Test the Learning Horror -- single SQL statement for all trie learning."""


def _dump_trie(conn):
    """Dump trie state as {(tree, word_path): (count, usage)} for comparison.

    Normalizes by word paths (not node IDs) so results are comparable
    across different execution orders, which may assign different IDs.
    """
    rows = conn.execute("""
        WITH RECURSIVE tree AS (
            SELECT id, tree, ARRAY[]::text[] AS path, usage, count
            FROM trie_nodes WHERE parent_id IS NULL
            UNION ALL
            SELECT tn.id, tn.tree, t.path || s.word, tn.usage, tn.count
            FROM trie_nodes tn
            JOIN tree t ON tn.parent_id = t.id AND tn.tree = t.tree
            JOIN symbols s ON s.id = tn.symbol
        )
        SELECT tree, path, count, usage FROM tree
        ORDER BY tree, path
    """).fetchall()
    return {(r[0].strip(), tuple(r[1])): (r[2], r[3]) for r in rows}


def test_learn_horror_creates_trie_nodes(db):
    """Learning via SQL creates forward and backward trie nodes."""
    conn = db

    before = conn.execute("SELECT count(*) FROM trie_nodes").fetchone()[0]

    conn.execute(
        "SELECT * FROM megahal_learn(%s)",
        ("Hello world this is a test sentence.",),
    )

    after = conn.execute("SELECT count(*) FROM trie_nodes").fetchone()[0]
    assert after > before, "Learning should create new trie nodes"

    # Both forward and backward trees should have new nodes
    fwd = conn.execute(
        "SELECT count(*) FROM trie_nodes WHERE tree = 'F' AND parent_id IS NOT NULL"
    ).fetchone()[0]
    bwd = conn.execute(
        "SELECT count(*) FROM trie_nodes WHERE tree = 'B' AND parent_id IS NOT NULL"
    ).fetchone()[0]
    assert fwd > 0, "Forward trie should have nodes"
    assert bwd > 0, "Backward trie should have nodes"


def test_learn_horror_returns_counts(db):
    """megahal_learn returns (tokens_learned, lines_learned, lines_processed)."""
    conn = db

    row = conn.execute(
        "SELECT * FROM megahal_learn(%s)",
        ("The cat sat on the mat.",),
    ).fetchone()
    tokens, lines, processed = row
    assert tokens > 0, "Should report tokens learned"
    assert lines == 1, "Should report 1 line learned"
    assert processed == 1, "Should report 1 line processed"

    # Short input should report 0 tokens/lines learned but still processed
    row = conn.execute("SELECT * FROM megahal_learn(%s)", ("hi",)).fetchone()
    assert row[0] == 0, "Short input: 0 tokens learned"
    assert row[1] == 0, "Short input: 0 lines learned"
    assert row[2] == 1, "Short input: 1 line processed"


def test_learn_horror_skips_short_input(db):
    """Inputs with <= order tokens should not create any trie nodes."""
    conn = db

    before = conn.execute("SELECT count(*) FROM trie_nodes").fetchone()[0]
    before_usage = conn.execute(
        "SELECT sum(usage) FROM trie_nodes WHERE parent_id IS NULL"
    ).fetchone()[0]

    # "hi" tokenizes to very few tokens (< order=5)
    conn.execute("SELECT * FROM megahal_learn(%s)", ("hi",))

    after = conn.execute("SELECT count(*) FROM trie_nodes").fetchone()[0]
    after_usage = conn.execute(
        "SELECT sum(usage) FROM trie_nodes WHERE parent_id IS NULL"
    ).fetchone()[0]
    assert after == before, "Short input should not create trie nodes"
    assert after_usage == before_usage, "Short input should not change root usage"


def test_learn_horror_then_generate(db):
    """Learn via SQL horror (multi-line), then generate via generation horror."""
    conn = db

    # Train several sentences in one call (multi-line)
    training_text = "\n".join([
        "The cat sat on the mat.",
        "Dogs are wonderful pets.",
        "Birds can fly very high in the sky.",
        "Fish swim in the ocean and in rivers.",
        "The weather is nice today.",
        "I like to read books about animals.",
        "The sun rises in the east and sets in the west.",
    ])
    row = conn.execute(
        "SELECT * FROM megahal_learn(%s)", (training_text,)
    ).fetchone()
    assert row[1] == 7, f"Should report 7 lines learned, got {row[1]}"

    # Generate a reply
    row = conn.execute(
        "SELECT megahal_reply(%s, %s)", ("hello", 5)
    ).fetchone()
    assert row is not None, "Should generate a reply after SQL learning"
    reply = row[0]
    assert isinstance(reply, str)
    assert len(reply) > 0, f"Reply should be non-empty: '{reply}'"


def test_learn_horror_bulk_matches_individual(db):
    """Bulk multi-line learning produces identical trie state to per-line learning."""
    conn = db
    sentences = [
        "The cat sat on the mat.",
        "# This comment should be skipped.",
        "Dogs are wonderful pets.",
        "",
        "Birds can fly very high in the sky.",
        "hi",  # too short to learn -- should be silently skipped
        "Fish swim in the ocean and in rivers.",
    ]

    # Learn individually (skip blanks/comments, like the horror does)
    conn.execute("SAVEPOINT before_learn")
    for s in sentences:
        stripped = s.strip()
        if stripped and not stripped.startswith('#'):
            conn.execute("SELECT * FROM megahal_learn(%s)", (stripped,))
    individual_state = _dump_trie(conn)

    conn.execute("ROLLBACK TO SAVEPOINT before_learn")

    # Learn all at once as a multi-line string
    conn.execute("SELECT * FROM megahal_learn(%s)", ("\n".join(sentences),))
    bulk_state = _dump_trie(conn)

    # Compare
    ind_only = set(individual_state) - set(bulk_state)
    bulk_only = set(bulk_state) - set(individual_state)
    mismatched = {
        k: ('ind=' + str(individual_state[k]), 'bulk=' + str(bulk_state[k]))
        for k in individual_state
        if k in bulk_state and individual_state[k] != bulk_state[k]
    }
    assert bulk_state == individual_state, (
        f"Trie states differ.\n"
        f"Individual-only keys: {ind_only}\n"
        f"Bulk-only keys: {bulk_only}\n"
        f"Mismatched values: {mismatched}"
    )


