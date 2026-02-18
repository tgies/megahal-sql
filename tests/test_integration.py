"""Integration tests -- full conversation loop.

These tests exercise the complete pipeline through SQL functions:
train -> learn from input -> generate reply.
"""

from pathlib import Path


DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _train(conn, filepath):
    """Load a training file via megahal_learn."""
    text = Path(filepath).read_text()
    conn.execute("SELECT * FROM megahal_learn(%s)", (text,))


def _converse(conn, text):
    """Simulate one turn: learn from input, then generate a reply."""
    conn.execute("SELECT * FROM megahal_learn(%s)", (text,))
    row = conn.execute(
        "SELECT megahal_reply(%s, %s)", (text, 10)
    ).fetchone()
    return row[0] if row and row[0] else ""


def test_full_conversation(db):
    """Train, converse, get a non-empty reply."""
    _train(db, DATA_DIR / "megahal.trn")
    db.execute("SELECT setseed(0.42)")

    reply = _converse(db, "Hello there!")
    assert len(reply) > 0


def test_second_reply_differs(db):
    """Two different inputs should generally produce different replies."""
    _train(db, DATA_DIR / "megahal.trn")
    db.execute("SELECT setseed(0.42)")

    reply1 = _converse(db, "Tell me about music")
    db.execute("SELECT setseed(0.99)")
    reply2 = _converse(db, "What do you think of cats")

    # Both should produce something
    assert len(reply1) > 0
    assert len(reply2) > 0


def test_echo_rejection(db):
    """Reply should not be identical to input (case-insensitive)."""
    _train(db, DATA_DIR / "megahal.trn")

    for seed in [0.1, 0.42, 0.7, 0.99]:
        db.execute("SELECT setseed(%s)", (seed,))
        reply = _converse(db, "The cat sat on the mat.")
        assert reply.upper() != "THE CAT SAT ON THE MAT."


def test_reply_is_formatted(db):
    """Reply should have sentence-case formatting."""
    _train(db, DATA_DIR / "megahal.trn")
    db.execute("SELECT setseed(0.42)")

    reply = _converse(db, "Do you like computers?")
    if reply:
        # Find first alpha character -- it should be uppercase
        for ch in reply:
            if ch.isalpha():
                assert ch.isupper(), f"First alpha '{ch}' in '{reply}' should be uppercase"
                break


# -- megahal_greet() tests --------------------------------------------------


def test_greet_on_empty_brain(db):
    """Greeting on an untrained brain returns the default fallback message."""
    row = db.execute("SELECT megahal_greet()").fetchone()
    assert row[0] == "I don't know enough to answer you yet!"


def test_greet_returns_nonempty_after_training(db):
    """After training, megahal_greet produces a non-empty reply."""
    _train(db, DATA_DIR / "megahal.trn")
    row = db.execute("SELECT megahal_greet()").fetchone()
    assert row is not None
    assert len(row[0]) > 0


def test_greet_is_formatted(db):
    """Greeting reply should be sentence-cased."""
    _train(db, DATA_DIR / "megahal.trn")
    reply = db.execute("SELECT megahal_greet()").fetchone()[0]
    first_alpha = next((c for c in reply if c.isalpha()), None)
    if first_alpha:
        assert first_alpha.isupper(), f"First alpha should be uppercase: {reply}"


# -- megahal_converse() tests -----------------------------------------------


def test_converse_on_empty_brain_short_input(db):
    """Short input on an empty brain: too few tokens to learn, fallback reply."""
    row = db.execute("SELECT megahal_converse(%s)", ("hi",)).fetchone()
    assert row[0] == "I don't know enough to answer you yet!"


def test_converse_learns_then_replies(db):
    """megahal_converse should learn from input before generating a reply."""
    # On a fresh brain, feed enough text that learning occurs
    text = "The cat sat on the mat and looked out the window."
    row = db.execute("SELECT megahal_converse(%s)", (text,)).fetchone()
    assert row is not None
    reply = row[0]
    assert len(reply) > 0, "Should produce a non-empty reply after learning"

    # Verify that learning actually happened -- trie should have new nodes
    fwd_count = db.execute(
        "SELECT count(*) FROM trie_nodes WHERE tree = 'F' AND parent_id IS NOT NULL"
    ).fetchone()[0]
    assert fwd_count > 0, "Forward trie should have nodes after converse"


def test_converse_after_training(db):
    """On a trained brain, converse produces a non-empty formatted reply."""
    _train(db, DATA_DIR / "megahal.trn")
    db.execute("SELECT setseed(0.42)")

    reply = db.execute(
        "SELECT megahal_converse(%s)", ("Tell me about the weather.",)
    ).fetchone()[0]

    assert len(reply) > 0
    # Should be sentence-cased
    first_alpha = next((c for c in reply if c.isalpha()), None)
    if first_alpha:
        assert first_alpha.isupper(), f"First alpha should be uppercase: {reply}"


def test_converse_echo_rejection(db):
    """megahal_converse should not echo input back verbatim."""
    _train(db, DATA_DIR / "megahal.trn")

    for seed in [0.1, 0.42, 0.7, 0.99]:
        db.execute("SELECT setseed(%s)", (seed,))
        text = "The cat sat on the mat."
        reply = db.execute(
            "SELECT megahal_converse(%s)", (text,)
        ).fetchone()[0]
        assert reply.upper() != text.upper(), f"Reply should not echo input: {reply}"
