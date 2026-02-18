"""Test THE HORROR -- complete text-in -> text-out single SQL statement."""

from pathlib import Path


DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _train(conn, filepath):
    """Load a training file via megahal_learn."""
    text = Path(filepath).read_text()
    conn.execute("SELECT * FROM megahal_learn(%s)", (text,))


def test_horror_returns_formatted_text(db):
    """The horror takes text and returns a formatted reply string."""
    _train(db, DATA_DIR / "megahal.trn")

    row = db.execute(
        "SELECT megahal_reply(%s, %s)", ("hello there", 5)
    ).fetchone()

    assert row is not None
    reply = row[0]
    assert isinstance(reply, str)
    assert len(reply) > 0
    # Should be sentence-cased (first letter uppercase)
    first_alpha = next((c for c in reply if c.isalpha()), None)
    assert first_alpha is not None
    assert first_alpha.isupper(), f"First alpha char should be uppercase: {reply}"


def test_horror_ends_with_punctuation(db):
    """Horror replies should end with terminal punctuation."""
    _train(db, DATA_DIR / "megahal.trn")

    inputs = ["hello", "what is the meaning of life", "tell me about dogs"]
    for text in inputs:
        row = db.execute(
            "SELECT megahal_reply(%s, %s)", (text, 5)
        ).fetchone()
        assert row is not None
        reply = row[0]
        assert len(reply) > 3, f"Reply too short for '{text}': '{reply}'"
        assert reply.rstrip()[-1] in '.!?', f"Reply should end with punctuation: '{reply}'"


def test_horror_different_inputs(db):
    """Different inputs tend to produce different replies."""
    _train(db, DATA_DIR / "megahal.trn")

    replies = set()
    for text in ["hello", "what is life", "tell me about dogs"]:
        row = db.execute(
            "SELECT megahal_reply(%s, %s)", (text, 5)
        ).fetchone()
        if row:
            replies.add(row[0])

    assert len(replies) >= 2, "Expected different inputs to produce at least 2 different replies"
