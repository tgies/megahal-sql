def test_banned_words_loaded(db):
    (count,) = db.execute("SELECT count(*) FROM banned_words").fetchone()
    assert count == 384


def test_aux_words_loaded(db):
    (count,) = db.execute("SELECT count(*) FROM aux_words").fetchone()
    assert count == 28


def test_greeting_words_loaded(db):
    rows = db.execute("SELECT word FROM greeting_words ORDER BY word").fetchall()
    words = [r[0] for r in rows]
    assert "HELLO" in words
    assert "HOWDY" in words
    assert len(words) == 6


def test_swap_pairs_loaded(db):
    (count,) = db.execute("SELECT count(*) FROM swap_pairs").fetchone()
    assert count == 25

    # Check I -> YOU
    rows = db.execute(
        "SELECT to_word FROM swap_pairs WHERE from_word = 'I'"
    ).fetchall()
    assert rows == [("YOU",)]

    # Check YOU -> I and YOU -> ME (many-to-one)
    rows = db.execute(
        "SELECT to_word FROM swap_pairs WHERE from_word = 'YOU' ORDER BY to_word"
    ).fetchall()
    assert [r[0] for r in rows] == ["I", "ME"]
