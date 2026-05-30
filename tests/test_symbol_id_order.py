"""Symbol ids are assigned in first-appearance (scan) order.

C add_word assigns id = size-1 at append time as the token stream is scanned
left to right (megahal.c:1072, 1773-1778), so a corpus produces a deterministic
word-to-id mapping. The SQL intern must order the new tokens by their first
(line_id, pos) rather than emitting them in DISTINCT hash order.
"""


def test_symbol_ids_assigned_in_first_seen_order(db):
    """Word ids increase in the order the words first appear in the input."""
    db.execute(
        "SELECT * FROM megahal_learn(%s)",
        ("zebra apple mango kiwi cherry plum grape\n",),
    )
    words = ["ZEBRA", "APPLE", "MANGO", "KIWI", "CHERRY", "PLUM", "GRAPE"]
    ids = []
    for w in words:
        (sid,) = db.execute("SELECT id FROM symbols WHERE word = %s", (w,)).fetchone()
        ids.append(sid)
    assert all(
        ids[i] < ids[i + 1] for i in range(len(ids) - 1)
    ), f"ids not in first-seen order: {dict(zip(words, ids))}"


def test_first_occurrence_determines_id_when_a_word_repeats(db):
    """A repeated word takes the id of its first occurrence, not a later one."""
    # APPLE first appears before BERRY; later repeats of APPLE must not reorder.
    db.execute(
        "SELECT * FROM megahal_learn(%s)",
        ("apple berry apple cherry date apple fig\n",),
    )
    (apple,) = db.execute("SELECT id FROM symbols WHERE word = 'APPLE'").fetchone()
    (berry,) = db.execute("SELECT id FROM symbols WHERE word = 'BERRY'").fetchone()
    assert apple < berry
