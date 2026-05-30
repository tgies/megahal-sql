"""The echo-rejection input array keeps every input token, including unseen words.

C dissimilar (megahal.c:2254-2262) compares the full normalized input word
array against the reply, including words absent from the dictionary. The SQL
must not drop unknown tokens (an inner join), or the comparison array would be
shorter than the real input and the dissimilarity test could flip.
"""


# Mirrors the input_sym_ids CTE join/aggregate from megahal_reply.
_INPUT_SYM_IDS_PROBE = """
WITH tokens(pos, token) AS (VALUES (1, %(t1)s), (2, %(t2)s), (3, %(t3)s))
SELECT array_agg(COALESCE(s.id, -1) ORDER BY t.pos) AS ids
FROM tokens t LEFT JOIN symbols s ON s.word = t.token
"""


def test_unknown_input_token_kept_as_sentinel(db):
    """A token absent from symbols keeps its array slot as -1, not dropped."""
    db.execute("INSERT INTO symbols (id, word) VALUES (5, 'HELLO'), (6, 'WORLD')")
    (ids,) = db.execute(
        _INPUT_SYM_IDS_PROBE, {"t1": "HELLO", "t2": "ZZZ", "t3": "WORLD"}
    ).fetchone()
    assert ids == [5, -1, 6]


def test_reply_runs_with_unknown_input_tokens(db):
    """megahal_reply does not error when the input contains unseen words."""
    db.execute(
        "SELECT * FROM megahal_learn(%s)", ("the quick brown fox jumps high\n",)
    )
    (reply,) = db.execute(
        "SELECT megahal_reply(%s, %s)", ("xyzzy plugh frobnitz", 5)
    ).fetchone()
    assert reply is not None
