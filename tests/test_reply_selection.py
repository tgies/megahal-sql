"""Tests for best-candidate selection precedence in megahal_reply.

C generate_reply (megahal.c:2215-2240) seeds output with the fallback string,
overrides it with the baseline reply iff dissimilar to input, then overrides
again with each keyword candidate whose surprise is strictly greater than the
running max (initialized -1.0) and which is dissimilar. So among dissimilar
keyword candidates the highest surprise wins (ties to the earliest-generated),
the baseline is used only when no dissimilar keyword candidate exists, and the
fallback string is used only when nothing is dissimilar.

The selection probe below mirrors the best_keyword / best_baseline /
best_candidate CTEs in schema/03-functions.sql, fed controlled candidate rows
so the precedence is exercised deterministically without the random walk.
"""


# Mirrors PHASE 7 of megahal_reply: pick the dissimilar keyword candidate with
# the highest score (earliest generation breaks ties); fall back to the
# baseline reply if it is dissimilar and no keyword candidate qualified; else
# return the fallback string via COALESCE on an empty selection.
_SELECTION_PROBE = """
WITH
input_sym_ids AS (
    SELECT %(input)s::int[] AS ids
),
baseline AS (
    SELECT %(baseline)s::int[] AS reply_syms
),
candidates AS (
    SELECT gen, reply_syms::int[] AS reply_syms, score
    FROM (VALUES {cand_rows}) AS v(gen, reply_syms, score)
),
best_keyword AS (
    SELECT c.reply_syms
    FROM candidates c, input_sym_ids isym
    WHERE c.reply_syms IS DISTINCT FROM isym.ids
    ORDER BY c.score DESC, c.gen ASC
    LIMIT 1
),
best_baseline AS (
    SELECT b.reply_syms
    FROM baseline b, input_sym_ids isym
    WHERE b.reply_syms IS DISTINCT FROM isym.ids
      AND NOT EXISTS (SELECT 1 FROM best_keyword)
    LIMIT 1
),
best_candidate AS (
    SELECT reply_syms FROM best_keyword
    UNION ALL
    SELECT reply_syms FROM best_baseline
)
SELECT COALESCE(
    (SELECT reply_syms FROM best_candidate LIMIT 1),
    NULL
) AS chosen
"""


def _select(db, input_ids, baseline_ids, candidate_rows):
    """Run the selection probe.

    candidate_rows: list of (gen, reply_syms_list, score). Returns the chosen
    reply_syms list, or None for the fallback (no dissimilar candidate).
    """
    rows_sql = ", ".join(
        f"({gen}, ARRAY{syms}::int[], {score}::float8)"
        for (gen, syms, score) in candidate_rows
    )
    sql = _SELECTION_PROBE.format(cand_rows=rows_sql)
    (chosen,) = db.execute(
        sql, {"input": input_ids, "baseline": baseline_ids}
    ).fetchone()
    return chosen


def test_keyword_candidate_beats_baseline(db):
    """A dissimilar keyword candidate is chosen over a dissimilar baseline.

    In C the baseline is the initial output but any keyword candidate with
    surprise > -1.0 overrides it; entropy scores are non-negative.
    """
    chosen = _select(
        db,
        input_ids=[10, 11],
        baseline_ids=[20, 21],
        candidate_rows=[(1, [30, 31], 0.0)],
    )
    # Score 0.0 still beats the baseline (C: surprise 0.0 > max_surprise -1.0).
    assert chosen == [30, 31]


def test_zero_score_keyword_beats_baseline(db):
    """Even a surprise-0 keyword candidate overrides the baseline.

    The old code injected the baseline at a fake score of 0.0, letting it tie
    or beat a surprise-0 candidate. C always overrides the baseline with a
    dissimilar candidate scoring >= 0.0.
    """
    chosen = _select(
        db,
        input_ids=[10, 11],
        baseline_ids=[20, 21],
        candidate_rows=[(1, [40, 41], 0.0)],
    )
    assert chosen == [40, 41]
    assert chosen != [20, 21]


def test_tie_broken_by_generation_order(db):
    """Equal-score keyword candidates resolve to the earliest generated.

    C replaces output only on strictly-greater surprise, so the first of a tie
    is retained.
    """
    chosen = _select(
        db,
        input_ids=[10, 11],
        baseline_ids=[20, 21],
        candidate_rows=[
            (1, [50, 51], 1.5),
            (2, [60, 61], 1.5),
            (3, [70, 71], 1.5),
        ],
    )
    assert chosen == [50, 51]


def test_highest_score_wins(db):
    """The maximum-score dissimilar keyword candidate is chosen."""
    chosen = _select(
        db,
        input_ids=[10, 11],
        baseline_ids=[20, 21],
        candidate_rows=[
            (1, [50, 51], 0.5),
            (2, [60, 61], 2.0),
            (3, [70, 71], 1.0),
        ],
    )
    assert chosen == [60, 61]


def test_no_dissimilar_keyword_yields_baseline(db):
    """When every keyword candidate echoes the input, the baseline is used."""
    chosen = _select(
        db,
        input_ids=[10, 11],
        baseline_ids=[20, 21],
        candidate_rows=[
            (1, [10, 11], 5.0),
            (2, [10, 11], 9.0),
        ],
    )
    assert chosen == [20, 21]


def test_nothing_dissimilar_yields_fallback(db):
    """When baseline and every candidate echo the input, nothing is selected.

    An empty best_candidate makes the reply formatter emit the fallback string
    via its COALESCE; here the probe returns NULL for that case.
    """
    chosen = _select(
        db,
        input_ids=[10, 11],
        baseline_ids=[10, 11],
        candidate_rows=[
            (1, [10, 11], 5.0),
        ],
    )
    assert chosen is None


def test_fallback_string_matches_c(db):
    """megahal_reply emits the exact C fallback string when nothing replies.

    On an empty brain there are no candidates, so the formatter's COALESCE
    yields the fallback string, which must match C's output_none verbatim.
    """
    (reply,) = db.execute(
        "SELECT megahal_reply(%s, %s)", ("anything at all", 5)
    ).fetchone()
    assert reply == "I don't know enough to answer you yet!"
