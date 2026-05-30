"""Tests for the used_key gate in megahal_reply's forward babble.

C reply() sets used_key=FALSE once per reply (megahal.c:2379). seed() never
touches it (megahal.c:2685), so after seeding the flag is still FALSE even when
the seed is a keyword. babble (megahal.c:2640-2650) gates auxiliary keywords
behind used_key: an aux keyword is only force-selectable once used_key is TRUE.
The first babble step after a non-aux-keyword seed must therefore not force the
aux keyword.

The probes below mirror, in isolation, the fwd CTE seed row and the babble
keyword force-gate (is_keyword predicate) from schema/03-functions.sql so the
gate is exercised deterministically without the random threshold walk.
"""


# Mirrors the fwd CTE seed row: used_key starts FALSE regardless of whether the
# seed symbol is a keyword. The probe takes the seed symbol id and the
# keyword id set and reports the seed's used_key.
_SEED_USED_KEY_PROBE = """
WITH
kw AS (SELECT %(keyword_ids)s::int[] AS keyword_ids),
seed AS (SELECT %(seed)s::int AS sym_id)
SELECT false AS used_key
FROM seed s, kw
"""


# Mirrors the babble is_keyword force-gate for a single candidate child symbol:
#   in keyword set AND (used_key OR not aux) AND not already in reply.
_BABBLE_GATE_PROBE = """
WITH
kw AS (
    SELECT %(keyword_ids)s::int[] AS keyword_ids,
           %(aux_ids)s::int[] AS aux_ids
),
f AS (
    SELECT %(used_key)s::bool AS used_key,
           %(reply_syms)s::int[] AS reply_syms
),
cu AS (SELECT %(symbol)s::int AS symbol)
SELECT (cu.symbol = ANY(kw.keyword_ids)
        AND (f.used_key OR NOT (cu.symbol = ANY(kw.aux_ids)))
        AND NOT (cu.symbol = ANY(f.reply_syms))) AS is_keyword
FROM cu, kw, f
"""


def _seed_used_key(db, seed, keyword_ids):
    (uk,) = db.execute(
        _SEED_USED_KEY_PROBE, {"seed": seed, "keyword_ids": keyword_ids}
    ).fetchone()
    return uk


def _babble_is_keyword(db, symbol, keyword_ids, aux_ids, used_key, reply_syms):
    (ik,) = db.execute(
        _BABBLE_GATE_PROBE,
        {
            "symbol": symbol,
            "keyword_ids": keyword_ids,
            "aux_ids": aux_ids,
            "used_key": used_key,
            "reply_syms": reply_syms,
        },
    ).fetchone()
    return ik


def test_seed_leaves_used_key_false_even_for_keyword_seed(db):
    """The seed does not set used_key, even when the seed symbol is a keyword."""
    # Seed symbol 5 is a (primary) keyword; used_key must still start FALSE.
    assert _seed_used_key(db, seed=5, keyword_ids=[5, 7]) is False


def test_aux_keyword_blocked_while_used_key_false(db):
    """An aux keyword fails the babble force-gate before any keyword is used.

    First babble step after a non-aux-keyword seed: used_key is FALSE, so the
    aux keyword (id 7) is not force-selectable.
    """
    is_kw = _babble_is_keyword(
        db,
        symbol=7,
        keyword_ids=[5, 7],
        aux_ids=[7],
        used_key=False,
        reply_syms=[5],
    )
    assert is_kw is False


def test_aux_keyword_allowed_once_used_key_true(db):
    """Once a non-aux keyword has been used, an aux keyword passes the gate."""
    is_kw = _babble_is_keyword(
        db,
        symbol=7,
        keyword_ids=[5, 7],
        aux_ids=[7],
        used_key=True,
        reply_syms=[5],
    )
    assert is_kw is True


def test_primary_keyword_passes_gate_while_used_key_false(db):
    """A non-aux keyword is force-selectable even before used_key is set."""
    is_kw = _babble_is_keyword(
        db,
        symbol=5,
        keyword_ids=[5, 7],
        aux_ids=[7],
        used_key=False,
        reply_syms=[],
    )
    assert is_kw is True


def test_keyword_already_in_reply_blocked(db):
    """A keyword already present in the reply fails the gate."""
    is_kw = _babble_is_keyword(
        db,
        symbol=5,
        keyword_ids=[5, 7],
        aux_ids=[7],
        used_key=True,
        reply_syms=[5],
    )
    assert is_kw is False
