-- MegaHAL SQL functions
--
--   SELECT * FROM megahal_learn('The cat sat on the mat.');
--   SELECT * FROM megahal_learn(pg_read_file('/path/to/megahal.trn'));
--   SELECT megahal_reply('hello world', 10);

-- megahal_learn(input text)
--
-- THE LEARNING HORROR -- learns from one or more lines of text.
-- Splits on newlines, skips comments (#) and blank lines, tokenizes each
-- line, interns symbols, and builds forward+backward trie entries.
CREATE OR REPLACE FUNCTION megahal_learn(input text)
RETURNS TABLE(tokens_learned bigint, lines_learned bigint, lines_processed bigint)
LANGUAGE sql VOLATILE AS
$learn_horror$
WITH

-- PHASE 1: LINE SPLITTING & TOKENIZATION

-- Split input into non-empty, non-comment lines
input_lines AS (
    SELECT
        row_number() OVER () AS line_id,
        UPPER(btrim(line)) AS str
    FROM regexp_split_to_table(input::text, E'\n') AS line
    WHERE btrim(line) <> '' AND btrim(line) !~ '^#'
),

-- Explode each line into individual characters
chars AS (
    SELECT il.line_id, i AS pos, substring(il.str FROM i FOR 1) AS ch
    FROM input_lines il, generate_series(1, length(il.str)) AS i
),

-- Classify characters with look-ahead/behind, partitioned by line
classified AS (
    SELECT
        c.line_id, c.pos, c.ch,
        (c.ch ~ '[A-Z]') AS is_alpha,
        (c.ch ~ '[0-9]') AS is_digit,
        LAG(c.ch, 1) OVER w AS prev_ch,
        LEAD(c.ch, 1) OVER w AS next_ch,
        LAG(c.ch, 2) OVER w AS prev2_ch,
        (COALESCE(LAG(c.ch, 1) OVER w, '') ~ '[A-Z]') AS prev_is_alpha,
        (COALESCE(LAG(c.ch, 1) OVER w, '') ~ '[0-9]') AS prev_is_digit
    FROM chars c
    WINDOW w AS (PARTITION BY c.line_id ORDER BY c.pos)
),

-- Detect token boundaries (transitions between alpha/non-alpha/digit)
boundaries AS (
    SELECT cl.line_id, cl.pos, cl.ch,
        CASE
            WHEN cl.pos = 1 THEN FALSE
            WHEN cl.ch = '''' AND cl.prev_is_alpha
                 AND COALESCE(cl.next_ch, '') ~ '[A-Z]' THEN FALSE
            WHEN cl.prev_ch = '''' AND cl.is_alpha
                 AND COALESCE(cl.prev2_ch, '') ~ '[A-Z]' THEN FALSE
            WHEN cl.is_alpha AND NOT cl.prev_is_alpha THEN TRUE
            WHEN NOT cl.is_alpha AND cl.prev_is_alpha THEN TRUE
            WHEN cl.is_digit != cl.prev_is_digit THEN TRUE
            ELSE FALSE
        END AS is_boundary
    FROM classified cl
),

-- Assign group IDs to consecutive characters within the same token
token_groups AS (
    SELECT b.line_id, b.pos, b.ch,
        SUM(CASE WHEN b.is_boundary THEN 1 ELSE 0 END)
            OVER (PARTITION BY b.line_id ORDER BY b.pos) AS grp
    FROM boundaries b
),

-- Concatenate characters within each group to form tokens
raw_tokens AS (
    SELECT tg.line_id, tg.grp, string_agg(tg.ch, '' ORDER BY tg.pos) AS token
    FROM token_groups tg GROUP BY tg.line_id, tg.grp
),

-- Number tokens sequentially within each line
numbered_tokens AS (
    SELECT rt.line_id,
           ROW_NUMBER() OVER (PARTITION BY rt.line_id ORDER BY rt.grp) AS pos,
           rt.token
    FROM raw_tokens rt
),

-- Find the last token of each line (needed for normalization)
last_info AS (
    SELECT DISTINCT ON (line_id) line_id, pos AS last_pos, token AS last_token
    FROM numbered_tokens
    ORDER BY line_id, pos DESC
),

-- Normalize: ensure each line ends with sentence-ending punctuation
normalized AS (
    -- Case 1: last token starts with alphanumeric -> append '.'
    SELECT nt.line_id, nt.pos, nt.token
    FROM numbered_tokens nt
    JOIN last_info li ON li.line_id = nt.line_id
    WHERE substring(li.last_token FROM 1 FOR 1) ~ '[A-Z0-9]'
    UNION ALL
    SELECT li.line_id, li.last_pos + 1, '.'
    FROM last_info li
    WHERE substring(li.last_token FROM 1 FOR 1) ~ '[A-Z0-9]'
    UNION ALL
    -- Case 2: last token is non-alpha and ends with sentence-ender -> keep as-is
    SELECT nt.line_id, nt.pos, nt.token
    FROM numbered_tokens nt
    JOIN last_info li ON li.line_id = nt.line_id
    WHERE NOT (substring(li.last_token FROM 1 FOR 1) ~ '[A-Z0-9]')
      AND substring(li.last_token FROM length(li.last_token) FOR 1) IN ('!', '.', '?')
    UNION ALL
    -- Case 3: last token is non-alpha, no sentence-ender -> replace last with '.'
    SELECT nt.line_id, nt.pos,
           CASE WHEN nt.pos = li.last_pos THEN '.' ELSE nt.token END
    FROM numbered_tokens nt
    JOIN last_info li ON li.line_id = nt.line_id
    WHERE NOT (substring(li.last_token FROM 1 FOR 1) ~ '[A-Z0-9]')
      AND NOT (substring(li.last_token FROM length(li.last_token) FOR 1) IN ('!', '.', '?'))
),

tokens AS (
    SELECT line_id, pos, token FROM normalized ORDER BY line_id, pos
),

-- PHASE 2: SYMBOL INTERNING

-- Intern unique tokens from learnable lines into the symbols table.
-- Only lines with more tokens than the Markov order are learnable,
-- matching the original C code's learn() which skips short inputs
-- entirely (including dictionary registration).  Without this guard,
-- words from too-short inputs become "orphan symbols" — present in
-- the symbol table but absent from the trie — which lets them be
-- used as keywords/seeds during reply generation, producing chaotic
-- output (context-free babble from root with no spacing guarantees).
-- ON CONFLICT DO UPDATE (no-op) ensures RETURNING works for existing words.
interned AS (
    INSERT INTO symbols (id, word)
    SELECT nextval('symbols_id_seq'), word
    FROM (
        SELECT DISTINCT token AS word
        FROM tokens
        WHERE line_id IN (
            SELECT line_id FROM tokens
            GROUP BY line_id
            HAVING count(*) > (SELECT value FROM config WHERE key = 'order')
        )
    ) t
    ON CONFLICT (word) DO UPDATE SET word = EXCLUDED.word
    RETURNING id, word
),

-- Map each token position to its symbol ID
token_syms AS (
    SELECT t.line_id, t.pos, i.id AS sym_id
    FROM tokens t
    JOIN interned i ON i.word = t.token
),

-- Build per-line symbol ID arrays
sym_arrays AS (
    SELECT line_id, array_agg(sym_id ORDER BY pos) AS syms,
           count(*) AS n_tokens
    FROM token_syms
    GROUP BY line_id
),

-- PHASE 3: MODEL PARAMETERS & PER-LINE GUARD

params AS (
    SELECT
        (SELECT value FROM config WHERE key = 'order') AS ord,
        (SELECT id FROM trie_nodes WHERE parent_id IS NULL AND tree = 'F') AS f_root,
        (SELECT id FROM trie_nodes WHERE parent_id IS NULL AND tree = 'B') AS b_root
),

-- Only learn lines with tokens > order
learnable AS (
    SELECT sa.line_id, sa.syms, sa.n_tokens
    FROM sym_arrays sa, params p
    WHERE sa.n_tokens > p.ord
),

-- PHASE 4: FORWARD TRIE LEARNING

-- Extended forward symbols: per-line tokens + <FIN> sentinel
fwd_syms AS (
    SELECT array_append(syms, 1::smallint) AS syms
    FROM learnable
),

-- Enumerate all forward n-grams, aggregated across all lines.
-- N-grams from different lines sharing the same trie path merge here.
-- p1..p6 represent the symbol path from root (max depth 6 for order=5).
fwd_nodes AS (
    SELECT
        d.d AS depth,
        fs.syms[p.p - d.d + 1] AS p1,
        CASE WHEN d.d >= 2 THEN fs.syms[p.p - d.d + 2] END AS p2,
        CASE WHEN d.d >= 3 THEN fs.syms[p.p - d.d + 3] END AS p3,
        CASE WHEN d.d >= 4 THEN fs.syms[p.p - d.d + 4] END AS p4,
        CASE WHEN d.d >= 5 THEN fs.syms[p.p - d.d + 5] END AS p5,
        CASE WHEN d.d >= 6 THEN fs.syms[p.p - d.d + 6] END AS p6,
        count(*) AS count_incr
    FROM fwd_syms fs,
    LATERAL generate_series(1, array_length(fs.syms, 1)) p(p),
    LATERAL generate_series(1, least(p.p, (SELECT ord + 1 FROM params))) d(d)
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- Pre-aggregated usage at each depth: total child observation count.
fwd_usage_1 AS (
    SELECT p1, SUM(count_incr)::int AS usage
    FROM fwd_nodes WHERE depth = 2 GROUP BY p1
),
fwd_usage_2 AS (
    SELECT p1, p2, SUM(count_incr)::int AS usage
    FROM fwd_nodes WHERE depth = 3 GROUP BY p1, p2
),
fwd_usage_3 AS (
    SELECT p1, p2, p3, SUM(count_incr)::int AS usage
    FROM fwd_nodes WHERE depth = 4 GROUP BY p1, p2, p3
),
fwd_usage_4 AS (
    SELECT p1, p2, p3, p4, SUM(count_incr)::int AS usage
    FROM fwd_nodes WHERE depth = 5 GROUP BY p1, p2, p3, p4
),
fwd_usage_5 AS (
    SELECT p1, p2, p3, p4, p5, SUM(count_incr)::int AS usage
    FROM fwd_nodes WHERE depth = 6 GROUP BY p1, p2, p3, p4, p5
),

-- Update forward root usage (sum of all depth-1 count increments)
fwd_root_update AS (
    UPDATE trie_nodes
    SET usage = usage + (SELECT COALESCE(SUM(count_incr), 0) FROM fwd_nodes WHERE depth = 1)
    WHERE id = (SELECT f_root FROM params)
      AND (SELECT COALESCE(SUM(count_incr), 0) FROM fwd_nodes WHERE depth = 1) > 0
    RETURNING id
),

-- Forward depth 1: children of root
fwd_d1 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        (SELECT f_root FROM params),
        'F',
        n.p1::smallint,
        n.count_incr::int,
        COALESCE(u.usage, 0)
    FROM fwd_nodes n
    LEFT JOIN fwd_usage_1 u ON u.p1 = n.p1
    WHERE n.depth = 1
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- Forward depth 2
fwd_d2 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        d1.id,
        'F',
        n.p2::smallint,
        n.count_incr::int,
        COALESCE(u.usage, 0)
    FROM fwd_nodes n
    JOIN fwd_d1 d1 ON d1.symbol = n.p1::smallint
    LEFT JOIN fwd_usage_2 u ON u.p1 = n.p1 AND u.p2 = n.p2
    WHERE n.depth = 2
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- Forward depth 3
fwd_d3 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        d2.id,
        'F',
        n.p3::smallint,
        n.count_incr::int,
        COALESCE(u.usage, 0)
    FROM fwd_nodes n
    JOIN fwd_d1 d1 ON d1.symbol = n.p1::smallint
    JOIN fwd_d2 d2 ON d2.parent_id = d1.id AND d2.symbol = n.p2::smallint
    LEFT JOIN fwd_usage_3 u ON u.p1 = n.p1 AND u.p2 = n.p2 AND u.p3 = n.p3
    WHERE n.depth = 3
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- Forward depth 4
fwd_d4 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        d3.id,
        'F',
        n.p4::smallint,
        n.count_incr::int,
        COALESCE(u.usage, 0)
    FROM fwd_nodes n
    JOIN fwd_d1 d1 ON d1.symbol = n.p1::smallint
    JOIN fwd_d2 d2 ON d2.parent_id = d1.id AND d2.symbol = n.p2::smallint
    JOIN fwd_d3 d3 ON d3.parent_id = d2.id AND d3.symbol = n.p3::smallint
    LEFT JOIN fwd_usage_4 u ON u.p1 = n.p1 AND u.p2 = n.p2 AND u.p3 = n.p3
                            AND u.p4 = n.p4
    WHERE n.depth = 4
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- Forward depth 5
fwd_d5 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        d4.id,
        'F',
        n.p5::smallint,
        n.count_incr::int,
        COALESCE(u.usage, 0)
    FROM fwd_nodes n
    JOIN fwd_d1 d1 ON d1.symbol = n.p1::smallint
    JOIN fwd_d2 d2 ON d2.parent_id = d1.id AND d2.symbol = n.p2::smallint
    JOIN fwd_d3 d3 ON d3.parent_id = d2.id AND d3.symbol = n.p3::smallint
    JOIN fwd_d4 d4 ON d4.parent_id = d3.id AND d4.symbol = n.p4::smallint
    LEFT JOIN fwd_usage_5 u ON u.p1 = n.p1 AND u.p2 = n.p2 AND u.p3 = n.p3
                            AND u.p4 = n.p4 AND u.p5 = n.p5
    WHERE n.depth = 5
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- Forward depth 6 (no children -> usage = 0)
fwd_d6 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        d5.id,
        'F',
        n.p6::smallint,
        n.count_incr::int,
        0::int
    FROM fwd_nodes n
    JOIN fwd_d1 d1 ON d1.symbol = n.p1::smallint
    JOIN fwd_d2 d2 ON d2.parent_id = d1.id AND d2.symbol = n.p2::smallint
    JOIN fwd_d3 d3 ON d3.parent_id = d2.id AND d3.symbol = n.p3::smallint
    JOIN fwd_d4 d4 ON d4.parent_id = d3.id AND d4.symbol = n.p4::smallint
    JOIN fwd_d5 d5 ON d5.parent_id = d4.id AND d5.symbol = n.p5::smallint
    WHERE n.depth = 6
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- PHASE 5: BACKWARD TRIE LEARNING

-- Reversed symbol arrays per learnable line
bwd_arrays AS (
    SELECT ts.line_id, array_agg(ts.sym_id ORDER BY ts.pos DESC) AS syms
    FROM token_syms ts
    JOIN learnable l ON l.line_id = ts.line_id
    GROUP BY ts.line_id
),

-- Extended backward symbols: reversed tokens + <FIN> sentinel
bwd_syms AS (
    SELECT array_append(syms, 1::smallint) AS syms
    FROM bwd_arrays
),

-- Backward n-grams (same structure as forward, different source arrays)
bwd_nodes AS (
    SELECT
        d.d AS depth,
        bs.syms[p.p - d.d + 1] AS p1,
        CASE WHEN d.d >= 2 THEN bs.syms[p.p - d.d + 2] END AS p2,
        CASE WHEN d.d >= 3 THEN bs.syms[p.p - d.d + 3] END AS p3,
        CASE WHEN d.d >= 4 THEN bs.syms[p.p - d.d + 4] END AS p4,
        CASE WHEN d.d >= 5 THEN bs.syms[p.p - d.d + 5] END AS p5,
        CASE WHEN d.d >= 6 THEN bs.syms[p.p - d.d + 6] END AS p6,
        count(*) AS count_incr
    FROM bwd_syms bs,
    LATERAL generate_series(1, array_length(bs.syms, 1)) p(p),
    LATERAL generate_series(1, least(p.p, (SELECT ord + 1 FROM params))) d(d)
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- Pre-aggregated backward usage
bwd_usage_1 AS (
    SELECT p1, SUM(count_incr)::int AS usage
    FROM bwd_nodes WHERE depth = 2 GROUP BY p1
),
bwd_usage_2 AS (
    SELECT p1, p2, SUM(count_incr)::int AS usage
    FROM bwd_nodes WHERE depth = 3 GROUP BY p1, p2
),
bwd_usage_3 AS (
    SELECT p1, p2, p3, SUM(count_incr)::int AS usage
    FROM bwd_nodes WHERE depth = 4 GROUP BY p1, p2, p3
),
bwd_usage_4 AS (
    SELECT p1, p2, p3, p4, SUM(count_incr)::int AS usage
    FROM bwd_nodes WHERE depth = 5 GROUP BY p1, p2, p3, p4
),
bwd_usage_5 AS (
    SELECT p1, p2, p3, p4, p5, SUM(count_incr)::int AS usage
    FROM bwd_nodes WHERE depth = 6 GROUP BY p1, p2, p3, p4, p5
),

-- Update backward root usage
bwd_root_update AS (
    UPDATE trie_nodes
    SET usage = usage + (SELECT COALESCE(SUM(count_incr), 0) FROM bwd_nodes WHERE depth = 1)
    WHERE id = (SELECT b_root FROM params)
      AND (SELECT COALESCE(SUM(count_incr), 0) FROM bwd_nodes WHERE depth = 1) > 0
    RETURNING id
),

-- Backward depth 1
bwd_d1 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        (SELECT b_root FROM params),
        'B',
        n.p1::smallint,
        n.count_incr::int,
        COALESCE(u.usage, 0)
    FROM bwd_nodes n
    LEFT JOIN bwd_usage_1 u ON u.p1 = n.p1
    WHERE n.depth = 1
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- Backward depth 2
bwd_d2 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        d1.id,
        'B',
        n.p2::smallint,
        n.count_incr::int,
        COALESCE(u.usage, 0)
    FROM bwd_nodes n
    JOIN bwd_d1 d1 ON d1.symbol = n.p1::smallint
    LEFT JOIN bwd_usage_2 u ON u.p1 = n.p1 AND u.p2 = n.p2
    WHERE n.depth = 2
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- Backward depth 3
bwd_d3 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        d2.id,
        'B',
        n.p3::smallint,
        n.count_incr::int,
        COALESCE(u.usage, 0)
    FROM bwd_nodes n
    JOIN bwd_d1 d1 ON d1.symbol = n.p1::smallint
    JOIN bwd_d2 d2 ON d2.parent_id = d1.id AND d2.symbol = n.p2::smallint
    LEFT JOIN bwd_usage_3 u ON u.p1 = n.p1 AND u.p2 = n.p2 AND u.p3 = n.p3
    WHERE n.depth = 3
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- Backward depth 4
bwd_d4 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        d3.id,
        'B',
        n.p4::smallint,
        n.count_incr::int,
        COALESCE(u.usage, 0)
    FROM bwd_nodes n
    JOIN bwd_d1 d1 ON d1.symbol = n.p1::smallint
    JOIN bwd_d2 d2 ON d2.parent_id = d1.id AND d2.symbol = n.p2::smallint
    JOIN bwd_d3 d3 ON d3.parent_id = d2.id AND d3.symbol = n.p3::smallint
    LEFT JOIN bwd_usage_4 u ON u.p1 = n.p1 AND u.p2 = n.p2 AND u.p3 = n.p3
                            AND u.p4 = n.p4
    WHERE n.depth = 4
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- Backward depth 5
bwd_d5 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        d4.id,
        'B',
        n.p5::smallint,
        n.count_incr::int,
        COALESCE(u.usage, 0)
    FROM bwd_nodes n
    JOIN bwd_d1 d1 ON d1.symbol = n.p1::smallint
    JOIN bwd_d2 d2 ON d2.parent_id = d1.id AND d2.symbol = n.p2::smallint
    JOIN bwd_d3 d3 ON d3.parent_id = d2.id AND d3.symbol = n.p3::smallint
    JOIN bwd_d4 d4 ON d4.parent_id = d3.id AND d4.symbol = n.p4::smallint
    LEFT JOIN bwd_usage_5 u ON u.p1 = n.p1 AND u.p2 = n.p2 AND u.p3 = n.p3
                            AND u.p4 = n.p4 AND u.p5 = n.p5
    WHERE n.depth = 5
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
),

-- Backward depth 6
bwd_d6 AS (
    INSERT INTO trie_nodes (parent_id, tree, symbol, count, usage)
    SELECT
        d5.id,
        'B',
        n.p6::smallint,
        n.count_incr::int,
        0::int
    FROM bwd_nodes n
    JOIN bwd_d1 d1 ON d1.symbol = n.p1::smallint
    JOIN bwd_d2 d2 ON d2.parent_id = d1.id AND d2.symbol = n.p2::smallint
    JOIN bwd_d3 d3 ON d3.parent_id = d2.id AND d3.symbol = n.p3::smallint
    JOIN bwd_d4 d4 ON d4.parent_id = d3.id AND d4.symbol = n.p4::smallint
    JOIN bwd_d5 d5 ON d5.parent_id = d4.id AND d5.symbol = n.p5::smallint
    WHERE n.depth = 6
    ON CONFLICT (parent_id, tree, symbol) DO UPDATE SET
        count = LEAST(trie_nodes.count + EXCLUDED.count, 65535),
        usage = trie_nodes.usage + EXCLUDED.usage
    RETURNING id, parent_id, symbol
)

-- THE FINAL OUTPUT

SELECT
    COALESCE((SELECT SUM(n_tokens) FROM learnable), 0)::bigint AS tokens_learned,
    COALESCE((SELECT COUNT(*) FROM learnable), 0)::bigint AS lines_learned,
    COALESCE((SELECT COUNT(*) FROM input_lines), 0)::bigint AS lines_processed
$learn_horror$;


-- megahal_reply(input text, num_candidates int)
--
-- THE HORROR -- generates a reply using the full MegaHAL pipeline.
-- Tokenize -> keywords -> N candidates (fwd+bwd babble -> eval) -> best -> format.
CREATE OR REPLACE FUNCTION megahal_reply(input text, num_candidates int DEFAULT 10)
RETURNS text
LANGUAGE sql VOLATILE AS
$horror$
WITH RECURSIVE

-- PHASE 1: TOKENIZATION

input_str AS (
    SELECT UPPER(input::text) AS str
),
chars AS (
    SELECT i AS pos, substring(s.str FROM i FOR 1) AS ch
    FROM input_str s, generate_series(1, length(s.str)) AS i
),
classified AS (
    SELECT
        c.pos, c.ch,
        (c.ch ~ '[A-Z]') AS is_alpha,
        (c.ch ~ '[0-9]') AS is_digit,
        LAG(c.ch, 1) OVER (ORDER BY c.pos) AS prev_ch,
        LEAD(c.ch, 1) OVER (ORDER BY c.pos) AS next_ch,
        LAG(c.ch, 2) OVER (ORDER BY c.pos) AS prev2_ch,
        (COALESCE(LAG(c.ch, 1) OVER (ORDER BY c.pos), '') ~ '[A-Z]') AS prev_is_alpha,
        (COALESCE(LAG(c.ch, 1) OVER (ORDER BY c.pos), '') ~ '[0-9]') AS prev_is_digit
    FROM chars c
),
boundaries AS (
    SELECT cl.pos, cl.ch,
        CASE
            WHEN cl.pos = 1 THEN FALSE
            WHEN cl.ch = '''' AND cl.prev_is_alpha
                 AND COALESCE(cl.next_ch, '') ~ '[A-Z]' THEN FALSE
            WHEN cl.prev_ch = '''' AND cl.is_alpha
                 AND COALESCE(cl.prev2_ch, '') ~ '[A-Z]' THEN FALSE
            WHEN cl.is_alpha AND NOT cl.prev_is_alpha THEN TRUE
            WHEN NOT cl.is_alpha AND cl.prev_is_alpha THEN TRUE
            WHEN cl.is_digit != cl.prev_is_digit THEN TRUE
            ELSE FALSE
        END AS is_boundary
    FROM classified cl
),
token_groups AS (
    SELECT b.pos, b.ch,
        SUM(CASE WHEN b.is_boundary THEN 1 ELSE 0 END) OVER (ORDER BY b.pos) AS grp
    FROM boundaries b
),
raw_tokens AS (
    SELECT tg.grp, string_agg(tg.ch, '' ORDER BY tg.pos) AS token
    FROM token_groups tg GROUP BY tg.grp
),
numbered_tokens AS (
    SELECT ROW_NUMBER() OVER (ORDER BY rt.grp) AS pos, rt.token
    FROM raw_tokens rt
),
last_info AS (
    SELECT MAX(pos) AS last_pos,
        (SELECT nt.token FROM numbered_tokens nt ORDER BY nt.pos DESC LIMIT 1) AS last_token
    FROM numbered_tokens
),
normalized AS (
    SELECT 1::bigint AS pos, '.'::text AS token
    WHERE NOT EXISTS (SELECT 1 FROM numbered_tokens)
    UNION ALL
    SELECT nt.pos, nt.token FROM numbered_tokens nt, last_info li
    WHERE substring(li.last_token FROM 1 FOR 1) ~ '[A-Z0-9]'
    UNION ALL
    SELECT li.last_pos + 1, '.' FROM last_info li
    WHERE substring(li.last_token FROM 1 FOR 1) ~ '[A-Z0-9]'
    UNION ALL
    SELECT nt.pos, nt.token FROM numbered_tokens nt, last_info li
    WHERE NOT (substring(li.last_token FROM 1 FOR 1) ~ '[A-Z0-9]')
      AND substring(li.last_token FROM length(li.last_token) FOR 1) IN ('!', '.', '?')
    UNION ALL
    SELECT nt.pos,
           CASE WHEN nt.pos = li.last_pos THEN '.' ELSE nt.token END
    FROM numbered_tokens nt, last_info li
    WHERE NOT (substring(li.last_token FROM 1 FOR 1) ~ '[A-Z0-9]')
      AND NOT (substring(li.last_token FROM length(li.last_token) FOR 1) IN ('!', '.', '?'))
),
tokens AS (
    SELECT pos, token FROM normalized ORDER BY pos
),

-- PHASE 2: KEYWORD EXTRACTION

primary_kw AS (
    SELECT DISTINCT s.id AS symbol_id, s.word, false AS is_aux
    FROM tokens t
    LEFT JOIN swap_pairs sp ON sp.from_word = t.token
    CROSS JOIN LATERAL (SELECT COALESCE(sp.to_word, t.token) AS candidate) sw
    JOIN symbols s ON s.word = sw.candidate
    WHERE substring(sw.candidate FROM 1 FOR 1) ~ '[A-Z0-9]'
      AND NOT EXISTS (SELECT 1 FROM banned_words bw WHERE bw.word = sw.candidate)
      AND NOT EXISTS (SELECT 1 FROM aux_words aw WHERE aw.word = sw.candidate)
),
aux_kw AS (
    SELECT DISTINCT s.id AS symbol_id, s.word, true AS is_aux
    FROM tokens t
    LEFT JOIN swap_pairs sp ON sp.from_word = t.token
    CROSS JOIN LATERAL (SELECT COALESCE(sp.to_word, t.token) AS candidate) sw
    JOIN symbols s ON s.word = sw.candidate
    WHERE substring(sw.candidate FROM 1 FOR 1) ~ '[A-Z0-9]'
      AND NOT EXISTS (SELECT 1 FROM banned_words bw WHERE bw.word = sw.candidate)
      AND EXISTS (SELECT 1 FROM aux_words aw WHERE aw.word = sw.candidate)
      AND EXISTS (SELECT 1 FROM primary_kw)
),
all_keywords AS (
    SELECT * FROM primary_kw UNION ALL SELECT * FROM aux_kw
),
kw_arrays AS (
    SELECT
        COALESCE(array_agg(symbol_id), ARRAY[]::int[]) AS keyword_ids,
        COALESCE(array_agg(symbol_id) FILTER (WHERE is_aux), ARRAY[]::int[]) AS aux_ids
    FROM all_keywords
),

-- PHASE 3: MODEL PARAMETERS

params AS (
    SELECT
        (SELECT value FROM config WHERE key = 'order') AS ord,
        (SELECT id FROM trie_nodes WHERE parent_id IS NULL AND tree = 'F') AS f_root,
        (SELECT id FROM trie_nodes WHERE parent_id IS NULL AND tree = 'B') AS b_root
),

-- PHASE 4: INPUT SYMBOL IDS (for echo rejection)

input_sym_ids AS (
    SELECT COALESCE(array_agg(s.id ORDER BY t.pos), ARRAY[]::int[]) AS ids
    FROM tokens t JOIN symbols s ON s.word = t.token
),

-- PHASE 5: BASELINE CANDIDATE (no keywords)
-- sorry about this

baseline AS (
    SELECT c.reply_syms, c.score
    FROM params p
    CROSS JOIN LATERAL (
        WITH RECURSIVE
        random_child AS (
            SELECT symbol AS sym_id FROM trie_nodes
            WHERE parent_id = p.f_root AND tree = 'F'
              AND symbol NOT IN (0, 1)
            ORDER BY random() LIMIT 1
        ),
        seed AS (SELECT sym_id FROM random_child LIMIT 1),
        initial_fwd_ctx AS (
            SELECT ARRAY[p.f_root,
                (SELECT tn.id FROM trie_nodes tn
                 WHERE tn.parent_id = p.f_root AND tn.tree = 'F' AND tn.symbol = s.sym_id)
            ] || array_fill(NULL::int, ARRAY[p.ord]) AS context
            FROM seed s
        ),
        fwd AS (
            SELECT 1 AS step, ARRAY[s.sym_id] AS reply_syms, ic.context,
                   false AS used_key, false AS done
            FROM seed s, initial_fwd_ctx ic
            UNION ALL
            SELECT f.step+1,
                CASE WHEN ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL THEN f.reply_syms
                     ELSE f.reply_syms || ns.symbol_id END,
                CASE WHEN ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL THEN f.context
                     ELSE nc.context END,
                false, ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL
            FROM fwd f
            CROSS JOIN LATERAL (
                SELECT val AS node_id FROM unnest(f.context) WITH ORDINALITY AS t(val, ord)
                WHERE val IS NOT NULL AND ord <= p.ord + 1 ORDER BY ord DESC LIMIT 1
            ) deepest
            CROSS JOIN LATERAL (SELECT tn.usage FROM trie_nodes tn WHERE tn.id = deepest.node_id) parent
            CROSS JOIN LATERAL (
                WITH ch AS (SELECT tn.id AS node_id, tn.symbol, tn.count FROM trie_nodes tn
                    WHERE tn.parent_id = deepest.node_id AND tn.tree = 'F'),
                nm AS (SELECT node_id, symbol, count, row_number() OVER (ORDER BY random()) AS pos FROM ch),
                cu AS (SELECT symbol, node_id, sum(count) OVER (ORDER BY pos) AS cum FROM nm),
                th AS (SELECT floor(random() * parent.usage)::bigint AS c),
                rp AS (SELECT cu.symbol AS symbol_id, cu.node_id, false AS is_keyword
                       FROM cu, th WHERE cu.cum > th.c ORDER BY cu.cum LIMIT 1)
                SELECT symbol_id::smallint, node_id, is_keyword FROM rp
                UNION ALL SELECT 0::smallint, NULL::int, false WHERE NOT EXISTS (SELECT 1 FROM rp)
                LIMIT 1
            ) ns
            CROSS JOIN LATERAL (
                SELECT array_agg(CASE WHEN d=1 THEN f.context[1]
                    ELSE (SELECT tn.id FROM trie_nodes tn WHERE tn.parent_id = f.context[d-1]
                          AND tn.tree = 'F' AND tn.symbol = ns.symbol_id) END ORDER BY d) AS context
                FROM generate_series(1, array_length(f.context, 1)) AS d
            ) nc
            WHERE NOT f.done AND f.step < 200
        ),
        fwd_result AS (SELECT reply_syms, used_key FROM fwd ORDER BY step DESC LIMIT 1),
        bwd_seed AS (
            SELECT ARRAY[p.b_root] || array_fill(NULL::int, ARRAY[p.ord + 1]) AS context,
                   LEAST(COALESCE(array_length(fr.reply_syms, 1), 0)-1, p.ord) AS walk_idx,
                   fr.reply_syms AS fwd_syms
            FROM fwd_result fr
            UNION ALL
            SELECT nc.context, bs.walk_idx-1, bs.fwd_syms FROM bwd_seed bs
            CROSS JOIN LATERAL (
                SELECT array_agg(CASE WHEN d=1 THEN bs.context[1]
                    ELSE (SELECT tn.id FROM trie_nodes tn WHERE tn.parent_id = bs.context[d-1]
                          AND tn.tree = 'B' AND tn.symbol = bs.fwd_syms[bs.walk_idx+1]) END ORDER BY d) AS context
                FROM generate_series(1, array_length(bs.context, 1)) AS d
            ) nc WHERE bs.walk_idx >= 0
        ),
        bwd_ctx AS (SELECT context FROM bwd_seed ORDER BY walk_idx ASC LIMIT 1),
        bwd AS (
            SELECT 1 AS step, fr.reply_syms, sc.context, false AS used_key, false AS done
            FROM fwd_result fr, bwd_ctx sc
            UNION ALL
            SELECT b.step+1,
                CASE WHEN ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL THEN b.reply_syms
                     ELSE array_prepend(ns.symbol_id, b.reply_syms) END,
                CASE WHEN ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL THEN b.context
                     ELSE nc.context END,
                false, ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL
            FROM bwd b
            CROSS JOIN LATERAL (
                SELECT val AS node_id FROM unnest(b.context) WITH ORDINALITY AS t(val, ord)
                WHERE val IS NOT NULL AND ord <= p.ord + 1 ORDER BY ord DESC LIMIT 1
            ) deepest
            CROSS JOIN LATERAL (SELECT tn.usage FROM trie_nodes tn WHERE tn.id = deepest.node_id) parent
            CROSS JOIN LATERAL (
                WITH ch AS (SELECT tn.id AS node_id, tn.symbol, tn.count FROM trie_nodes tn
                    WHERE tn.parent_id = deepest.node_id AND tn.tree = 'B'),
                nm AS (SELECT node_id, symbol, count, row_number() OVER (ORDER BY random()) AS pos FROM ch),
                cu AS (SELECT symbol, node_id, sum(count) OVER (ORDER BY pos) AS cum FROM nm),
                th AS (SELECT floor(random() * parent.usage)::bigint AS c),
                rp AS (SELECT cu.symbol AS symbol_id, cu.node_id, false AS is_keyword
                       FROM cu, th WHERE cu.cum > th.c ORDER BY cu.cum LIMIT 1)
                SELECT symbol_id::smallint, node_id, is_keyword FROM rp
                UNION ALL SELECT 0::smallint, NULL::int, false WHERE NOT EXISTS (SELECT 1 FROM rp)
                LIMIT 1
            ) ns
            CROSS JOIN LATERAL (
                SELECT array_agg(CASE WHEN d=1 THEN b.context[1]
                    ELSE (SELECT tn.id FROM trie_nodes tn WHERE tn.parent_id = b.context[d-1]
                          AND tn.tree = 'B' AND tn.symbol = ns.symbol_id) END ORDER BY d) AS context
                FROM generate_series(1, array_length(b.context, 1)) AS d
            ) nc
            WHERE NOT b.done AND b.step < 200
        ),
        bwd_result AS (SELECT reply_syms FROM bwd ORDER BY step DESC LIMIT 1)
        SELECT br.reply_syms, 0.0::float8 AS score FROM bwd_result br
    ) c
),

-- PHASE 6: KEYWORD CANDIDATES (N candidates via generate_series + LATERAL)

candidates AS (
    SELECT c.reply_syms, c.score
    FROM generate_series(1, num_candidates) AS n(i)
    CROSS JOIN params p
    CROSS JOIN kw_arrays kw
    CROSS JOIN LATERAL (
        WITH RECURSIVE
        kw_seed AS (
            SELECT unnest(kw.keyword_ids) AS sym_id
            EXCEPT SELECT unnest(kw.aux_ids)
        ),
        eligible AS (
            SELECT ks.sym_id FROM kw_seed ks
            JOIN symbols s ON s.id = ks.sym_id ORDER BY random() LIMIT 1
        ),
        random_child AS (
            SELECT symbol AS sym_id FROM trie_nodes
            WHERE parent_id = p.f_root AND tree = 'F'
              AND symbol NOT IN (0, 1)
            ORDER BY random() LIMIT 1
        ),
        seed AS (
            SELECT sym_id FROM eligible
            UNION ALL SELECT sym_id FROM random_child
            WHERE NOT EXISTS (SELECT 1 FROM eligible)
            LIMIT 1
        ),
        initial_fwd_ctx AS (
            SELECT ARRAY[p.f_root,
                (SELECT tn.id FROM trie_nodes tn
                 WHERE tn.parent_id = p.f_root AND tn.tree = 'F' AND tn.symbol = s.sym_id)
            ] || array_fill(NULL::int, ARRAY[p.ord]) AS context
            FROM seed s
        ),
        fwd AS (
            SELECT 1 AS step, ARRAY[s.sym_id] AS reply_syms, ic.context,
                   s.sym_id = ANY(kw.keyword_ids) AS used_key, false AS done
            FROM seed s, initial_fwd_ctx ic
            UNION ALL
            SELECT f.step+1,
                CASE WHEN ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL THEN f.reply_syms
                     ELSE f.reply_syms || ns.symbol_id END,
                CASE WHEN ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL THEN f.context
                     ELSE nc.context END,
                f.used_key OR COALESCE(ns.is_keyword, false),
                ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL
            FROM fwd f
            CROSS JOIN LATERAL (
                SELECT val AS node_id FROM unnest(f.context) WITH ORDINALITY AS t(val, ord)
                WHERE val IS NOT NULL AND ord <= p.ord + 1 ORDER BY ord DESC LIMIT 1
            ) deepest
            CROSS JOIN LATERAL (SELECT tn.usage FROM trie_nodes tn WHERE tn.id = deepest.node_id) parent
            CROSS JOIN LATERAL (
                WITH ch AS (SELECT tn.id AS node_id, tn.symbol, tn.count FROM trie_nodes tn
                    WHERE tn.parent_id = deepest.node_id AND tn.tree = 'F'),
                nm AS (SELECT node_id, symbol, count, row_number() OVER (ORDER BY random()) AS pos FROM ch),
                kp AS (SELECT n.symbol AS symbol_id, n.node_id, true AS is_keyword FROM nm n
                    WHERE n.symbol = ANY(kw.keyword_ids)
                      AND (f.used_key OR NOT (n.symbol = ANY(kw.aux_ids)))
                      AND NOT (n.symbol = ANY(f.reply_syms))
                    ORDER BY n.pos LIMIT 1),
                cu AS (SELECT symbol, node_id, sum(count) OVER (ORDER BY pos) AS cum FROM nm),
                th AS (SELECT floor(random() * parent.usage)::bigint AS c),
                rp AS (SELECT cu.symbol AS symbol_id, cu.node_id, false AS is_keyword
                       FROM cu, th WHERE cu.cum > th.c ORDER BY cu.cum LIMIT 1)
                SELECT symbol_id::smallint, node_id, is_keyword FROM kp
                UNION ALL SELECT symbol_id::smallint, node_id, is_keyword FROM rp
                    WHERE NOT EXISTS (SELECT 1 FROM kp)
                UNION ALL SELECT 0::smallint, NULL::int, false
                    WHERE NOT EXISTS (SELECT 1 FROM kp) AND NOT EXISTS (SELECT 1 FROM rp)
                LIMIT 1
            ) ns
            CROSS JOIN LATERAL (
                SELECT array_agg(CASE WHEN d=1 THEN f.context[1]
                    ELSE (SELECT tn.id FROM trie_nodes tn WHERE tn.parent_id = f.context[d-1]
                          AND tn.tree = 'F' AND tn.symbol = ns.symbol_id) END ORDER BY d) AS context
                FROM generate_series(1, array_length(f.context, 1)) AS d
            ) nc
            WHERE NOT f.done AND f.step < 200
        ),
        fwd_result AS (SELECT reply_syms, used_key FROM fwd ORDER BY step DESC LIMIT 1),
        bwd_seed AS (
            SELECT ARRAY[p.b_root] || array_fill(NULL::int, ARRAY[p.ord + 1]) AS context,
                   LEAST(COALESCE(array_length(fr.reply_syms, 1), 0)-1, p.ord) AS walk_idx,
                   fr.reply_syms AS fwd_syms
            FROM fwd_result fr
            UNION ALL
            SELECT nc.context, bs.walk_idx-1, bs.fwd_syms FROM bwd_seed bs
            CROSS JOIN LATERAL (
                SELECT array_agg(CASE WHEN d=1 THEN bs.context[1]
                    ELSE (SELECT tn.id FROM trie_nodes tn WHERE tn.parent_id = bs.context[d-1]
                          AND tn.tree = 'B' AND tn.symbol = bs.fwd_syms[bs.walk_idx+1]) END ORDER BY d) AS context
                FROM generate_series(1, array_length(bs.context, 1)) AS d
            ) nc WHERE bs.walk_idx >= 0
        ),
        bwd_ctx AS (SELECT context FROM bwd_seed ORDER BY walk_idx ASC LIMIT 1),
        bwd AS (
            SELECT 1 AS step, fr.reply_syms, sc.context, fr.used_key, false AS done
            FROM fwd_result fr, bwd_ctx sc
            UNION ALL
            SELECT b.step+1,
                CASE WHEN ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL THEN b.reply_syms
                     ELSE array_prepend(ns.symbol_id, b.reply_syms) END,
                CASE WHEN ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL THEN b.context
                     ELSE nc.context END,
                b.used_key OR COALESCE(ns.is_keyword, false),
                ns.symbol_id IN (0,1) OR ns.symbol_id IS NULL
            FROM bwd b
            CROSS JOIN LATERAL (
                SELECT val AS node_id FROM unnest(b.context) WITH ORDINALITY AS t(val, ord)
                WHERE val IS NOT NULL AND ord <= p.ord + 1 ORDER BY ord DESC LIMIT 1
            ) deepest
            CROSS JOIN LATERAL (SELECT tn.usage FROM trie_nodes tn WHERE tn.id = deepest.node_id) parent
            CROSS JOIN LATERAL (
                WITH ch AS (SELECT tn.id AS node_id, tn.symbol, tn.count FROM trie_nodes tn
                    WHERE tn.parent_id = deepest.node_id AND tn.tree = 'B'),
                nm AS (SELECT node_id, symbol, count, row_number() OVER (ORDER BY random()) AS pos FROM ch),
                kp AS (SELECT n.symbol AS symbol_id, n.node_id, true AS is_keyword FROM nm n
                    WHERE n.symbol = ANY(kw.keyword_ids)
                      AND (b.used_key OR NOT (n.symbol = ANY(kw.aux_ids)))
                      AND NOT (n.symbol = ANY(b.reply_syms))
                    ORDER BY n.pos LIMIT 1),
                cu AS (SELECT symbol, node_id, sum(count) OVER (ORDER BY pos) AS cum FROM nm),
                th AS (SELECT floor(random() * parent.usage)::bigint AS c),
                rp AS (SELECT cu.symbol AS symbol_id, cu.node_id, false AS is_keyword
                       FROM cu, th WHERE cu.cum > th.c ORDER BY cu.cum LIMIT 1)
                SELECT symbol_id::smallint, node_id, is_keyword FROM kp
                UNION ALL SELECT symbol_id::smallint, node_id, is_keyword FROM rp
                    WHERE NOT EXISTS (SELECT 1 FROM kp)
                UNION ALL SELECT 0::smallint, NULL::int, false
                    WHERE NOT EXISTS (SELECT 1 FROM kp) AND NOT EXISTS (SELECT 1 FROM rp)
                LIMIT 1
            ) ns
            CROSS JOIN LATERAL (
                SELECT array_agg(CASE WHEN d=1 THEN b.context[1]
                    ELSE (SELECT tn.id FROM trie_nodes tn WHERE tn.parent_id = b.context[d-1]
                          AND tn.tree = 'B' AND tn.symbol = ns.symbol_id) END ORDER BY d) AS context
                FROM generate_series(1, array_length(b.context, 1)) AS d
            ) nc
            WHERE NOT b.done AND b.step < 200
        ),
        bwd_result AS (SELECT reply_syms FROM bwd ORDER BY step DESC LIMIT 1),
        -- Forward evaluation
        fwd_eval AS (
            SELECT 1 AS step,
                   ARRAY[p.f_root] || array_fill(NULL::int, ARRAY[p.ord + 1]) AS context,
                   0.0::float8 AS entropy, 0 AS num,
                   (SELECT reply_syms FROM bwd_result) AS syms
            UNION ALL
            SELECT fe.step+1, nc.context,
                fe.entropy + COALESCE(ev.delta, 0.0),
                fe.num + CASE WHEN ev.scored THEN 1 ELSE 0 END, fe.syms
            FROM fwd_eval fe
            CROSS JOIN LATERAL (SELECT fe.syms[fe.step] AS sym_id) cur
            CROSS JOIN LATERAL (
                SELECT CASE WHEN cur.sym_id = ANY(kw.keyword_ids) AND sc.ctx_count > 0
                            THEN -ln(sc.probability / sc.ctx_count::float8) ELSE 0.0 END AS delta,
                       (cur.sym_id = ANY(kw.keyword_ids) AND sc.ctx_count > 0) AS scored
                FROM (SELECT COALESCE(SUM(child.count::float8 / parent.usage::float8), 0.0) AS probability,
                             COUNT(child.id)::int AS ctx_count
                      FROM generate_series(0, p.ord - 1) AS d(d)
                      LEFT JOIN trie_nodes parent ON parent.id = fe.context[d+1] AND fe.context[d+1] IS NOT NULL
                      LEFT JOIN trie_nodes child ON child.parent_id = parent.id
                          AND child.symbol = cur.sym_id AND child.tree = 'F' AND parent.usage > 0
                      WHERE child.id IS NOT NULL) sc
            ) ev
            CROSS JOIN LATERAL (
                SELECT array_agg(CASE WHEN d=1 THEN fe.context[1]
                    ELSE (SELECT tn.id FROM trie_nodes tn WHERE tn.parent_id = fe.context[d-1]
                          AND tn.tree = 'F' AND tn.symbol = cur.sym_id) END ORDER BY d) AS context
                FROM generate_series(1, array_length(fe.context, 1)) AS d
            ) nc
            WHERE fe.step <= COALESCE(array_length(fe.syms, 1), 0)
        ),
        fwd_ev_r AS (SELECT entropy, num FROM fwd_eval ORDER BY step DESC LIMIT 1),
        -- Backward evaluation
        bwd_eval AS (
            SELECT COALESCE(array_length((SELECT reply_syms FROM bwd_result), 1), 0) AS step,
                   ARRAY[p.b_root] || array_fill(NULL::int, ARRAY[p.ord + 1]) AS context,
                   0.0::float8 AS entropy, 0 AS num,
                   (SELECT reply_syms FROM bwd_result) AS syms
            UNION ALL
            SELECT be.step-1, nc.context,
                be.entropy + COALESCE(ev.delta, 0.0),
                be.num + CASE WHEN ev.scored THEN 1 ELSE 0 END, be.syms
            FROM bwd_eval be
            CROSS JOIN LATERAL (SELECT be.syms[be.step] AS sym_id) cur
            CROSS JOIN LATERAL (
                SELECT CASE WHEN cur.sym_id = ANY(kw.keyword_ids) AND sc.ctx_count > 0
                            THEN -ln(sc.probability / sc.ctx_count::float8) ELSE 0.0 END AS delta,
                       (cur.sym_id = ANY(kw.keyword_ids) AND sc.ctx_count > 0) AS scored
                FROM (SELECT COALESCE(SUM(child.count::float8 / parent.usage::float8), 0.0) AS probability,
                             COUNT(child.id)::int AS ctx_count
                      FROM generate_series(0, p.ord - 1) AS d(d)
                      LEFT JOIN trie_nodes parent ON parent.id = be.context[d+1] AND be.context[d+1] IS NOT NULL
                      LEFT JOIN trie_nodes child ON child.parent_id = parent.id
                          AND child.symbol = cur.sym_id AND child.tree = 'B' AND parent.usage > 0
                      WHERE child.id IS NOT NULL) sc
            ) ev
            CROSS JOIN LATERAL (
                SELECT array_agg(CASE WHEN d=1 THEN be.context[1]
                    ELSE (SELECT tn.id FROM trie_nodes tn WHERE tn.parent_id = be.context[d-1]
                          AND tn.tree = 'B' AND tn.symbol = cur.sym_id) END ORDER BY d) AS context
                FROM generate_series(1, array_length(be.context, 1)) AS d
            ) nc
            WHERE be.step >= 1
        ),
        bwd_ev_r AS (SELECT entropy, num FROM bwd_eval ORDER BY step ASC LIMIT 1),
        scored AS (
            SELECT br.reply_syms,
                   COALESCE(fe.entropy, 0.0) + COALESCE(be.entropy, 0.0) AS raw_entropy,
                   COALESCE(fe.num, 0) + COALESCE(be.num, 0) AS total_num
            FROM bwd_result br LEFT JOIN fwd_ev_r fe ON true LEFT JOIN bwd_ev_r be ON true
        )
        SELECT reply_syms,
               CASE WHEN total_num >= 16 THEN raw_entropy / sqrt(total_num-1) / total_num
                    WHEN total_num >= 8  THEN raw_entropy / sqrt(total_num-1)
                    ELSE raw_entropy END AS score
        FROM scored
    ) c
),

-- PHASE 7: BEST CANDIDATE SELECTION

all_candidates AS (
    SELECT reply_syms, score FROM baseline
    UNION ALL
    SELECT reply_syms, score FROM candidates
),
best_candidate AS (
    SELECT reply_syms
    FROM all_candidates, input_sym_ids isym
    WHERE reply_syms IS DISTINCT FROM isym.ids
      AND array_length(reply_syms, 1) > 1
    ORDER BY score DESC
    LIMIT 1
),

-- PHASE 8: OUTPUT FORMATTING

reply_words AS (
    SELECT s.word, u.ord
    FROM best_candidate bc
    CROSS JOIN LATERAL unnest(bc.reply_syms) WITH ORDINALITY AS u(sym_id, ord)
    JOIN symbols s ON s.id = u.sym_id
),
raw_reply AS (
    SELECT string_agg(word, '' ORDER BY ord) AS str FROM reply_words
),
formatted AS (
    -- Base case: first character
    SELECT
        1 AS pos,
        CASE WHEN substring(r.str FROM 1 FOR 1) ~ '[A-Za-z]'
             THEN upper(substring(r.str FROM 1 FOR 1))
             ELSE substring(r.str FROM 1 FOR 1) END AS out_ch,
        CASE WHEN substring(r.str FROM 1 FOR 1) ~ '[A-Za-z]' THEN false
             ELSE true END AS capitalize_next,
        substring(r.str FROM 1 FOR 1) IN ('!', '.', '?') AS after_terminal,
        length(r.str) AS total_len
    FROM raw_reply r WHERE length(r.str) > 0

    UNION ALL

    -- Recursive: walk each character
    SELECT
        f.pos + 1,
        CASE WHEN f.capitalize_next AND ch.c ~ '[A-Za-z]' THEN upper(ch.c)
             WHEN ch.c ~ '[A-Za-z]' THEN lower(ch.c)
             ELSE ch.c END,
        CASE WHEN f.capitalize_next AND ch.c ~ '[A-Za-z]' THEN false
             WHEN f.after_terminal AND ch.c ~ '^\s$' THEN true
             WHEN f.capitalize_next AND NOT (ch.c ~ '[A-Za-z]') THEN true
             ELSE false END,
        ch.c IN ('!', '.', '?'),
        f.total_len
    FROM formatted f
    CROSS JOIN LATERAL (
        SELECT substring((SELECT str FROM raw_reply) FROM f.pos + 1 FOR 1) AS c
    ) ch
    WHERE f.pos < f.total_len
)

-- THE FINAL OUTPUT

SELECT COALESCE(
    string_agg(out_ch, '' ORDER BY pos),
    'I don''t know enough to answer you yet!'
) AS reply
FROM formatted
$horror$;

-- megahal_greet(num_candidates)
--
-- Generate an initial greeting by picking a random greeting word and
-- using it as input to megahal_reply. Matches original MegaHAL's
-- megahal_initial_greeting() / make_greeting() behavior.
CREATE OR REPLACE FUNCTION megahal_greet(num_candidates int DEFAULT 1)
RETURNS text
LANGUAGE sql VOLATILE AS $$
    SELECT megahal_reply(
        (SELECT word FROM greeting_words ORDER BY random() LIMIT 1),
        num_candidates
    )
$$;

-- megahal_converse(input, num_candidates)
--
-- Learn from the input, then generate a reply. This is the standard
-- conversational loop -- equivalent to the original MegaHAL REPL's
-- learn-then-reply cycle in a single function call.
CREATE OR REPLACE FUNCTION megahal_converse(input text, num_candidates int DEFAULT 10)
RETURNS text
LANGUAGE sql VOLATILE AS $$
    SELECT megahal_reply(
        input,
        num_candidates
    )
    FROM megahal_learn(input)
$$;
