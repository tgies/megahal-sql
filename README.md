# MegaHAL-SQL

*MegaHAL in pure SQL*

A faithful port of [MegaHAL](https://en.wikipedia.org/wiki/MegaHAL), the 1998
Loebner Prize-winning Markov chain chatbot by [Jason
Hutchens](https://github.com/kranzky), implemented in pure SQL.

This is based on [the classic MegaHAL][megahal-github], not Hutchen's
tangentially-related [newer rewrite](https://github.com/kranzky/megahal).

All algorithm logic lives in PostgreSQL queries. No PL/pgSQL, nothing up my
sleeve. Learning is a single SQL statement. Reply generation is a single SQL
statement. Seriously, look at `schema/03-functions.sql`. The Python driver is
just for convenience; you can skip it entirely and use `psql`.

## Architecture

```
driver.py                Thin bootstrap + I/O shell, initializes schema and runs REPL

schema/
  01-tables.sql          Table definitions (symbols, trie_nodes, config, support)
  02-seed.sql            Seed data (<ERROR>, <FIN>, root nodes, default config)
  03-functions.sql       SQL functions (the entire MegaHAL algorithm)

scripts/
  load-support-data.psql Data loader for psql users

data/                    (All lifted directly from the original MegaHAL)
  megahal.trn            Training corpus (may be freely substituted)
  megahal.ban            Banned words (stopwords)
  megahal.aux            Auxiliary keywords (lower priority)
  megahal.swp            Keyword swap pairs
  megahal.grt            Greeting words

tests/                   pytest tests

docs/
  MEGAHAL_SPEC.md        A **machine-generated, not human-reviewed**
                         description of the original C implementation. This
                         was generated using Claude Opus 4.6 in Claude Code,
                         and checked using Claude Opus 4.6 and Gemini 3 Pro.
```

> [!NOTE]
> OK OK OK technically we use the not-pure-SQL `\copy` in some helper scripts
> to perform the stunt of loading some static plaintext word list files from
> the original MegaHAL distribution. This doesn't count as cheating because
> it's not part of the core algorithm, the pure-SQL way is trivial and
> uninteresting, and loading directly from the original distribution's files
> has great "watch this" value.

## SQL Functions

The public API is four SQL functions in `03-functions.sql`:

| Function | Description |
|---|---|
| `megahal_learn(text)` | Learn from one or more lines of text. Splits on newlines, skips comments and blanks. Returns `(tokens_learned, lines_learned, lines_processed)`. |
| `megahal_reply(text, num_candidates)` | Generate a reply to the given text. Tokenizes, extracts keywords, generates and scores candidates, returns the formatted best reply. |
| `megahal_greet(num_candidates)` | Generate an initial greeting by picking a random greeting word as keyword to build a reply from. Original MegaHAL did this once on startup.|
| `megahal_converse(text, num_candidates)` | Learn from the input, then generate a reply. One function call per conversational turn. |

## Quick Start

### With the Python driver

- Docker (for PostgreSQL via `docker-compose.yml`)
- Python 3.10+ and ideally [uv](https://docs.astral.sh/uv/) (for the driver)

> [!NOTE]
> These are prerequisites *for the Python and Docker quick start*. You can do
> it all in SQL, with your own server, without Docker or the driver -- see
> [psql instructions](#with-psql).

```bash
cd megahal-sql
docker compose up -d --wait
uv run python driver.py
```

The driver initializes the schema and loads support data automatically.

### With psql

Run all of this from the project root.

If you don't have your own DB server handy, you can use our `docker-compose.yml`:

```bash
docker compose up -d --wait
```

Connect as normal. Substitute your own connection string if using your own
server:

```bash
# Connect to the database
psql postgresql://megahal:megahal@localhost:5434/megahal
```

Then inside psql:

```sql
-- Set up schema
\i schema/01-tables.sql
\i schema/02-seed.sql
\i schema/03-functions.sql

-- Load support data (banned words, aux words, swap pairs, greetings)
\i scripts/load-support-data.psql

-- Train from the corpus
\set training `cat data/megahal.trn`
SELECT * FROM megahal_learn(:'training');

-- Chat
SELECT megahal_greet();
SELECT megahal_converse('Hello there!');
SELECT megahal_converse('Tell me about the meaning of life.');
```

## How It Works

We implement [Jason Hutchens' MegaHAL algorithm][introducing-megahal]. Note
that the real implementation differs in minor details (e.g. Markov order) from
the paper, and in those cases, we follow the real implementation.

MegaHAL maintains two Markov tries (forward and backward) of configurable depth
(default: 5, which we hard-code here because we have to unroll it). The core
loop:

1. Tokenize input into alternating word/separator tokens
2. Learn by walking both tries, incrementing counts
3. Extract keywords from input (with swap, ban, aux filtering)
4. Generate candidates -- each candidate is built by:
   - Selecting a seed keyword
   - Babbling (see below) forward until `<FIN>`
   - Babbling backward to reach sentence start
5. Score each candidate by information-theoretic surprise
6. Select the highest-scoring non-echo candidate
7. Format with sentence-case capitalization

The babble step is the core modified Markov chaining routine. It finds the
deepest context node, assigns a random permutation to children, greedily picks
keyword matches, and falls back to weighted random selection via cumulative
sums.

Both learning (THE LEARNING HORROR) and generation (THE HORROR) are each a
single SQL statement: depth-unrolled writable CTEs for the trie walks,
recursive CTEs for the Markov chain babbling, and lateral joins holding it all
together.

## Tests

```bash
docker compose up -d --wait
uv run pytest tests/ -v
```

## Notes

### GitHub language statistics

GitHub's language statistics for this repo currently state it's 4% PL/pgSQL,
which is a complete lie and I can't figure out where it's coming up with that.
This is pure SQL unless you count the setup scripts doing `\copy` -- the
equivalent pure-SQL implementation of that is obvious and uninteresting.

### Generative AI Use Disclosure

Large language models (LLMs) were used in development.

**Purpose:** Assisted analysis of original MegaHAL C source; generation of 100%
of `MEGAHAL_SPEC.md`, generation of boilerplate code, smart find/replace and
refactoring, documentation research, generating unsolicited opinions on the
utility and purpose of what I was working on.

**Models:** Anthropic Claude Opus 4.6, Claude Sonnet 4.5; Google Gemini 3 Pro,
Gemini 3 Flash.

## References

- Hutchens, Jason L.; Alder, Michael D. (1998), ["Introducing MegaHAL"][introducing-megahal], NeMLaP3/CoNLL98 Workshop on Human-Computer Conversation, ACL, pp. 271--274.
- Hutchens, Jason L. (1997), ["How to Pass the Turing Test by Cheating"][turing-test-cheating], Technical Report TR97-05, Department of E&E Engineering, University of Western Australia.
- Hutchens, Jason L., ["How MegaHAL Works"][how-megahal-works], MegaHAL homepage.
- Hutchens, Jason L., [Original MegaHAL C source code][megahal-github], GitHub.

[introducing-megahal]: https://aclanthology.org/W98-1233.pdf
[turing-test-cheating]: https://courses.cs.umbc.edu/471/papers/hutchens.pdf
[how-megahal-works]: https://megahal.sourceforge.net/How.html
[megahal-github]: https://github.com/pteichman/megahal
