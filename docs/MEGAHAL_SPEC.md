# MegaHAL Algorithm Specification

## Document Purpose

This specification describes the MegaHAL conversational engine in sufficient detail
to produce a faithful reimplementation without reference to the original C source
code. It is derived from analysis of the MegaHALv8 codebase (Jason Hutchens, 1998)
and the accompanying paper "Introducing MegaHAL." Where the paper and the code
disagree, the code is authoritative and this document follows the code.

---

## 1. Overview

MegaHAL is a chatbot that learns from conversation using Markov chains. It maintains
two n-gram trie structures — one modeling language in the forward direction and one
in the backward direction. When the user provides input, MegaHAL:

1. Tokenizes the input into an alternating sequence of word and non-word symbols.
2. **Learns** from the input by updating both the forward and backward models.
3. Extracts keywords from the input.
4. Generates many candidate replies seeded by those keywords.
5. Scores each candidate by the "surprise" its keywords cause in the model.
6. Returns the highest-scoring reply that is not identical to the input.

> **Critical ordering note:** The model learns from the user's input *before*
> generating a reply. This means the user's own words are already incorporated into
> the model at generation time, which can cause the bot to echo fragments back.

---

## 2. Data Structures

### 2.1 Symbol (String Token)

A symbol is a byte-string with an explicit length:

- **length**: unsigned 8-bit integer (max 255 characters)
- **word**: byte array of `length` characters (not null-terminated in storage)

All comparisons are **case-insensitive** (both characters are uppercased via
`toupper()` before comparison). If two symbols share a common prefix, the shorter
one compares as less-than.

### 2.2 Dictionary

A dictionary stores an ordered collection of unique symbols. It has:

- **size**: unsigned 32-bit integer — number of entries
- **entry[]**: array of Symbols, in insertion order. Each symbol's position in this
  array is its **symbol ID** (a 16-bit unsigned integer).
- **index[]**: array of 16-bit unsigned integers, kept sorted alphabetically by
  the symbol each element points to. Used for O(log n) binary search.

When a symbol is added:
1. Binary search `index[]` to check if the symbol already exists.
2. If found, return the existing symbol ID.
3. If not found, append the symbol to `entry[]`, giving it the next sequential
   symbol ID. Insert its ID into `index[]` at the sorted position (shifting
   elements to maintain order).

Symbol ID 0 is always `<ERROR>` (a sentinel for "word not found").
Symbol ID 1 is always `<FIN>` (the sentence-termination marker).

These two sentinels are added to every new model dictionary at initialization.

### 2.3 Trie Node

Each node in the Markov trie represents a context state and contains:

- **symbol**: 16-bit unsigned — the symbol ID this node represents
- **usage**: 32-bit unsigned — total count of all observations through this node
  (sum of all children's counts, accumulated during training)
- **count**: 16-bit unsigned — how many times this specific node was the observed
  "next symbol" in its parent's context (capped at 65535; once reached, further
  increments are silently dropped for both `count` and parent `usage`)
- **branch**: 16-bit unsigned — number of child nodes
- **children[]**: array of pointers to child Trie Nodes, kept sorted by `symbol`
  (for O(log n) binary search)

### 2.4 Model

A model contains:

- **order**: unsigned 8-bit integer (default: **5**)
- **forward**: root Trie Node for the forward model
- **backward**: root Trie Node for the backward model
- **context[]**: array of `order + 2` Trie Node pointers, used as a sliding window
  during training and generation (indices 0 through `order + 1`)
- **dictionary**: the model's Dictionary of all known symbols

> **Note on order:** The original paper describes "4th-order Markov models" and
> the source file header says "third-order." The actual code uses `order = 5`.
> This means the trie is 5 levels deep, providing up to 5 symbols of prediction
> context.

### 2.5 Swap Table

A list of word-pair substitutions applied when extracting keywords:

- **size**: 16-bit unsigned — number of pairs
- **from[]**: array of Symbols (the input-side words)
- **to[]**: array of Symbols (the substituted words)

When a word from user input matches a `from` entry, the corresponding `to` entry
is used as the keyword instead. This is a many-to-one mapping (multiple `from`
entries may match the same input word, producing multiple keywords). The swap is
applied only to keywords, not to model training.

---

## 3. Support Files

MegaHAL uses several plain-text configuration files. All are optional — missing
files are silently treated as empty. Lines beginning with `#` are comments and
are skipped.

### 3.1 Training File (`megahal.trn`)

Lines of text used to bootstrap the model before any conversation. Each non-comment
line is treated as user input: uppercased, tokenized, and learned. Typical contents
include canned personality sentences, encyclopedic facts, quotations, and
conversational templates.

### 3.2 Banned Keywords (`megahal.ban`)

One word per line (uppercased). Words in this list are **never** used as keywords
when generating a reply. This filters out common function words like "THE", "AND",
"IS", "BUT", etc. The list contains approximately 387 entries.

### 3.3 Auxiliary Keywords (`megahal.aux`)

One word per line (uppercased). Words in this list are used as keywords **only if**
at least one non-auxiliary keyword has already been selected. This includes pronouns
and other words that are useful for context but should not drive topic selection
on their own (e.g., "I", "YOU", "HE", "SHE", "MY", "YOUR").

### 3.4 Greeting Keywords (`megahal.grt`)

One word per line (uppercased). When generating an initial greeting (before any user
input), one word is chosen at random from this list and used as the sole keyword.
Typical entries: "HELLO", "HI", "HOWDY", "WELCOME", "GREETINGS", "G'DAY".

### 3.5 Swap File (`megahal.swp`)

Tab-or-space-separated pairs, one per line. The left word is the `from` (word to
match in input), the right word is the `to` (keyword to substitute). This
implements perspective-switching so the bot replies from its own point of view:

```
I       YOU
YOU     ME
MY      YOUR
YOUR    MY
WHY     BECAUSE
YES     NO
LOVE    HATE
...
```

Note: `YOU` maps to both `I` and `ME` (two separate entries). This means when the
user says "YOU", both "I" and "ME" are added as keyword candidates.

---

## 4. Tokenization

Input text is converted to uppercase, then split into an alternating sequence of
**word tokens** and **separator tokens** (non-word symbols). This preserves both
vocabulary and punctuation/spacing patterns in the model.

### 4.1 Boundary Detection

A word boundary exists at position `p` in the string if and only if:

1. `p == 0`: **never** a boundary (the first character always starts the first token).
2. `p == len(string)`: **always** a boundary (end of input terminates the last token).
3. **Apostrophe rule**: If `string[p]` is an apostrophe (`'`) and both `string[p-1]`
   and `string[p+1]` are alphabetic, there is **no** boundary. Similarly, if
   `string[p-1]` is an apostrophe and both `string[p-2]` and `string[p]` are
   alphabetic, there is **no** boundary. This keeps contractions like "DON'T",
   "I'M", "YOU'RE" as single tokens.
4. **Alpha transition**: If exactly one of `string[p]` and `string[p-1]` is
   alphabetic (one is, the other isn't), it's a boundary.
5. **Digit transition**: If `isdigit(string[p]) != isdigit(string[p-1])`, it's a
   boundary.

### 4.2 Sentence-Terminal Normalization

After tokenization, the sequence is guaranteed to end with a sentence-terminating
punctuation token:

- If the **last token** starts with an alphanumeric character, a new token `"."` is
  **appended** to the sequence.
- Otherwise, if the last token does not end with `!`, `.`, or `?`, the last token
  is **replaced** with `"."`.

### 4.3 Example

Input: `"Don't you think so?"`

After uppercasing: `"DON'T YOU THINK SO?"`

Tokens: `["DON'T", " ", "YOU", " ", "THINK", " ", "SO", "?"]`

(The apostrophe rule keeps "DON'T" as one token. Each space is its own separator
token. The trailing "?" satisfies the sentence-terminal requirement.)

---

## 5. Learning

Learning updates both the forward and backward trie models. It is skipped entirely
if the number of tokens is less than or equal to the model order (i.e., the input
is too short to form a meaningful context).

### 5.1 Forward Learning

1. Initialize the context: set `context[0]` to the forward root node; set all other
   context slots to NULL.
2. For each token in order (index 0 through `size - 1`):
   a. Add the token to the model's dictionary if it isn't already present, obtaining
      its symbol ID.
   b. Update the model: for each depth `d` from `order + 1` down to `1`, if
      `context[d-1]` is not NULL, set `context[d]` to the result of adding `symbol`
      as a child of `context[d-1]` (creating the child node if necessary, and
      incrementing both the child's `count` and the parent's `usage`).
3. After all tokens, update the model one final time with symbol ID 1 (`<FIN>`) to
   mark the end of the sentence.

### 5.2 Backward Learning

1. Initialize the context: set `context[0]` to the backward root node; set all
   other context slots to NULL.
2. For each token in **reverse** order (index `size - 1` down to `0`):
   a. Look up the token in the model's dictionary (it was added during forward
      learning) to get its symbol ID.
   b. Update the model (same procedure as 5.1 step 2).
3. After all tokens, update with `<FIN>`.

### 5.3 Count Saturation

When a child node's `count` reaches 65535, neither `count` nor the parent's `usage`
are incremented further. This prevents overflow but means the model's probability
estimates can become slightly stale for extremely frequent n-grams.

---

## 6. Keyword Extraction

Given the tokenized input, keywords are extracted in two passes.

### 6.1 Primary Keywords

For each input token (after applying any swap substitutions — see Section 3.5):

1. **Skip** if the token does not exist in the model's dictionary (the model has
   never seen this word).
2. **Skip** if the token's first character is not alphanumeric (punctuation and
   whitespace are never keywords).
3. **Skip** if the token appears in the **banned** keyword list.
4. **Skip** if the token appears in the **auxiliary** keyword list.
5. Otherwise, **add** the token to the keyword set.

### 6.2 Auxiliary Keywords

This pass runs **only if** the primary pass produced at least one keyword.

For each input token (after swap substitutions):

1. **Skip** if the token does not exist in the model's dictionary.
2. **Skip** if the token's first character is not alphanumeric.
3. **Skip** if the token **does not** appear in the auxiliary keyword list (i.e.,
   only aux-listed words are added here).
4. Otherwise, **add** the token to the keyword set.

### 6.3 Swap Application

For each input token, the entire swap table is scanned. If any `from` entry matches
the token (case-insensitive), the corresponding `to` entry is used as the keyword
candidate instead. If multiple swap entries match, all corresponding `to` values
produce candidates. If no swap entry matches, the original token is used.

---

## 7. Reply Generation

### 7.1 Candidate Generation Loop

```
keywords = extract_keywords(input)
best_output = generate_one_reply(empty_keywords)  // baseline reply
if best_output is identical to input:
    best_output = "I don't know enough to answer you yet!"
max_surprise = -1.0
start_time = now()

repeat:
    candidate = generate_one_reply(keywords)
    surprise = evaluate(candidate, keywords)
    if surprise > max_surprise AND candidate ≠ input:
        max_surprise = surprise
        best_output = candidate
until (now() - start_time) >= TIMEOUT

return best_output
```

**TIMEOUT** is **1 second**. The engine generates as many candidates as it can in
that time window and returns the best-scoring one.

The "candidate ≠ input" check (the **dissimilarity test**) compares the candidate's
token sequence against the input's token sequence. They must differ in either total
count or in at least one token (case-insensitive comparison).

### 7.2 Single Reply Generation

Each candidate reply is built in two phases: forward generation from a seed keyword,
then backward generation to reach the beginning of the sentence.

#### 7.2.1 Seeding

A global flag `used_key` is reset to FALSE at the start of each reply.

The **seed** function selects the first symbol for the forward phase:

1. Default seed: pick a random child of the forward root node.
2. If keywords exist: pick a random index `i` into the keyword list. Scan from `i`
   through the list (wrapping around) looking for a keyword that:
   - Exists in the model dictionary, AND
   - Is NOT in the auxiliary keyword list.
3. If such a keyword is found, its symbol ID is the seed. Otherwise, use the default.

The seed symbol becomes the first token of the reply.

#### 7.2.2 Forward Phase

Starting from the seed, repeatedly select the next symbol using the **babble**
function (Section 7.3). After each symbol:

- Append it to the reply sequence.
- Advance the context (update `context[]` by walking deeper into the trie, but
  do **not** modify the trie — this is generation, not learning).
- Stop when `<FIN>` (symbol 1) or `<ERROR>` (symbol 0) is produced.

#### 7.2.3 Backward Phase

1. Re-initialize context with `context[0]` set to the backward root.
2. Walk the backward context forward through the first `min(reply_length - 1, order)`
   tokens of the reply (from that index down to index 0), updating context at each
   step. This re-establishes the backward model's state at the beginning of the
   reply.
3. Repeatedly babble backward, **prepending** each new symbol to the front of the
   reply sequence, until `<FIN>` or `<ERROR>` is produced.

### 7.3 The Babble Function

This function selects a context-weighted random symbol, with a strong bias toward
keywords:

1. **Select deepest context:** Walk `context[0]` through `context[order]`, choosing
   the last non-NULL entry. This is the deepest (most specific) available context
   node.
2. If the selected node has no children, return 0 (sentence terminates).
3. **Random walk with keyword priority:**
   - Pick a random starting index `i` among the node's children.
   - Pick a random count `c` in `[0, node.usage)`.
   - Iterate through children circularly from `i`:
     - Let `sym` = current child's symbol.
     - **If** `sym` is in the keyword set, AND (`used_key` is TRUE **or** `sym` is
       not in the auxiliary list), AND `sym` does not already appear in the reply
       being built: immediately select this symbol and set `used_key = TRUE`.
     - **Otherwise**, subtract the child's `count` from `c`. If `c < 0`, select
       this symbol.
     - Advance to the next child (wrapping around to 0 at the end).

This mechanism has two effects:
- Keywords encountered during the random walk are greedily selected, causing replies
  to incorporate the user's topics.
- When no keyword is found, the selection is probability-weighted (each child is
  selected with probability proportional to its `count / usage`).
- Auxiliary keywords are only used if a non-auxiliary keyword has already been used
  in this reply (the `used_key` flag).
- A keyword already present in the reply is not re-selected, preventing repetition.

---

## 8. Reply Evaluation (Surprise Scoring)

The evaluation function scores a candidate reply by measuring how "surprising"
the keywords are in context — i.e., the information content of the keywords as
judged by the model.

### 8.1 Forward Evaluation

1. Initialize context: `context[0]` = forward root.
2. For each token in the reply (left to right):
   a. Look up the token's symbol ID in the model dictionary.
   b. If the token is a keyword:
      - Initialize `probability = 0`, `context_count = 0`.
      - For each context depth `j` from `0` to `order - 1`:
        - If `context[j]` is not NULL:
          - Find the child of `context[j]` matching this symbol.
          - `probability += child.count / context[j].usage`
          - `context_count += 1`
      - If `context_count > 0`:
        - `entropy -= log(probability / context_count)`
      - Increment `num` (the total number of keyword-in-context evaluations).
   c. Update context (walk deeper into the forward trie without modifying it).

> **Implementation hazard:** The original code calls `find_symbol()` on
> `context[j]` and dereferences the result without checking for NULL. If the
> keyword symbol does not exist as a child of a particular context node, this is
> an undefined-behavior NULL dereference. In practice it rarely triggers because
> the reply was generated from the same model, so the n-gram paths generally exist.
> A faithful reimplementation should either guard against this (skip that context
> depth if the symbol is not found) or replicate the original's behavior.

### 8.2 Backward Evaluation

1. Initialize context: `context[0]` = backward root.
2. For each token in the reply (**right to left**):
   - Same procedure as 8.1, using the backward trie.
   - `entropy` and `num` accumulate across both passes.

### 8.3 Length Penalty

After both passes:

- If `num >= 8`: `entropy /= sqrt(num - 1)`
- If `num >= 16`: `entropy /= num`

(Both conditions can apply simultaneously for `num >= 16`, compounding the penalty.)

This penalizes replies that are too long or that match too many keywords, favoring
concise, targeted responses.

### 8.4 Scoring Summary

The final score is `entropy`, representing the total surprise of all keyword
occurrences, averaged across available context depths in both directions, with a
length penalty. Higher scores indicate more surprising (and thus more "interesting")
replies.

---

## 9. Output Formatting

### 9.1 Capitalization

The final reply string is capitalized as follows:

- The first alphabetic character in the string is uppercased.
- All subsequent alphabetic characters are lowercased.
- After any `!`, `.`, or `?` character followed by a whitespace character (at
  position > 2 in the string), the next alphabetic character is uppercased.

This produces standard sentence-case formatting.

### 9.2 Token Concatenation

Reply tokens are concatenated directly with no additional separators. Since the
model preserves non-word tokens (spaces, punctuation), the output naturally contains
appropriate spacing and punctuation.

---

## 10. Conversation Flow

The complete interaction loop per user turn:

```
1. Read user input string
2. Convert input to uppercase
3. Tokenize input into symbol sequence (Section 4)
4. Learn from tokenized input (Section 5)
5. Extract keywords (Section 6)
6. Generate and score candidate replies for TIMEOUT seconds (Section 7)
7. Capitalize the best reply (Section 9.1)
8. Display the reply
```

For the **initial greeting** (before any user input):
1. Select a random word from the greeting keyword list.
2. Generate a reply using that word as the sole keyword (no learning step).

---

## 11. Brain Persistence (Binary Format)

The model can be saved to and loaded from a binary file (`megahal.brn`). At startup,
if a brain file exists it is loaded; otherwise the training file (`megahal.trn`) is
used to bootstrap.

### 11.1 File Structure

All multi-byte integers are written in **native byte order** (platform-dependent;
the original implementation used whatever the host's `fwrite` produced).

```
[Cookie]        9 bytes: ASCII "MegaHALv8"
[Order]         1 byte:  uint8 — the model order
[Forward Tree]  recursive trie serialization
[Backward Tree] recursive trie serialization
[Dictionary]    word list
```

### 11.2 Trie Node Serialization (Recursive, Pre-Order)

Each node is written as:

```
[symbol]   2 bytes: uint16
[usage]    4 bytes: uint32
[count]    2 bytes: uint16
[branch]   2 bytes: uint16
[children] branch × (recursive node serialization)
```

Children are written in order (they are sorted by symbol ID in memory).

### 11.3 Dictionary Serialization

```
[size]     4 bytes: uint32 — number of entries
For each entry:
  [length] 1 byte:  uint8
  [word]   length bytes: raw characters
```

Entries are written in insertion order (by symbol ID), not alphabetical order.
The sorted index is reconstructed at load time by `add_word`.

---

## 12. Numerical Constants and Limits

| Constant     | Value   | Description                                  |
|-------------|---------|----------------------------------------------|
| `order`     | 5       | Default Markov model order (trie depth)      |
| `TIMEOUT`   | 1       | Reply generation time limit (seconds)        |
| Max symbol length | 255 | Symbol length stored as uint8           |
| Max symbol ID    | 65535 | Symbol ID stored as uint16              |
| Max node count   | 65535 | Node count stored as uint16 (saturates) |
| Max node usage   | ~4.2B | Node usage stored as uint32             |
| Max dict size    | ~4.2B | Dictionary size stored as uint32        |

---

## 13. Implementation Notes

### 13.1 Differences from the Published Paper

This specification is derived from the code, which differs from the 1998 paper in
several ways:

1. **Model order**: The paper says "4th-order Markov models." The code uses order 5.
2. **Learn-then-reply**: The paper describes generating a reply and then updating
   the model. The code learns from input *before* generating a reply.
3. **Evaluation formula**: The paper describes information as `-log P(w|s)`. The
   code averages P(w|s) across all available context depths (0 through order-1)
   before taking the log, and applies length penalties for replies with many
   keywords.
4. **Context depth in evaluation**: The forward and backward models are built to
   depth `order` (5), but evaluation only uses context depths 0 through `order - 1`
   (0 through 4). The deepest context level is used during training and generation
   but not scoring.

### 13.2 Random Number Generation

The original uses `drand48()` (seeded once with `srand48(time(NULL))`), producing
a uniform double in [0, 1). The `rnd(range)` function returns
`floor(drand48() * range)`, giving a uniform integer in [0, range-1].

Any uniform PRNG is acceptable for a port; exact reproduction of conversations
would require matching the PRNG state.

### 13.3 Encoding

All text is processed as raw bytes. The original makes no attempt at Unicode
handling. `isalpha()`, `isalnum()`, `isdigit()`, `toupper()`, and `tolower()` are
used with the C locale. A port targeting Unicode should decide whether to replicate
this ASCII-centric behavior or to extend it.

---

## Appendix A: Default Support File Contents

### A.1 Swap Pairs (`megahal.swp`)

```
DISLIKE     LIKE
HATE        LOVE
I           YOU
I'D         YOU'D
I'LL        YOU'LL
I'M         YOU'RE
I'VE        YOU'VE
LIKE        DISLIKE
LOVE        HATE
ME          YOU
MINE        YOURS
MY          YOUR
MYSELF      YOURSELF
NO          YES
WHY         BECAUSE
YES         NO
YOU         I
YOU         ME
YOU'D       I'D
YOU'LL      I'LL
YOU'RE      I'M
YOU'VE      I'VE
YOUR        MY
YOURS       MINE
YOURSELF    MYSELF
```

### A.2 Auxiliary Keywords (`megahal.aux`)

```
DISLIKE  HE  HER  HERS  HIM  HIS  I  I'D  I'LL  I'M  I'VE
LIKE  ME  MINE  MY  MYSELF  ONE  SHE  THREE  TWO
YOU  YOU'D  YOU'LL  YOU'RE  YOU'VE  YOUR  YOURS  YOURSELF
```

### A.3 Greeting Keywords (`megahal.grt`)

```
G'DAY  GREETINGS  HELLO  HI  HOWDY  WELCOME
```

### A.4 Banned Keywords (`megahal.ban`)

The banned list contains ~387 common English function words. Rather than reproduce
the full list here, the definitive list should be sourced from the original
`megahal.ban` file. Representative entries include:

```
A  ABOUT  AFTER  AGAIN  ALL  ALMOST  ALSO  ALWAYS  AM  AN  AND  ANOTHER
ANY  ARE  AREN'T  AS  AT  BACK  BAD  BE  BEEN  BEFORE  BEING  BEST
BETTER  BIG  BUT  BY  CAN  CAN'T  COME  COULD  DAY  DID  DIDN'T  DO
DOES  DOESN'T  DON'T  DOWN  EACH  EVEN  EVERY  FAR  FEW  FOR  FROM
GET  GIVE  GO  GOING  GOOD  GOT  HAD  HAS  HAVE  HERE  HOW  IF  IN
IS  IT  IT'S  JUST  KEEP  KNOW  LAST  LET  LONG  LOOK  MADE  MAKE
MANY  MAY  MIGHT  MORE  MOST  MUCH  MUST  NEVER  NEW  NEXT  NO  NOT
NOTHING  NOW  OF  OFF  ON  ONE  ONLY  OR  OTHER  OUR  OUT  OVER  OWN
PART  PERHAPS  PUT  QUITE  REALLY  SAID  SAME  SAY  SEE  SEEM  SHALL
SHOULD  SHOW  SMALL  SO  SOME  STILL  SUCH  SURE  TAKE  TELL  THAN
THAT  THE  THEIR  THEM  THEN  THERE  THESE  THEY  THING  THIS  THOSE
THOUGH  TO  TOO  UNDER  UP  US  VERY  WANT  WAS  WAY  WE  WELL  WERE
WHAT  WHEN  WHERE  WHICH  WHILE  WHO  WILL  WITH  WOULD  YET  YOU  YOUR
```

---

## Appendix B: Pseudocode for Core Algorithms

### B.1 Tokenization

```
function tokenize(input):
    input = uppercase(input)
    tokens = []
    offset = 0

    loop:
        if is_boundary(input, offset):
            tokens.append(input[0..offset])
            if offset == len(input): break
            input = input[offset..]
            offset = 0
        else:
            offset += 1

    // Ensure sentence-terminal punctuation
    last = tokens[tokens.length - 1]
    if is_alphanumeric(last[0]):
        tokens.append(".")
    else if last[last.length - 1] not in {'!', '.', '?'}:
        tokens[tokens.length - 1] = "."

    return tokens
```

### B.2 Learning

```
function learn(model, tokens):
    if tokens.length <= model.order: return

    // Forward pass
    clear_context(model)
    context[0] = model.forward
    for token in tokens:
        symbol = add_to_dictionary(model.dictionary, token)
        update_model(model, symbol)
    update_model(model, FIN)  // symbol 1

    // Backward pass
    clear_context(model)
    context[0] = model.backward
    for token in reverse(tokens):
        symbol = lookup(model.dictionary, token)
        update_model(model, symbol)
    update_model(model, FIN)

function update_model(model, symbol):
    for d = model.order + 1 down to 1:
        if context[d - 1] is not NULL:
            context[d] = add_symbol(context[d - 1], symbol)

function add_symbol(parent, symbol):
    child = find_or_create_child(parent, symbol)
    if child.count < 65535:
        child.count += 1
        parent.usage += 1
    return child
```

### B.3 Reply Generation

```
function generate_one_reply(model, keywords):
    reply = []
    used_key = false

    // Forward phase
    clear_context(model)
    context[0] = model.forward
    symbol = seed(model, keywords)
    if symbol == ERROR or symbol == FIN: goto backward

    reply.append(dictionary_word(symbol))
    update_context(model, symbol)

    loop:
        symbol = babble(model, keywords, reply)
        if symbol == ERROR or symbol == FIN: break
        reply.append(dictionary_word(symbol))
        update_context(model, symbol)

backward:
    // Backward phase
    clear_context(model)
    context[0] = model.backward
    for i = min(reply.length - 1, model.order) down to 0:
        symbol = lookup(model.dictionary, reply[i])
        update_context(model, symbol)

    loop:
        symbol = babble(model, keywords, reply)
        if symbol == ERROR or symbol == FIN: break
        reply.prepend(dictionary_word(symbol))
        update_context(model, symbol)

    return reply
```

### B.4 Babble

```
function babble(model, keywords, reply):
    // Find deepest available context
    node = NULL
    for d = 0 to model.order:
        if context[d] is not NULL: node = context[d]

    if node.branch == 0: return 0

    i = random(0, node.branch - 1)
    count = random(0, node.usage - 1)

    loop:
        symbol = node.children[i].symbol
        word = model.dictionary.entry[symbol]

        if word in keywords
           AND (used_key OR word not in aux_list)
           AND word not in reply:
            used_key = true
            return symbol

        count -= node.children[i].count
        if count < 0: return symbol

        i = (i + 1) mod node.branch
```

### B.5 Evaluation

```
function evaluate(model, keywords, reply_tokens):
    entropy = 0.0
    num = 0

    // Forward evaluation
    clear_context(model)
    context[0] = model.forward
    for token in reply_tokens:
        symbol = lookup(model.dictionary, token)
        if token in keywords:
            prob = 0.0; n = 0
            for j = 0 to model.order - 1:
                if context[j] is not NULL:
                    child = find_child(context[j], symbol)
                    prob += child.count / context[j].usage
                    n += 1
            if n > 0: entropy -= log(prob / n)
            num += 1
        update_context(model, symbol)

    // Backward evaluation
    clear_context(model)
    context[0] = model.backward
    for token in reverse(reply_tokens):
        symbol = lookup(model.dictionary, token)
        if token in keywords:
            // Same probability averaging as above
            ...
            num += 1
        update_context(model, symbol)

    // Length penalty
    if num >= 8:  entropy /= sqrt(num - 1)
    if num >= 16: entropy /= num

    return entropy
```
