CREATE TABLE config (
    key   TEXT PRIMARY KEY,
    value INT NOT NULL
);

CREATE TABLE symbols (
    id   SMALLINT PRIMARY KEY,
    word TEXT NOT NULL UNIQUE
);
CREATE SEQUENCE symbols_id_seq AS SMALLINT START WITH 2 OWNED BY symbols.id;

CREATE TABLE trie_nodes (
    id        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_id INT REFERENCES trie_nodes(id),
    tree      CHAR(1) NOT NULL CHECK (tree IN ('F', 'B')),
    symbol    SMALLINT NOT NULL REFERENCES symbols(id),
    usage     INT NOT NULL DEFAULT 0,
    count     INT NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX idx_trie_children ON trie_nodes (parent_id, tree, symbol);

CREATE TABLE banned_words   (word TEXT PRIMARY KEY);
CREATE TABLE aux_words      (word TEXT PRIMARY KEY);
CREATE TABLE greeting_words (word TEXT PRIMARY KEY);
CREATE TABLE swap_pairs     (from_word TEXT NOT NULL, to_word TEXT NOT NULL);
