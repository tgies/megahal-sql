INSERT INTO symbols (id, word) VALUES (0, '<ERROR>');
INSERT INTO symbols (id, word) VALUES (1, '<FIN>');

INSERT INTO trie_nodes (parent_id, tree, symbol, usage, count)
VALUES (NULL, 'F', 0, 0, 0);
INSERT INTO trie_nodes (parent_id, tree, symbol, usage, count)
VALUES (NULL, 'B', 0, 0, 0);

INSERT INTO config (key, value) VALUES ('order', 5);
