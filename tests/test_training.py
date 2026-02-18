"""Tests for training file loading via megahal_learn()."""

from pathlib import Path


DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _train(conn, filepath):
    """Load a training file via megahal_learn -- read file, pass as string."""
    text = Path(filepath).read_text()
    row = conn.execute("SELECT * FROM megahal_learn(%s)", (text,)).fetchone()
    return row[2] or 0  # lines_processed


def test_training_populates_symbols(db):
    """Training file should add many symbols to the model."""
    _train(db, DATA_DIR / "megahal.trn")

    (sym_count,) = db.execute(
        "SELECT count(*) FROM symbols"
    ).fetchone()
    # Training file has ~100+ lines of text with varied vocabulary
    assert sym_count > 100


def test_training_populates_forward_trie(db):
    """Forward trie should have children after training."""
    _train(db, DATA_DIR / "megahal.trn")

    (root_f,) = db.execute(
        "SELECT id FROM trie_nodes WHERE parent_id IS NULL AND tree = 'F'"
    ).fetchone()
    (child_count,) = db.execute(
        "SELECT count(*) FROM trie_nodes WHERE parent_id = %s",
        (root_f,),
    ).fetchone()
    assert child_count > 50


def test_training_populates_backward_trie(db):
    """Backward trie should also have children after training."""
    _train(db, DATA_DIR / "megahal.trn")

    (root_b,) = db.execute(
        "SELECT id FROM trie_nodes WHERE parent_id IS NULL AND tree = 'B'"
    ).fetchone()
    (child_count,) = db.execute(
        "SELECT count(*) FROM trie_nodes WHERE parent_id = %s",
        (root_b,),
    ).fetchone()
    assert child_count > 50


def test_training_returns_line_count(db):
    """Training should report the number of lines processed."""
    count = _train(db, DATA_DIR / "megahal.trn")
    # The training file has many non-comment, non-empty lines
    assert count > 50


def test_training_skips_comments(db):
    """Lines starting with '#' should be skipped."""
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".trn", delete=False) as f:
        f.write("# This is a comment\n")
        f.write("Hello world.\n")
        f.write("#Another comment\n")
        f.write("Goodbye friend.\n")
        f.write("\n")  # empty line
        f.write("Final line.\n")
        tmp_path = f.name

    count = _train(db, tmp_path)
    assert count == 3  # only non-comment, non-empty lines

    Path(tmp_path).unlink()
