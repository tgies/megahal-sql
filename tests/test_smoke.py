def test_db_connection(db):
    (version,) = db.execute("SELECT version()").fetchone()
    assert "PostgreSQL" in version
