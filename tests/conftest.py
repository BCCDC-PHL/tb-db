"""
To be used with Pytest
"""

import csv
from datetime import date
import os
import pathlib
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from tb_db.model import Base
from tb_db.model import Sample


@pytest.fixture(scope="session")
def connection():
    engine = create_engine(
        "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            os.environ.get("TEST_DB_USER", "postgres"),
            os.environ.get("TEST_DB_PASSWORD", "postgres"),
            os.environ.get("TEST_DB_HOST", "localhost"),
            os.environ.get("TEST_DB_PORT", "5432"),
            os.environ.get("TEST_DB_NAME", "postgres"),
        )
    )
    return engine.connect()


def seed_database(session):
    test_path = pathlib.Path(__file__).parent
    with open(test_path / "data" / "samples_01.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sample = Sample(
                sample_id=row["sample_id"],
                collection_date=date.fromisoformat(row["collection_date"]),
            )
            session.add(sample)
    session.commit()


@pytest.fixture(scope="session")
def setup_database(connection):
    Base.metadata.bind = connection
    Base.metadata.drop_all()
    Base.metadata.create_all()
    session = scoped_session(sessionmaker(bind=connection))
    seed_database(session)
    session.close()

    yield

    Base.metadata.drop_all()


@pytest.fixture
def db_session(setup_database, connection):
    transaction = connection.begin()
    yield scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=connection)
    )
    transaction.rollback()
