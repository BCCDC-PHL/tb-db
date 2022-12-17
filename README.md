# TB DB
A database for storing results from genomic analysis of M. tuberculosis.

# Development Status
Currently prototype/demo stage. Not ready for production.

## Getting started

Clone the repo and navigate to the repo directory
```
git clone git@github.com:BCCDC-PHL/tb-db.git
cd tb-db
```

Create a conda environment, activate it, and install the project dependencies. This will also install the 'tb_db' module into the conda environment, in 'editable' mode.
```
conda create -n tb-db python=3.10
conda activate tb-db
pip install -e ".[dev]"
```

If you haven't already done so, create a PostgreSQL user to use for database development.

Create a new PostgreSQL database (replace `username` with your PostgreSQL role name). Run the following SQL commands:
```sql
create database tb;
grant all privileges on tb to username;
```

Configure database connection.

Edit the `dev-config.json` file with the appropriate username, password and database name:
```json
{
  "connection_uri": "postgresql://username:password@localhost/tb"
}
```

Edit the `alembic.ini` file with the appropriate username, password and database name:

```
sqlalchemy.url = postgresql://username:password@localhost/tb
```

The project is configured to auto-generate database migrations.

To generate migrations based on the current model, run the following. Replace `message...` with some message.
Since we haven't stored any migrations to the repo, your message might be some thing like `init db`.:

```
alembic revision --autogenerate -m 'message...'
```

Run migrations:
```
alembic upgrade head
```

Use a database tool such as [DBeaver](https://dbeaver.io/) to confirm that the database is structured as expected.

Load some test data:
```
scripts/load_samples.py -c dev-config.json test/data/samples_01.csv
scripts/load_cgmlst.py -c dev-config.json test/data/cgmlst_01.csv
scripts/load_miru.py -c dev-config.json test/data/miru_01.csv
```

Use a database tool to confirm that the data was loaded as expected.

### Running tests

Unit tests can be written into the `tests` directory. 

Appropriate fixtures to use for testing against the database have been defined in `tests/conftest.py`.

To write a test that needs a database connection, use the `db_session` fixture. For example:
```python

from tb_db.models import Sample

def test_creating_new_sample(db_session):
    sample = Sample(sample_id='sample1')
    db_session.add(sample)
    db_session.commit()
    db_session.refresh(sample)
    assert sample.id is not None
```

The fixtures are designed to create the appropriate tables and load the data needed at the start. Transactions run within a test are rolledback at the end of the test.
So, there is separation between tests. All tables are removed at the of testing, so there is no need to clean up after tests.

To run the tests, run the following:

```bash
pytest
```
