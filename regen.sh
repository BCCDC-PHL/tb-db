rm alembic/versions/*.py
alembic revision --autogenerate -m 'init'
alembic upgrade head