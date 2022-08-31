from setuptools import setup, find_packages


setup(name='tb_db',
      version='0.1.0',
      packages=find_packages(),
      scripts=[],
      package_data={},
      install_requires=[
            "psycopg2-binary==2.9.3",
            "sqlalchemy==1.4.40",
            "alembic==1.8.0"
        ],
      description='',
      url='',
      author='',
      author_email='',
      include_package_data=True,
      keywords=[],
      zip_safe=False
)
