from setuptools import setup, find_packages


setup(
    name="tb_db",
    version="0.1.0",
    packages=find_packages(),
    scripts=[],
    package_data={},
    install_requires=[
        "psycopg2-binary==2.9.3",
        "sqlalchemy==1.4.40",
        "sqlalchemy-utils==0.40.0",
        "alembic==1.8.0"
    ],
    description="",
    url="",
    author="",
    author_email="",
    include_package_data=True,
    keywords=[],
    zip_safe=False,
    extra_requires={
        "dev": [
            "pytest>=7.1.2",
            "hypothesis==6.61.0",
            "sphinx==5.3.0",
            "sphinx-rtd-theme==1.1.1",
        ]
    },
)
