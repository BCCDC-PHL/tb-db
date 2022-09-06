from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Float
from sqlalchemy import String
from sqlalchemy import Date
from sqlalchemy import JSON
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import relationship


def camel_to_snake(s):
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


class Base:
    @declared_attr
    def __tablename__(cls):
        return camel_to_snake(cls.__name__)

    id = Column(Integer, primary_key=True)


Base = declarative_base(cls=Base)


class Sample(Base):

    sample_id = Column(String)
    collection_date = Column(Date)
    cgmlst_cluster_id = Column(Integer, ForeignKey("cgmlst_cluster.id"), nullable=True)
    miru_cluster_id = Column(Integer, ForeignKey("miru_cluster.id"), nullable=True)

    cgmlst_cluster = relationship("CgMlstCluster", back_populates="samples")
    miru_cluster = relationship("MiruCluster", back_populates="samples")


class Library(Base):

    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    sequencing_run_id = Column(String)
    library_id = Column(String)


class CgmlstScheme(Base):

    name = Column(String)
    version = Column(String)
    num_loci = Column(Integer)


class CgmlstAlleleProfile(Base):

    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    cgmlst_scheme_id = Column(Integer, ForeignKey("cgmlst_scheme.id"), nullable=True)
    percent_called = Column(Float)
    profile = Column(JSON)


class MiruProfile(Base):

    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    percent_called = Column(Float)
    profile = Column(JSON)


class CgmlstCluster(Base):

    cluster_id = Column(String)

    samples = relationship(
        "Sample", back_populates="cgmlst_cluster", cascade="all, delete-orphan"
    )


class MiruCluster(Base):

    cluster_id = Column(String)
    samples = relationship(
        "Sample", back_populates="miru_cluster", cascade="all, delete-orphan"
    )
