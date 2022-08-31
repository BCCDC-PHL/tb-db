from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Float
from sqlalchemy import String
from sqlalchemy import Date
from sqlalchemy import JSON
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Sample(Base):
    __tablename__ = 'sample'
    id = Column(Integer, primary_key=True)
    sample_id = Column(String)
    collection_date = Column(Date)
    cgmlst_cluster_id = Column(Integer, ForeignKey("cgmlst_cluster.id"), nullable=True)
    miru_cluster_id = Column(Integer, ForeignKey("miru_cluster.id"), nullable=True)
    
    cgmlst_cluster = relationship("CgMlstCluster", back_populates="samples")
    miru_cluster = relationship("MiruCluster", back_populates="samples")

class Library(Base):
    __tablename__ = 'library'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    sequencing_run_id = Column(String)
    library_id = Column(String)

class CgMlstScheme(Base):
    __tablename__ = 'cgmlst_scheme'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    version = Column(String)
    num_loci = Column(Integer)

class CgMlstAlleleProfile(Base):
    __tablename__ = 'cgmlst_allele_profile'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    cgmlst_scheme_id = Column(Integer, ForeignKey("cgmlst_scheme.id"), nullable=True)
    percent_called = Column(Float)
    profile = Column(JSON)

class MiruProfile(Base):
    __tablename__ = 'miru_profile'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    percent_called = Column(Float)
    profile = Column(JSON)
    
    
class CgMlstCluster(Base):
    __tablename__ = 'cgmlst_cluster'

    id = Column(Integer, primary_key=True)
    cluster_id = Column(String)
    
    samples = relationship(
        "Sample", back_populates="cgmlst_cluster", cascade="all, delete-orphan"
    )

class MiruCluster(Base):
    __tablename__ = 'miru_cluster'

    id = Column(Integer, primary_key=True)
    cluster_id = Column(String)
    samples = relationship(
        "Sample", back_populates="miru_cluster", cascade="all, delete-orphan"
    )
