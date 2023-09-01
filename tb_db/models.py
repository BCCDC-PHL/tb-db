import sqlalchemy as sa
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Float
from sqlalchemy import String
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import JSON
from sqlalchemy import event
from sqlalchemy import func
from sqlalchemy.orm import attributes
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import declarative_mixin
from sqlalchemy.orm import relationship
from sqlalchemy.orm import make_transient
from sqlalchemy.orm import Session
from sqlalchemy import Table
from sqlalchemy import BigInteger

from sqlalchemy_utils import create_view
from sqlalchemy_utils.view import CreateView, DropView

def camel_to_snake(s: str) -> str:
    """
    Converts camelCase to snake_case
    :param s: String to convert
    :type s: str
    :return: snake_case equivalent of camelCase input
    :rtype: str
    """
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


class Base:
    @declared_attr
    def __tablename__(cls):
        return camel_to_snake(cls.__name__)

    id = Column(Integer, primary_key=True)


Base = declarative_base(cls=Base)

association_table_cgmlst = Table(
    "association_table_cgmlst",
    Base.metadata,
    Column("library_id", ForeignKey("library.id")),
    Column("cgmlst_cluster_id", ForeignKey("cgmlst_cluster.id")),
)

association_table_miru = Table(
    "association_table_miru",
    Base.metadata,
    Column("sample_id", ForeignKey("sample.id"), primary_key=True),
    Column("miru_cluster_id", ForeignKey("miru_cluster.id"), primary_key=True),
)


class Sample(Base):

    sample_id = Column(String)
    accession = Column(String)
    collection_date = Column(Date)

    library = relationship("Library", backref = 'samples')
    miru_profile = relationship("MiruProfile", backref = 'samples', cascade="all,delete")
    miru_cluster = relationship("MiruCluster", secondary=association_table_miru, backref ='samples', cascade="all, delete")



class Library(Base):
    """
    """

    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    sample_name = Column(String)
    sequencing_run_id = Column(String)
    library_id = Column(String)
    most_abundant_species_name = Column(String)
    most_abundant_species_fraction_total_reads = Column(Float)
    estimated_genome_size_bp = Column(BigInteger)
    estimated_depth_coverage = Column(Float)
    total_bases = Column(BigInteger)
    average_base_quality = Column(Float)
    percent_bases_above_q30 = Column(Float)
    percent_gc = Column(Float)


    cgmlst_cluster = relationship("CgmlstCluster", secondary=association_table_cgmlst, backref = 'libraries', cascade="all, delete")

    cgmlst_allele_profile = relationship("CgmlstAlleleProfile", backref = 'libraries', cascade="all,delete")
    

    tb_complex = relationship('TbComplex',backref = 'libraries', cascade = "all,delete")
    tb_species = relationship('TbSpecies',backref = 'libraries', cascade = "all,delete")
    amr_profile = relationship('AmrProfile', backref = 'libraries',cascade = "all,delete")



class CgmlstScheme(Base):
    """
    """

    name = Column(String)
    version = Column(String)
    num_loci = Column(Integer)


class CgmlstAlleleProfile(Base):
    """
    """

    library_id = Column(Integer, ForeignKey("library.id"), nullable=False)
    cgmlst_scheme_id = Column(Integer, ForeignKey("cgmlst_scheme.id"), nullable=True)
    percent_called = Column(Float)
    profile = Column(JSON)


class MiruProfile(Base):
    """
    """

    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    percent_called = Column(Float)
    profile_by_position = Column(JSON)
    miru_pattern = Column(String)


class CgmlstCluster(Base):

    cluster_id = Column(String)


class MiruCluster(Base):

    cluster_id = Column(String)


class TbComplex(Base):

    library_id = Column(Integer, ForeignKey("library.id"), nullable=False)
    mtbc_prop = Column(Float)
    ntm_prop = Column(Float)
    nonmycobacterium_prop = Column(Float)
    unclassified_prop = Column(Float)
    complex = Column(String)
    reason = Column(String)
    flag = Column(String)


class TbSpecies(Base):

    library_id = Column(Integer, ForeignKey("library.id"), nullable= False)
    taxonomy_level = Column(String)
    species_name = Column(String)
    ncbi_taxonomy_id = Column(Float)
    fraction_total_reads = Column(Float)
    num_assigned_reads = Column(Float)


class Drug(Base):

    drug_id = Column(String)


class AmrProfile(Base):

    library_id = Column(Integer, ForeignKey("library.id"),nullable = False)
    date = Column(Date)
    dr_type = Column(String)
    median_depth = Column(Integer)
    tbprofiler_version = Column(JSON)

    drug_mutation_profile = relationship("DrugMutationProfile", backref = 'amr_profile', cascade="all,delete")

class DrugMutationProfile(Base):
    #sample_id = Column(Integer, ForeignKey("sample.id"),nullable = False)
    amr_id = Column(Integer, ForeignKey("amr_profile.id"), nullable= False)
    drug = Column(Integer, ForeignKey("drug.id"), nullable = True)
    mutation = Column(String)

class LibraryView(Base):

    selectable = sa.select(
        Library.id,
        Sample.sample_id,
        Library.sample_name,
        Library.most_abundant_species_name,
        Library.sequencing_run_id,
        Library.most_abundant_species_fraction_total_reads,
        Library.estimated_genome_size_bp,
        Library.estimated_depth_coverage,
        Library.average_base_quality,
        Library.total_bases,
        Library.percent_bases_above_q30,
        Library.percent_gc
    ).join_from(
        Library,
        Sample,
    )

    __table__ = create_view(
        'library_view',
        selectable,
        Base.metadata
    )

    @classmethod
    def create(cls, op):
        cls.drop(op)
        create_sql = CreateView(cls.__table__.fullname, cls.selectable)
        op.execute(create_sql)
        for idx in cls.__table__.indexes:
            idx.create(op.get_bind())

    @classmethod
    def drop(cls, op):
        drop_sql = DropView(cls.__table__.fullname, cascade=False)
        op.execute(drop_sql)



class CgmlstClusterView(Base):

    selectable = sa.select(
        Library.sample_name,
        Library.sequencing_run_id,
        association_table_cgmlst,
        #CgmlstCluster.id,
        CgmlstCluster.cluster_id,
        Library.id,


    ).join_from(
        association_table_cgmlst,
        Library,


    ).join_from(
        association_table_cgmlst,        
        CgmlstCluster,
        #association_table_cgmlst,

    )

    __table__ = create_view(
        'cgmlst_cluster_view',
        selectable,
        Base.metadata
    )

    @classmethod
    def create(cls, op):
        cls.drop(op)
        create_sql = CreateView(cls.__table__.fullname, cls.selectable)
        op.execute(create_sql)
        for idx in cls.__table__.indexes:
            idx.create(op.get_bind())

    @classmethod
    def drop(cls, op):
        drop_sql = DropView(cls.__table__.fullname, cascade=False)
        op.execute(drop_sql)


class MiruClusterView(Base):

    selectable = sa.select(
        Library.sample_name,
        Library.sequencing_run_id,
        association_table_miru,
        MiruCluster.cluster_id,
        Library.id,



    ).join_from(
        association_table_miru,
        Sample,
    ).join_from(
        association_table_miru,
        MiruCluster,
    ).join_from(
        Sample,
        Library,
    )

    __table__ = create_view(
        'miru_cluster_view',
        selectable,
        Base.metadata
    )

    @classmethod
    def create(cls, op):
        cls.drop(op)
        create_sql = CreateView(cls.__table__.fullname, cls.selectable)
        op.execute(create_sql)
        for idx in cls.__table__.indexes:
            idx.create(op.get_bind())

    @classmethod
    def drop(cls, op):
        drop_sql = DropView(cls.__table__.fullname, cascade=False)
        op.execute(drop_sql)


class AmrProfileView(Base):

    selectable = sa.select(
        Library.id,
        Library.sample_name,
        Library.sequencing_run_id,
        AmrProfile.library_id,
        AmrProfile.date,
        AmrProfile.dr_type,
        AmrProfile.median_depth,
        AmrProfile.tbprofiler_version,
	    AmrProfile.id,
        DrugMutationProfile.amr_id,
        DrugMutationProfile.drug,
        DrugMutationProfile.mutation,
        Drug.drug_id,

    ).join_from(
        AmrProfile,
        Library,
    ).join_from(
        AmrProfile,
        DrugMutationProfile,
    ).join_from(
        DrugMutationProfile,
        Drug,
    )

    __table__ = create_view(
        'amr_profile_view',
        selectable,
        Base.metadata
    )

    @classmethod
    def create(cls, op):
        cls.drop(op)
        create_sql = CreateView(cls.__table__.fullname, cls.selectable)
        op.execute(create_sql)
        for idx in cls.__table__.indexes:
            idx.create(op.get_bind())

    @classmethod
    def drop(cls, op):
        drop_sql = DropView(cls.__table__.fullname, cascade=False)
        op.execute(drop_sql)