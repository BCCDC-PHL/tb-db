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


    library = relationship("Library", backref = 'samples')
    miru_profile = relationship("MiruProfile", backref = 'samples', cascade="all,delete")
    miru_cluster = relationship("MiruCluster", secondary=association_table_miru, backref ='samples', cascade="all, delete")



class Library(Base):
    """
    """

    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    sequencing_run_id = Column(String)
    most_abundant_species_name = Column(String)
    most_abundant_species_fraction_total_reads = Column(Float)
    estimated_genome_size_bp = Column(BigInteger)
    estimated_depth_coverage = Column(Float)
    total_bases = Column(BigInteger)
    average_base_quality = Column(Float)
    percent_bases_above_q30 = Column(Float)
    percent_gc = Column(Float)
    
    #use below if switching to using fastp
    #total_reads_before_filtering = Column(BigInteger)
    #total_reads_after_filtering	= Column(BigInteger)
    #total_bases_before_filtering = Column(BigInteger)	
    #total_bases_after_filtering	= Column(BigInteger)
    #estimated_depth_coverage = Column(Float)
    #read1_mean_length_before_filtering = Column(Integer)
    #read1_mean_length_after_filtering = Column(Integer)
    #read2_mean_length_before_filtering = Column(Integer)	
    #read2_mean_length_after_filtering = Column(Integer)
    #q20_bases_before_filtering	= Column(BigInteger)
    #q20_bases_after_filtering	= Column(BigInteger)
    #q20_rate_before_filtering = Column(Float)
    #q20_rate_after_filtering = Column(Float)	
    #q30_bases_before_filtering = Column(BigInteger)
    #q30_bases_after_filtering = Column(BigInteger)
    #q30_rate_before_filtering = Column(Float)	
    #q30_rate_after_filtering = Column(Float)
    #gc_content_before_filtering = Column(Float)
    #gc_content_after_filtering = Column(Float)


    cgmlst_cluster = relationship("CgmlstCluster", secondary=association_table_cgmlst, backref = 'libraries', cascade="all, delete")

    cgmlst_allele_profile = relationship("CgmlstAlleleProfile", backref = 'libraries', cascade="all,delete")
    

    tb_complex = relationship('TbComplex',backref = 'libraries', cascade = "all,delete")
    tb_species = relationship('TbSpecies',backref = 'libraries', cascade = "all,delete")
    amr_profile = relationship('AmrProfile', backref = 'libraries',cascade = "all,delete")
    snpit = relationship('Snpit', backref = 'libraries', cascade = "all,delete")


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

    amr_id = Column(Integer, ForeignKey("amr_profile.id"), nullable= False)
    drug = Column(Integer, ForeignKey("drug.id"), nullable = True)
    mutation = Column(String)

class Snpit(Base):

    library_id = Column(Integer, ForeignKey("library.id"), nullable = False)
    species = Column(String)
    lineage = Column(String)
    sublineage = Column(String)
    name = Column(String)
    percent = Column(Float)
