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


@declarative_mixin
class HistoryMixin:
    created_at = Column(DateTime, default=func.now())
    valid_until = Column(DateTime, default=None)

    def new_version(self, session):
        # invalidate current version - generate an update
        old_id = self.id
        session.query(self.__class__).filter_by(id=old_id).update(
            values=dict(valid_until=func.now()), synchronize_session=False
        )

        # make us transient (removes persistent identity)
        # make this an INSERT
        make_transient(self)
        self.id = None

        # set created_at and valid_until, which means we have a new PK.
        self.created_at = func.now()
        self.valid_until = None


@event.listens_for(Session, "before_flush")
def before_flush(session, flush_context, instances):
    for instance in session.dirty:
        if not isinstance(instance, HistoryMixin):
            continue
        if not session.is_modified(instance):
            continue

        if not attributes.instance_state(instance).has_identity:
            continue

        # make it transient
        instance.new_version(session)

        # re-add
        session.add(instance)


class Sample(Base, HistoryMixin):

    sample_id = Column(String)
    accession = Column(String)
    collection_date = Column(Date)

    cgmlst_cluster_id = Column(
        Integer,
        ForeignKey("cgmlst_cluster.id"),
        nullable=True,
    )
    miru_cluster_id = Column(Integer, ForeignKey("miru_cluster.id"), nullable=True)

    cgmlst_cluster = relationship("CgmlstCluster", back_populates="samples")
    miru_cluster = relationship("MiruCluster", back_populates="samples")


class Library(Base):
    """
    """

    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    sequencing_run_id = Column(String)
    library_id = Column(String)


class CgmlstScheme(Base, HistoryMixin):
    """
    """

    name = Column(String)
    version = Column(String)
    num_loci = Column(Integer)


class CgmlstAlleleProfile(Base, HistoryMixin):
    """
    """

    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    cgmlst_scheme_id = Column(Integer, ForeignKey("cgmlst_scheme.id"), nullable=True)
    percent_called = Column(Float)
    profile = Column(JSON)


class MiruProfile(Base, HistoryMixin):
    """
    """

    sample_id = Column(Integer, ForeignKey("sample.id"), nullable=False)
    percent_called = Column(Float)
    profile_by_position = Column(JSON)
    miru_pattern = Column(String)


class CgmlstCluster(Base, HistoryMixin):

    cluster_id = Column(String)

    samples = relationship(
        "Sample", back_populates="cgmlst_cluster", cascade="all, delete-orphan"
    )


class MiruCluster(Base, HistoryMixin):

    cluster_id = Column(String)
    samples = relationship(
        "Sample", back_populates="miru_cluster", cascade="all, delete-orphan"
    )
