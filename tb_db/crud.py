import json

from sqlalchemy import select, delete, and_
from sqlalchemy.orm import Session

from .models import *


### Samples
def create_sample(db: Session, sample: dict[str, object]):
    """
    Create a single sample record.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param sample: Dictionary representing a sample. Must include keys `sample_id` and `collection_date`
    :type sample: dict[str, object]
    :return: Created sample
    :rtype: models.Sample
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    db_sample = None

    if sample['sample_id'] not in existing_sample_ids:
        db_sample = Sample(
            sample_id = sample['sample_id'],
            collection_date = sample['collection_date'],
        )
        db.add(db_sample)
        db.commit()
        db.refresh(db_sample)

    return db_sample


def create_samples(db: Session, samples: list[dict[str, object]]):
    """
    Create multiple sample records.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param samples: List of dictionaries representing samples. Must include keys `sample_id` and `collection_date`
    :type samples: list[dict[str, object]]
    :return: Created samples
    :rtype: list[models.Sample]
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    db_samples = []
    for sample in samples:
        if sample['sample_id'] not in existing_sample_ids:
            db_sample = Sample(
                sample_id = sample['sample_id'],
                collection_date = sample['collection_date'],
            )
            db_samples.append(db_sample)

    db.add_all(db_samples)
    db.commit()
    for db_sample in db_samples:
        db.refresh(db_sample)

    return db_samples


def delete_sample(db: Session, sample_id: str):
    """
    Delete all database records for a sample.

    :param db: Database session
    :type db: sqlalchemy.orm.Session
    :param sample_id: Sample ID
    :type sample_id: str
    :return: All deleted records for sample.
    :rtype: list[models.Sample]
    """
    sample_records = db.query(Sample).where(Sample.sample_id == sample_id).all()

    for sample_record in sample_records:
        db.delete(sample_record)
    db.commit()

    return sample_records


### cgMLST
def create_cgmlst_allele_profile(db: Session, cgmlst_allele_profile: dict[str, object]):
    """
    Create a single cgMLST allele profile record.

    :param db: Database session
    :type db: sqlalchemy.orm.Session
    :param cgmlst_allele_profile: Dictionary representing a cgMLST allele profile. Must include keys `sample_id`, `profile`, and `percent_called`
    :type cgmlst_allele_profiles: list[dict[str, object]]
    :return: Created cgMLST profiles
    :rtype: models.CgmlstAlleleProfile
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    sample_id = cgmlst_allele_profile['sample_id']
    if sample_id not in existing_sample_ids:
        db_sample = Sample(
            sample_id = sample_id
        )
        db.add(db_sample)
        db.commit()
    stmt = select(Sample).where(and_(Sample.sample_id == sample_id, Sample.valid_until == None))
    sample = db.scalars(stmt).one()
    db_cgmlst_allele_profile = CgmlstAlleleProfile(
        sample_id = sample.id,
        profile = json.dumps(cgmlst_allele_profile['profile']),
        percent_called = cgmlst_allele_profile['percent_called'],
    )

    db.add(db_cgmlst_profile)
    db.commit()
    db.refresh(db_cgmlst_profile)

    return db_cgmlst_profile


def create_cgmlst_allele_profiles(db: Session, cgmlst_allele_profiles: list[dict[str, object]]):
    """
    Create multiple cgMLST allele profile records.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param cgmlst_allele_profiles: List of dictionaries representing cgMLST allele profiles.
    :type cgmlst_allele_profiles: list[dict[str, object]]
    :return: Created cgMLST allele profiles.
    :rtype: list[models.CgmlstAlleleProfile]
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    db_cgmlst_allele_profiles = []
    for cgmlst_allele_profile in cgmlst_allele_profiles:
        sample_id = cgmlst_allele_profile['sample_id']
        if sample_id not in existing_sample_ids:
            db_sample = Sample(
                sample_id = sample_id
            )
            db.add(db_sample)
            db.commit()
        stmt = select(Sample).where(and_(Sample.sample_id == sample_id, Sample.valid_until == None))
        sample = db.scalars(stmt).one()
        db_cgmlst_allele_profile = CgmlstAlleleProfile(
            sample_id = sample.id,
            profile = json.dumps(cgmlst_allele_profile['profile']),
            percent_called = cgmlst_allele_profile['percent_called'],
        )
        db_cgmlst_allele_profiles.append(db_cgmlst_allele_profile)

    db.add_all(db_cgmlst_allele_profiles)
    db.commit()

    for db_cgmlst_allele_profile in db_cgmlst_allele_profiles:
        db.refresh(db_cgmlst_allele_profile)

    return db_cgmlst_allele_profiles


### MIRU
def create_miru_profile(db: Session, sample_id: str, miru_profile: dict[str, object]):
    """
    Create single MIRU profile record, for sample specified by `sample_id`.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param sample_id: Sample ID
    :type sample_id: str
    :param miru_profile: Dict representing a MIRU profile.
    :type miru_profile: dict[str, object]
    :return: Created MIRU profile.
    :rtype: models.MiruProfile
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    existing_miru_clusters = db.query(MiruCluster).all()
    existing_miru_cluster_ids = set([cluster.cluster_id for cluster in existing_miru_clusters])

    cluster_id = miru_profile['cluster']
    if (cluster_id not in existing_miru_cluster_ids):
        db_miru_cluster = MiruCluster(
            cluster_id = cluster_id,
        )
        db.add(db_miru_cluster)
        db.commit()

    select_miru_cluster_stmt = select(MiruCluster).where(and_(MiruCluster.cluster_id == cluster_id, MiruCluster.valid_until == None))
    db_miru_cluster = db.scalars(select_miru_cluster_stmt).one()

    if sample_id not in existing_sample_ids:
        db_sample = Sample(
            sample_id = sample_id,
            collection_date = miru_profile['collection_date'],
            miru_cluster_id = db_miru_cluster.id,
        )
        db.add(db_sample)
        db.commit()
    else:
        select_sample_stmt = select(Sample).where(and_(Sample.sample_id == sample_id, Sample.valid_until == None))
        sample = db.scalars(select_sample_stmt).one()
        sample.miru_cluster_id = db_miru_cluster.id
        db.commit()

    select_sample_stmt = select(Sample).where(and_(Sample.sample_id == sample_id, Sample.valid_until == None))
    sample = db.scalars(select_sample_stmt).one()

    vntr_fields = {}
    for k, v in miru_profile.items():
        if k.startswith("vntr_locus"):
            vntr_fields[k] = v

    num_fields_called = len(list(filter(lambda x: x != '-', vntr_fields.values())))
    num_fields_total = len(list(vntr_fields.values()))
    if num_fields_total != 0:
        percent_called = num_fields_called / num_fields_total * 100.0
    else:
        percent_called = None

    profile_by_position = {int(k.split('vntr_locus_position_')[1]): v for k, v in vntr_fields.items()}

    db_miru_profile = MiruProfile(
        sample_id = sample.id,
        percent_called = percent_called,
        profile_by_position = json.dumps(profile_by_position),
        miru_pattern = miru_profile['miru_pattern'],
    )
    select_miru_profile_stmt = select(MiruProfile).where(and_(MiruProfile.sample_id == sample.id, MiruProfile.valid_until == None))
    existing_profile_for_sample = db.scalars(select_miru_profile_stmt).one_or_none()
    if existing_profile_for_sample is not None:
        existing_profile_for_sample.percent_called = db_miru_profile.percent_called
        existing_profile_for_sample.profile_by_position = db_miru_profile.profile_by_position
        existing_profile_for_sample.miru_pattern = db_miru_profile.miru_pattern
        db.commit()
        db.refresh(existing_profile_for_sample)
        created_miru_profile = existing_profile_for_sample
    else:
        db.add(db_miru_profile)
        db.commit()
        db.refresh(db_miru_profile)
        created_miru_profile = db_miru_profile

    return created_miru_profile


def create_miru_profiles(db: Session, miru_profiles_by_sample_id: dict[str, object]):
    """
    Create multiple MIRU profile records.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param miru_profiles_by_sample_id:
    :type miru_profiles_by_sample_id: dict[str, object]
    :return: Created MIRU profiles.
    :rtype: list[models.MiruProfile]
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    existing_miru_clusters = db.query(MiruCluster).all()
    existing_miru_cluster_ids = set([cluster.cluster_id for cluster in existing_miru_clusters])

    db_miru_profiles = []
    created_miru_profiles = []
    added_miru_cluster_ids = set()
    for sample_id, miru_profile in miru_profiles_by_sample_id.items():
        cluster_id = miru_profile['cluster']
        if (cluster_id not in existing_miru_cluster_ids) and (cluster_id not in added_miru_cluster_ids):
            db_miru_cluster = MiruCluster(
                cluster_id = cluster_id,
            )
            db.add(db_miru_cluster)
            db.commit()
            added_miru_cluster_ids.add(cluster_id)
        else:
            select_miru_cluster_stmt = select(MiruCluster).where(and_(MiruCluster.cluster_id == cluster_id, MiruCluster.valid_until == None))
            db_miru_cluster = db.scalars(select_miru_cluster_stmt).one()

        if sample_id not in existing_sample_ids:
            db_sample = Sample(
                sample_id = sample_id,
                accession = miru_profile['accession'],
                collection_date = miru_profile['collection_date'],
                miru_cluster_id = db_miru_cluster.id,
            )
            db.add(db_sample)
            db.commit()
        else:
            select_sample_stmt = select(Sample).where(and_(Sample.sample_id == sample_id, Sample.valid_until == None))
            sample = db.scalars(select_sample_stmt).one()
            sample.miru_cluster_id = db_miru_cluster.id
            db.commit()

        select_sample_stmt = select(Sample).where(and_(Sample.sample_id == sample_id, Sample.valid_until == None))
        sample = db.scalars(select_sample_stmt).one()

        vntr_fields = {}
        for k, v in miru_profile.items():
            if k.startswith("vntr_locus"):
                vntr_fields[k] = v

        num_fields_called = len(list(filter(lambda x: x != '-', vntr_fields.values())))
        num_fields_total = len(list(vntr_fields.values()))
        if num_fields_total != 0:
            percent_called = num_fields_called / num_fields_total * 100.0
        else:
            percent_called = None

        profile_by_position = {int(k.split('vntr_locus_position_')[1]): v for k, v in vntr_fields.items()}

        db_miru_profile = MiruProfile(
            sample_id = sample.id,
            percent_called = percent_called,
            profile_by_position = json.dumps(profile_by_position),
            miru_pattern = miru_profile['miru_pattern'],
        )
        select_miru_profile_stmt = select(MiruProfile).where(and_(MiruProfile.sample_id == sample.id, MiruProfile.valid_until == None))
        existing_profile_for_sample = db.scalars(select_miru_profile_stmt).one_or_none()
        if existing_profile_for_sample is not None:
            existing_profile_for_sample.percent_called = db_miru_profile.percent_called
            existing_profile_for_sample.profile_by_position = db_miru_profile.profile_by_position
            existing_profile_for_sample.miru_pattern = db_miru_profile.miru_pattern
            db.commit()
            db.refresh(existing_profile_for_sample)
            created_miru_profiles.append(existing_profile_for_sample)
        else:
            db_miru_profiles.append(db_miru_profile)

    db.add_all(db_miru_profiles)
    db.commit()

    for db_miru_profile in db_miru_profiles:
        db.refresh(db_miru_profile)
        created_miru_profiles.append(db_miru_profile)

    return created_miru_profiles
