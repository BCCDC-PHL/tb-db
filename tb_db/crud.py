import json

from sqlalchemy import select, delete, and_
from sqlalchemy.orm import Session

from .models import *

import tb_db.utils as utils
import logging

### Samples
def create_sample(db: Session, sample: dict[str, object]):
    """
    Create a single sample record.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param sample: Dictionary representing a sample. Must include keys `sample_id`,
                   `accession`, and `collection_date`.
    :type sample: dict[str, object]
    :return: Created sample
    :rtype: models.Sample|NoneType
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    db_sample = None

    conditions_for_insertion = {
        "sample_id_not_in_db": sample['sample_id'] not in existing_sample_ids,
        "sample_id_not_empty_string": sample['sample_id'] != '',
        "accession_not_empty_string": sample['accession'] != '',
    }

    conditions_met = conditions_for_insertion.values()

    if all(conditions_met):
        db_sample = Sample(
            sample_id = sample['sample_id'],
            accession = sample['accession'],
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


def get_sample(db: Session, sample_id: str):
    """
    Get current valid database record for a sample.

    :param db: Database session
    :type db: sqlalchemy.orm.Session
    :param sample_id: Sample ID
    :type sample_id: str
    :return: Current valid database record for the sample.
    :rtype: models.Sample|NoneType
    """
    sample_record = db.query(Sample).where(Sample.sample_id == sample_id).one_or_none()

    return sample_record


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
    stmt = select(Sample).where(Sample.sample_id == sample_id)
    sample = db.scalars(stmt).one()
    db_cgmlst_allele_profile = CgmlstAlleleProfile(
        sample_id = sample.id,
        profile = json.dumps(cgmlst_allele_profile['profile']),
        percent_called = cgmlst_allele_profile['percent_called'],
    )

    db.add(db_cgmlst_allele_profile)
    db.commit()
    db.refresh(db_cgmlst_allele_profile)

    return db_cgmlst_allele_profile


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
        stmt = select(Sample).where(Sample.sample_id == sample_id)
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
            cluster_id = cluster_id
        )
        db.add(db_miru_cluster)
        db.commit()

    select_miru_cluster_stmt = select(MiruCluster).where(MiruCluster.cluster_id == cluster_id)
    db_miru_cluster = db.scalars(select_miru_cluster_stmt).one()

    if sample_id not in existing_sample_ids:
        db_sample = Sample(
            sample_id = sample_id,
            collection_date = miru_profile['collection_date']
        )
        db.add(db_sample)
        db.commit()
    
    select_sample_stmt = select(Sample).where(Sample.sample_id == sample_id)
    sample = db.scalars(select_sample_stmt).one()
    sample_miru_cluster = SampleMiruCluster(
        sample_id = sample.id,
        cluster_id = db_miru_cluster.id

    )

    db.add(sample_miru_cluster)
    db.commit()

    select_sample_stmt = select(Sample).where(Sample.sample_id == sample_id)
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
    select_miru_profile_stmt = select(MiruProfile).where(MiruProfile.sample_id == sample.id)
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
                cluster_id = cluster_id
            )
            db.add(db_miru_cluster)
            db.commit()
            added_miru_cluster_ids.add(cluster_id)
        else:
            select_miru_cluster_stmt = select(MiruCluster).where(MiruCluster.cluster_id == cluster_id)
            db_miru_cluster = db.scalars(select_miru_cluster_stmt).one()

        if sample_id not in existing_sample_ids:
            db_sample = Sample(
                sample_id = sample_id,
                accession = miru_profile['accession'],
                collection_date = miru_profile['collection_date']
                #miru_cluster_id = db_miru_cluster.id,
            )
            db.add(db_sample)
            db.commit()
        else:
            select_sample_stmt = select(Sample).where(Sample.sample_id == sample_id)
            sample = db.scalars(select_sample_stmt).one()
            #sample.miru_cluster_id = db_miru_cluster.id
            

        select_sample_stmt = select(Sample).where(Sample.sample_id == sample_id)
        sample = db.scalars(select_sample_stmt).one()

        sample_miru_cluster = SampleMiruCluster(
        sample_id = sample.id,
        cluster_id = db_miru_cluster.id

        )
        db.add(sample_miru_cluster)
        db.commit()

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
            miru_pattern = miru_profile['miru_pattern']
        )
        select_miru_profile_stmt = select(MiruProfile).where(MiruProfile.sample_id == sample.id)
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


def get_miru_cluster_by_sample_id(db: Session, sample_id: str):
    
    query_result = db.query(Sample).filter(
        Sample.sample_id == sample_id
    )
    id = query_result.one_or_none().id

 
    query_result = db.query(SampleMiruCluster).filter(
        SampleMiruCluster.sample_id == id
    )

    
    miru_cluster_id = query_result.one_or_none().cluster_id
    
    miru_cluster_code = db.query(MiruCluster).get(miru_cluster_id).cluster_id

    return miru_cluster_code


### cgmlst
def add_samples_to_cgmlst_clusters(db: Session, cgmlst_cluster: list[dict[str, object]]):
    """
    Create multiple cgmlst clusters, for sample specified by `sample_id`.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param sample_id: Sample ID
    :type sample_id: str
    :param cgmlst_cluster: Dict representing a cgmlst cluster.
    :type cgmlst_cluster: dict[str, object]
    :return: sample with cgmlst cluster added.
    :rtype: models.Sample
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    existing_cgmlst_clusters = db.query(CgmlstCluster).all()
    existing_cgmlst_cluster_ids = set([cluster.cluster_id for cluster in existing_cgmlst_clusters])

    #cluster_id = cgmlst_cluster['cluster']
    db_samples = []
    added_cgmlst_cluster_ids = set()
    for row in cgmlst_cluster:
        sample_id = row['sample_id']
        cluster_id = row['cluster']
        if (cluster_id not in existing_cgmlst_cluster_ids) and (cluster_id not in added_cgmlst_cluster_ids):
            db_cgmlst_cluster = CgmlstCluster(
                cluster_id = cluster_id
            )
            db.add(db_cgmlst_cluster)
            db.commit()
            added_cgmlst_cluster_ids.add(cluster_id)
        select_cgmlst_cluster_stmt = select(CgmlstCluster).where(CgmlstCluster.cluster_id == cluster_id)
        db_cgmlst_cluster = db.scalars(select_cgmlst_cluster_stmt).one()
        if sample_id not in existing_sample_ids:
            logging.warning('cannot add cgmlst cluster for a sample that does not exist...')         
        else:
            select_sample_stmt = select(Sample).where(Sample.sample_id == sample_id)
            sample = db.scalars(select_sample_stmt).one()
            #sample.cgmlst_cluster_id = db_cgmlst_cluster.id
            cgmlst_sample_cluster = SampleCgmlstCluster(
                sample_id = sample.id,
                cluster_id = db_cgmlst_cluster.id
            )
            db_samples.append(sample)
            db.add(cgmlst_sample_cluster)
            db.commit()
            #db.refresh()


    #select_sample_stmt = select(Sample).where(and_(Sample.sample_id == sample_id, Sample.valid_until == None))
    #sample = db.scalars(select_sample_stmt).one()
    return db_samples

### cgmlst
def add_sample_to_cgmlst_cluster(db: Session, sample_id: str, cgmlst_cluster: dict[str, object]):

    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    existing_cgmlst_clusters = db.query(CgmlstCluster).all()
    existing_cgmlst_cluster_ids = set([cluster.cluster_id for cluster in existing_cgmlst_clusters])

    cluster_id = cgmlst_cluster['cluster']
    if (cluster_id not in existing_cgmlst_cluster_ids):
        db_cgmlst_cluster = CgmlstCluster(
            cluster_id = cluster_id
        )
        db.add(db_cgmlst_cluster)
        db.commit()

    select_cgmlst_cluster_stmt = select(CgmlstCluster).where(CgmlstCluster.cluster_id == cluster_id)
    db_cgmlst_cluster = db.scalars(select_cgmlst_cluster_stmt).one()

    if sample_id not in existing_sample_ids:
        logging.warning('cannot add cgmlst cluster for a sample that does not exist...')
        return None

    else:
        select_sample_stmt = select(Sample).where(Sample.sample_id == sample_id)
        sample = db.scalars(select_sample_stmt).one()
        #sample.cgmlst_cluster_id = db_cgmlst_cluster.id
        cgmlst_sample_cluster = SampleCgmlstCluster(
                sample_id = sample.id,
                cluster_id = db_cgmlst_cluster.id
        )
        db.add(cgmlst_sample_cluster)
        db.commit()
        return sample


def get_cgmlst_cluster_by_sample_id(db: Session, sample_id: str):
    
    query_result = db.query(Sample).filter(
        Sample.sample_id == sample_id
    )
    id = query_result.one_or_none().id

 
    query_result = db.query(SampleCgmlstCluster).filter(
        SampleCgmlstCluster.sample_id == id
    )

    
    cgmlst_cluster_id = query_result.one_or_none().cluster_id
    
    cgmlst_cluster_code = db.query(CgmlstCluster).get(cgmlst_cluster_id).cluster_id


    return cgmlst_cluster_code



