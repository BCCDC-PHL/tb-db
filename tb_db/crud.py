import json

from sqlalchemy import select, delete, and_, update
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
        #"accession_not_empty_string": sample['accession'] != '',
    }

    conditions_met = conditions_for_insertion.values()

    if all(conditions_met):
        db_sample = Sample(
            sample_id = sample['sample_id'],
            #accession = sample['accession'],
            #collection_date = sample['collection_date'],
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
def create_cgmlst_allele_profile(db: Session, scheme: dict, cgmlst_allele_profile: dict[str, object],runid:str):
    """
    Create a single cgMLST allele profile record.

    :param db: Database session
    :type db: sqlalchemy.orm.Session
    :param cgmlst_allele_profile: Dictionary representing a cgMLST allele profile. Must include keys `sample_id`, `profile`, and `percent_called`
    :type cgmlst_allele_profiles: list[dict[str, object]]
    :param runid: Sequencing Run ID
    :type runid: str
    :return: Created cgMLST profiles
    :rtype: models.CgmlstAlleleProfile
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])
    existing_schemes = db.query(CgmlstScheme).all()
    existing_schemes_names = set([s.name for s in existing_schemes])

    sample_id = cgmlst_allele_profile['sample_id']
    if sample_id not in existing_sample_ids:
        db_sample = Sample(
            sample_id = sample_id
        )
        db.add(db_sample)
        db.commit()
    if scheme['name'] not in existing_schemes_names:
        db_scheme = CgmlstScheme(
            name = scheme['name'],
            version = scheme['version'],
            num_loci = scheme['num_loci']
        )
        db.add(db_scheme)
        db.commit()
    stmt = select(Sample).where(Sample.sample_id == sample_id)
    sample = db.scalars(stmt).one()
    library = [lib for lib in sample.library if lib.sequencing_run_id == runid][0]
    stmt = select(CgmlstScheme).where(CgmlstScheme.name == scheme['name'])
    scheme_ins = db.scalars(stmt).one()
    db_cgmlst_allele_profile = CgmlstAlleleProfile(
        library_id = library.id,
        profile = json.dumps(cgmlst_allele_profile['profile']),
        percent_called = cgmlst_allele_profile['percent_called'],
        cgmlst_scheme_id = scheme_ins.id
    )

    select_cgmlst_profile_stmt = select(CgmlstAlleleProfile).where(CgmlstAlleleProfile.library_id == library.id)
    existing_profile_for_sample = db.scalars(select_cgmlst_profile_stmt).one_or_none()

    if existing_profile_for_sample is not None:

        existing_profile_for_sample.percent_called = db_cgmlst_allele_profile.percent_called
        existing_profile_for_sample.profile = db_cgmlst_allele_profile.profile
        db.commit()
        db.refresh(existing_profile_for_sample)

    else:
        db.add(db_cgmlst_allele_profile)

    
    db.commit()
    db.refresh(db_cgmlst_allele_profile)

    return db_cgmlst_allele_profile


def create_cgmlst_allele_profiles(db: Session, scheme: dict, cgmlst_allele_profiles: list[dict[str, object]], runs: str):
    """
    Create multiple cgMLST allele profile records.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param cgmlst_allele_profiles: List of dictionaries representing cgMLST allele profiles.
    :param runs: a list of samples with theirs Sequencing Run ID
    :type runid: list[dict[str,str]], keys: sample ids, value: sequencing run ids
    :type cgmlst_allele_profiles: list[dict[str, object]]
    :return: Created cgMLST allele profiles.
    :rtype: list[models.CgmlstAlleleProfile]
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])
    existing_schemes = db.query(CgmlstScheme).all()
    existing_schemes_names = set([s.name for s in existing_schemes])

    if scheme['name'] not in existing_schemes_names:
        db_scheme = CgmlstScheme(
            name = scheme['name'],
            version = scheme['version'],
            num_loci = scheme['num_loci']
        )
        db.add(db_scheme)
        db.commit()
    stmt = select(CgmlstScheme).where(CgmlstScheme.name == scheme['name'])
    scheme_ins = db.scalars(stmt).one()
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
        print(sample_id)
        print(sample)
        library = [lib for lib in sample.library if lib.sequencing_run_id == runs][0]

        db_cgmlst_allele_profile = CgmlstAlleleProfile(
            library_id = library.id,
            profile = json.dumps(cgmlst_allele_profile['profile']),
            percent_called = cgmlst_allele_profile['percent_called'],
            cgmlst_scheme_id = scheme_ins.id
        )
        

        select_cgmlst_profile_stmt = select(CgmlstAlleleProfile).where(CgmlstAlleleProfile.library_id == library.id)
        existing_profile_for_sample = db.scalars(select_cgmlst_profile_stmt).one_or_none()

        if existing_profile_for_sample is not None:

            existing_profile_for_sample.percent_called = db_cgmlst_allele_profile.percent_called
            existing_profile_for_sample.profile = db_cgmlst_allele_profile.profile
            db.commit()
            db.refresh(existing_profile_for_sample)

        else:
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
            #collection_date = miru_profile['collection_date']
        )
        db.add(db_sample)
        db.commit()
    
    select_sample_stmt = select(Sample).where(Sample.sample_id == sample_id)
    sample = db.scalars(select_sample_stmt).one()
    sample.miru_cluster.append(db_miru_cluster)
    db.commit()

    select_sample_stmt = select(Sample).where(Sample.sample_id == sample_id)
    sample = db.scalars(select_sample_stmt).one()

    vntr_fields = {}
    for k, v in miru_profile.items():
        if k is not None and isinstance(k,str):
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
                #accession = miru_profile['accession'],
                #collection_date = miru_profile['collection_date']
            )
            db.add(db_sample)
            db.commit()
            
        select_sample_stmt = select(Sample).where(Sample.sample_id == sample_id)
        sample = db.scalars(select_sample_stmt).one()


        sample.miru_cluster.append(db_miru_cluster)


        vntr_fields = {}
        for k, v in miru_profile.items():
            if k is not None and isinstance(k,str):
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
    """
    Get miru cluster for a given sample.

    :param db: Database session
    :type db: sqlalchemy.orm.Session
    :param sample_id: Sample ID
    :type sample_id: str
    :return: Miru Cluster name for the sample.
    :rtype: str
    """
    query_result = db.query(Sample).filter(
        Sample.sample_id == sample_id
    )
    miru_clusters = query_result.one_or_none().miru_cluster
    
    miru_cluster_code = []
    for row in miru_clusters:
        miru_cluster_id = row.id
        code = db.query(MiruCluster).get(miru_cluster_id).cluster_id
        miru_cluster_code.append(code)

    return miru_cluster_code


### cgmlst
def add_samples_to_cgmlst_clusters(db: Session, cgmlst_cluster: list[dict[str, object]], runs: str):
    """
    Create multiple cgmlst clusters, for sample specified by `sample_id`.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param cgmlst_cluster: Dict representing a cgmlst cluster.
    :type cgmlst_cluster: dict[str, object]
    :param runs: Dict with sample ids and their run ids
    :type runs: dict[str,str]
    :return: sample with cgmlst cluster added.
    :rtype: models.Sample
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    existing_cgmlst_clusters = db.query(CgmlstCluster).all()
    existing_cgmlst_cluster_ids = set([cluster.cluster_id for cluster in existing_cgmlst_clusters])

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
            return None         
        else:
            select_sample_stmt = select(Sample).where(Sample.sample_id == sample_id)
            sample = db.scalars(select_sample_stmt).one()
            library = [lib for lib in sample.library if lib.sequencing_run_id == runs][0]
        
            library.cgmlst_cluster.append(db_cgmlst_cluster)
            
            db_samples.append(library)
            db.commit()

    return db_samples

### cgmlst
def add_sample_to_cgmlst_cluster(db: Session, sample_id: str, cgmlst_cluster: dict[str, object], runid):
    """
    Create a cgmlst cluster, for sample specified by `sample_id` and `runid`.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param cgmlst_cluster: Dict representing a cgmlst cluster.
    :type cgmlst_cluster: dict[str, object]
    :param runs: Dict with sample ids and their run ids
    :type runs: dict[str,str]
    :return: sample with cgmlst cluster added.
    :rtype: models.Sample
    """
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
        library = [lib for lib in sample.library if lib.sequencing_run_id == runid][0]
        library.cgmlst_cluster.append(db_cgmlst_cluster)
        db.commit()

        return sample


def get_cgmlst_cluster_by_sample_id(db: Session, sample_id: str):
    """
    Get cgmlst cluster(s) for sample specified by `sample_id`.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param sample_id: str representing sample id.
    :type cgmlst_cluster: str
    :return: a list of strings of cgmlst clusters this sample belongs to
    :rtype: str
    """
    query_result = db.query(Sample).filter(
        Sample.sample_id == sample_id
    )

    cgmlst_cluster_code = []
    for i in query_result.one_or_none().library:
        cluster = i.cgmlst_cluster
        for row in cluster:
            cgmlst_cluster_id = row.id
            code = db.query(CgmlstCluster).get(cgmlst_cluster_id).cluster_id
            cgmlst_cluster_code.append(code)

    return cgmlst_cluster_code

def create_libraries(db:Session, libraries: dict[str, object]):
    """
    Create/add libraries tables

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param libraries: str representing sample id.
    :type libraries: dict[str,object], dictionaries representing sample qc, keys:sample_id,sample_name,sequencing_run_id,most_abundant_species_name,most_abundant_species_fraction_total_reads,estimated_genome_size_bp...
    :return: created libraries object
    :rtype: models.Library
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])
    print(existing_samples)

    db_created_libraries = []

    for row in libraries:
        sample_id = row['sample_id']

        if sample_id not in existing_sample_ids:
            db_sample = Sample(
                sample_id = sample_id
            )
            db.add(db_sample)
            db.commit()
        stmt = select(Sample).where(Sample.sample_id == sample_id)
        sample = db.scalars(stmt).one()
        print(sample)
        stmt = select(Library).where(Library.sample_id == sample.id) 
        db_libraries = db.scalars(stmt).fetchmany()

        if not db_libraries:
            library_created = Library(
                    sample_id = sample.id,
                    sequencing_run_id = row['sequencing_run_id'],
                    most_abundant_species_name = row['most_abundant_species_name'],
                    most_abundant_species_fraction_total_reads = row['most_abundant_species_fraction_total_reads'],
                    estimated_genome_size_bp = row['estimated_genome_size_bp'],
                    estimated_depth_coverage = row['estimated_depth_coverage'],
                    total_bases = row['total_bases'],
                    average_base_quality = row['average_base_quality'],
                    percent_bases_above_q30 = row['percent_bases_above_q30'],
                    percent_gc = row['percent_gc']

                    #use below if switching to fastp
                    #total_reads_before_filtering = row['total_reads_before_filtering'],
                    #total_reads_after_filtering	= row['total_reads_after_filtering'],
                    #total_bases_before_filtering = row['total_bases_before_filtering'],	
                    #total_bases_after_filtering	= row['total_bases_after_filtering'],
                    #estimated_depth_coverage = row['estimated_depth_coverage'],
                    #read1_mean_length_before_filtering = row['read1_mean_length_before_filtering'],
                    #read1_mean_length_after_filtering = row['read1_mean_length_after_filtering'],
                    #read2_mean_length_before_filtering = row['read2_mean_length_before_filtering'],
                    #read2_mean_length_after_filtering = row['read2_mean_length_after_filtering'],
                    #q20_bases_before_filtering	= row['q20_bases_before_filtering'],
                    #q20_bases_after_filtering	= row['q20_bases_after_filtering'],
                    #q20_rate_before_filtering = row['q20_rate_before_filtering'],
                    #q20_rate_after_filtering = row['q20_rate_after_filtering'],
                    #q30_bases_before_filtering = row['q30_bases_before_filtering'],
                    #q30_bases_after_filtering = row['q30_bases_after_filtering'],
                    #q30_rate_before_filtering = row['q30_rate_before_filtering'],
                    #q30_rate_after_filtering = row['q30_rate_after_filtering'],
                    #gc_content_before_filtering =row['gc_content_before_filtering'],
                    #gc_content_after_filtering = row['gc_content_after_filtering'],


                )
            db_created_libraries.append(library_created)
            db.add_all(db_created_libraries)
            db.commit()  
            for db_created_library in db_created_libraries:
                db.refresh(db_created_library)

        else:   
            for i in db_libraries:
                libraries_json = {}
                libraries_json['sample_id'] = sample_id
                libraries_json['sequencing_run_id'] = i.sequencing_run_id
                libraries_json['most_abundant_species_name'] = i.most_abundant_species_name
                libraries_json['most_abundant_species_fraction_total_reads'] = i.most_abundant_species_fraction_total_reads
                libraries_json['estimated_genome_size_bp'] = i.estimated_genome_size_bp
                libraries_json['estimated_depth_coverage'] = i.estimated_depth_coverage
                libraries_json['total_bases'] = i.total_bases
                libraries_json['average_base_quality'] = i.average_base_quality
                libraries_json['percent_bases_above_q30'] = i.percent_bases_above_q30
                libraries_json['percent_gc'] = i.percent_gc

                #use below if switching to fastp

                #libraries_json['total_reads_before_filtering'] = i.total_reads_before_filtering
                #libraries_json['total_reads_after_filtering']	= i.total_reads_after_filtering
                #libraries_json['total_bases_before_filtering'] = i.total_bases_before_filtering	
                #libraries_json['total_bases_after_filtering']	= i.total_bases_after_filtering
                #libraries_json['estimated_depth_coverage'] = i.estimated_depth_coverage
                #libraries_json['read1_mean_length_before_filtering'] = i.read1_mean_length_before_filtering
                #libraries_json['read1_mean_length_after_filtering'] = i.read1_mean_length_after_filtering
                #libraries_json['read2_mean_length_before_filtering'] = i.read2_mean_length_before_filtering
                #libraries_json['read2_mean_length_after_filtering'] = i.read2_mean_length_after_filtering
                #libraries_json['q20_bases_before_filtering']	= i.q20_bases_before_filtering
                #libraries_json['q20_bases_after_filtering']	= i.q20_bases_after_filtering
                #libraries_json['q20_rate_before_filtering'] = i.q20_rate_before_filtering
                #libraries_json['q20_rate_after_filtering'] = i.q20_rate_after_filtering
                #libraries_json['q30_bases_before_filtering'] = i.q30_bases_before_filtering
                #libraries_json['q30_bases_after_filtering'] = i.q30_bases_after_filtering
                #libraries_json['q30_rate_before_filtering'] = i.q30_rate_before_filtering
                #libraries_json['q30_rate_after_filtering'] = i.q30_rate_after_filtering
                #libraries_json['gc_content_before_filtering'] =i.gc_content_before_filtering
                #libraries_json['gc_content_after_filtering'] = i.gc_content_after_filtering
                
                if row != libraries_json: #only add if different from what is in the db
                    library_created = Library(
                        sample_id = sample.id,
                        sequencing_run_id = row['sequencing_run_id'],
                        most_abundant_species_name = row['most_abundant_species_name'],
                        most_abundant_species_fraction_total_reads = row['most_abundant_species_fraction_total_reads'],
                        estimated_genome_size_bp = row['estimated_genome_size_bp'],
                        estimated_depth_coverage = row['estimated_depth_coverage'],
                        total_bases = row['total_bases'],
                        average_base_quality = row['average_base_quality'],
                        percent_bases_above_q30 = row['percent_bases_above_q30'],
                        percent_gc = row['percent_gc'],

                        #use below if switching to fastp

                        #total_reads_before_filtering = row['total_reads_before_filtering'],
                        #total_reads_after_filtering	= row['total_reads_after_filtering'],
                        #total_bases_before_filtering = row['total_bases_before_filtering']	,
                        #total_bases_after_filtering	= row['total_bases_after_filtering'],
                        #estimated_depth_coverage = row['estimated_depth_coverage'],
                        #read1_mean_length_before_filtering = row['read1_mean_length_before_filtering'],
                        #read1_mean_length_after_filtering = row['read1_mean_length_after_filtering'],
                        #read2_mean_length_before_filtering = row['read2_mean_length_before_filtering'],
                        #read2_mean_length_after_filtering = row['read2_mean_length_after_filtering'],
                        #q20_bases_before_filtering	= row['q20_bases_before_filtering'],
                        #q20_bases_after_filtering	= row['q20_bases_after_filtering'],
                        #q20_rate_before_filtering = row['q20_rate_before_filtering'],
                        #q20_rate_after_filtering = row['q20_rate_after_filtering'],
                        #q30_bases_before_filtering = row['q30_bases_before_filtering'],
                        #q30_bases_after_filtering = row['q30_bases_after_filtering'],
                        #q30_rate_before_filtering = row['q30_rate_before_filtering'],
                        #q30_rate_after_filtering = row['q30_rate_after_filtering'],
                        #gc_content_before_filtering =row['gc_content_before_filtering'],
                        #gc_content_after_filtering = row['gc_content_after_filtering'],

                    )
                    db_created_libraries.append(library_created)
                    db.add_all(db_created_libraries)
                    db.commit()  
                    for db_created_library in db_created_libraries:
                        db.refresh(db_created_library)
                else:
                    print("qc for sample "+ sample_id, " already exists in the database..")

    return db_created_libraries

def create_complexes(db: Session, complexes: list[dict[str, object]], runs:str):
    """
    Create multiple tb complexes assignment table.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param complexes: List of dictionaries designating MTBC complex, NTM or non-mycobacteria.
    :type complexes: list[dict[str, object]]
    :return: Created tb complexes.
    :rtype: list[models.TbComplex]
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    db_complexes = []
    for complex in complexes:
        sample_id = complex['sample_id']
        print(sample_id)
        if sample_id not in existing_sample_ids:
            db_sample = Sample(
                sample_id = sample_id
            )
            db.add(db_sample)
            db.commit()
        stmt = select(Sample).where(Sample.sample_id == sample_id)
        sample = db.scalars(stmt).one()
        library = [lib for lib in sample.library if lib.sequencing_run_id == runs][0]

        db_complex = library.tb_complex

        if db_complex:
            db_complex.library_id = library.id
            db_complex.mtbc_prop = complex['mtbc_prop']
            db_complex.ntm_prop = complex['ntm_prop']
            db_complex.nonmycobacterium_prop = complex['nonmycobacterium_prop']
            db_complex.unclassified_prop = complex['unclassified_prop']
            db_complex.complex = complex['complex']
            db_complex.reason = complex['reason']
            db_complex.flag = complex['flag']
            db.commit()

        else:
            db_tb_complex = TbComplex(
                library_id = library.id,
                mtbc_prop = complex['mtbc_prop'],
                ntm_prop = complex['ntm_prop'],
                nonmycobacterium_prop = complex['nonmycobacterium_prop'],
                unclassified_prop = complex['unclassified_prop'],
                complex = complex['complex'],
                reason = complex['reason'],
                flag = complex['flag'],

            )
            db_complexes.append(db_tb_complex)
            library.tb_complex.append(db_tb_complex)

    db.add_all(db_complexes)
    db.commit()

    return db_complexes

def create_species(db: Session, species: list[dict[str, object]],runs:str):
    """
    Create multiple tb species table.

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param species: List of dictionaries designating MTBC complex, NTM or non-mycobacteria.
    :type species: list[dict[str, object]]
    :return: Created tb species.
    :rtype: list[models.TbSpecies]
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    for row in species:
        sample_id = row
        stmt = select(Sample).where(Sample.sample_id == sample_id)
        sample = db.scalars(stmt).one()
        print(sample_id)
        library = [lib for lib in sample.library if lib.sequencing_run_id == runs][0]
        #print(library)

        db_species = library.tb_species
        #print(db_species)
        db_created_species = []

        if db_species: #if top5 species already exist, update
        #    continue
            for i in range(len(species[row])) :
                print(i)
                print(len(species[row]))
                db_species[i].library_id = library.id
                db_species[i].taxonomy_level = species[row][i]['taxonomy_level']
                db_species[i].species_name = species[row][i]['name']
                db_species[i].ncbi_taxonomy_id = species[row][i]['ncbi_taxonomy_id']
                db_species[i].fraction_total_reads = species[row][i]['fraction_total_reads']
                db_species[i].num_assigned_reads = species[row][i]['num_assigned_reads']
                db.commit()
        else:

            for item in species[row]:    
                speci_created = TbSpecies(
                        library_id = library.id,
                    
                        taxonomy_level = item['taxonomy_level'],
                        species_name = item['name'],
                        ncbi_taxonomy_id = item['ncbi_taxonomy_id'],
                        fraction_total_reads = item['fraction_total_reads'],
                        num_assigned_reads = item['num_assigned_reads']
                    )
                db_created_species.append(speci_created)
            
            db.add_all(db_created_species)

            for ds in db_created_species:
                library.tb_species.append(ds)
            db.commit()

    return db_created_species


def create_amr_summary(db: Session, amr_report: list[dict[str, object]], runs:str):
    """
    creating models for amr and drug resistance, amr is one to one relationship with sample,whereas drug resistance is one to many

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param amr_report: list of dict representing amr profile for each sample
    :type amr_report: list[dict[str,object]]
    :param runs: dictionary representing samples and their run ids
    :type runs: dict[str,str]
    :return: created amr object
    :rtype: models.AmrProfile
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])
    sample_id = amr_report['id']
    stmt = select(Sample).where(Sample.sample_id == sample_id)
    sample = db.scalars(stmt).one()

    library = [lib for lib in sample.library if lib.sequencing_run_id == runs][0]

    db_amr_profile = library.amr_profile
    

    created_amr_profiles = []

    if not db_amr_profile:

        created_amr = AmrProfile(
            library_id = library.id,
            date = amr_report['timestamp'],
            dr_type = amr_report['drtype'],
            median_depth = amr_report['qc']['median_coverage'],
            tbprofiler_version = amr_report['db_version']
        )
        db.add(created_amr)
        library.amr_profile.append(created_amr)
        db.commit()
        created_amr_profiles.append(created_amr)

    db_amr_profile = library.amr_profile[0]


    for dr_variant in amr_report['dr_variants']: 

        for drug in dr_variant['drugs']:

            existing_drugs = db.query(Drug).all()
            existing_drugs_ids = set([drug.drug_id for drug in existing_drugs])

            amr_drug = drug['drug']
            if amr_drug not in existing_drugs_ids:
                db_created_drug = Drug(
                    drug_id = amr_drug
                )
                db.add(db_created_drug)
                db.commit()

            stmt = select(Drug).where(Drug.drug_id == amr_drug)
            db_drug = db.scalars(stmt).one_or_none()

            created_resistance_profile = DrugMutationProfile(
                amr_id = db_amr_profile.id,
                drug = db_drug.id,
                mutation = dr_variant['gene'] +' ' + dr_variant['nucleotide_change'] + ' ('+ str(dr_variant['freq']) +')'

            )
            print(created_resistance_profile)
            db.add(created_resistance_profile)
            db.commit()

            db_amr_profile.drug_mutation_profile.append(created_resistance_profile)

        db.commit()

        

    return created_amr_profiles



def create_snpit(db: Session, snpit: list[dict[str, object]], runs:str):
    """
    creating models for snpit, snpit is one to one relationship with library 

    :param db: Database session.
    :type db: sqlalchemy.orm.Session
    :param snpit: list of dict representing snpit results for each sample
    :type snpit: list[dict[str,object]]
    :param runs: dictionary representing samples and their run ids
    :type runs: str
    :return: created snpit object
    :rtype: models.Snpit
    """
    existing_samples = db.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    db_snpits = []
    for snp in snpit:
        print(snp)
        sample_id = snp['sample_id']
        print(sample_id)
        if sample_id not in existing_sample_ids:
            db_sample = Sample(
                sample_id = sample_id
            )
            db.add(db_sample)
            db.commit()
        stmt = select(Sample).where(Sample.sample_id == sample_id)
        sample = db.scalars(stmt).one()
        library = [lib for lib in sample.library if lib.sequencing_run_id == runs][0]

        db_snpit = library.snpit

        if db_snpit:
            db_snpit.library_id = library.id
            db_snpit.species = snp['species']
            db_snpit.lineage = snp['lineage']
            db_snpit.sublineage = snp['sublineage']
            db_snpit.name = snp['name']
            db_snpit.percent = snp['percent']

            db.commit()

        else:
            db_snpit = Snpit(
                library_id = library.id,
                species = snp['species'],
                lineage = snp['lineage'],
                sublineage = snp['sublineage'],
                name = snp['name'],
                percent = snp['percent']

            )
            db_snpits.append(db_snpit)
            library.snpit.append(db_snpit)

    db.add_all(db_snpits)
    db.commit()

    return db_snpits
