import datetime
import json
import logging
import os
import unittest

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

import alembic
import alembic.config

import tb_db.models as models
import tb_db.crud as crud
import tb_db.utils as utils

from hypothesis import settings, Phase, Verbosity, given, note, strategies as st
from hypothesis.stateful import rule, precondition, RuleBasedStateMachine, Bundle

ASCII_ALPHANUMERIC = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
    "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
    "U", "V", "W", "X", "Y", "Z",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
    "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
    "u", "v", "w", "x", "y", "z",
]

ASCII_NUMERIC = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
]

ASCII_SYMBOLS = [
    "!", '"', "#", "$", "%", "&", "'", "(", ")", "*",
    "+", ",", "-", ".", "/", ":", ";", "<", "=", ">",
    "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|",
    "}", "~", 
]

ASCII_ACCEPTABLE_IDENTIFIER_SYMBOLS = [
    "-", "_", ".",
]

ASCII_SPACE = [" "]

connection_uri = "sqlite:///:memory:"

project_root_path = os.path.join(os.path.dirname(__file__), '..')
alembic_dir_path = os.path.join(project_root_path, 'alembic')
alembic_config_file_path = os.path.join(project_root_path, "alembic.ini")
alembic_cfg = alembic.config.Config(file_=alembic_config_file_path)
alembic_cfg.set_main_option('script_location', alembic_dir_path)
alembic_cfg.set_main_option('sqlalchemy.url', connection_uri)

# We do this to prevent the logging.config.fileConfig() method
# from being called in alembic/env.py.
# This gives us better control of logging while testing.
alembic_cfg.config_file_name = None
logging.getLogger('alembic').setLevel(logging.CRITICAL)

log = logging.getLogger(__name__)
logging.basicConfig(format = '%(asctime)s %(module)s %(levelname)s: %(message)s',
                    datefmt = '%Y-%m-%dT%H:%M:%S',
                    level = logging.DEBUG)

class TestCrudSample(unittest.TestCase):

    def setUp(self):
        alembic.command.upgrade(alembic_cfg, 'head')

        self.engine = create_engine(connection_uri)
        self.session = Session(self.engine)
        models.Base.metadata.create_all(self.engine)


    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)


    def test_create_sample_unit_0(self):
        
        sample_dict = {
            'sample_id': 'SAM001',
            'accession': 'ACC001',
            'collection_date': datetime.date(1970, 1, 1),          
        }

        created_sample = crud.create_sample(self.session, sample_dict)

        self.assertIsNotNone(created_sample)
        self.assertEqual(created_sample.id, 1)

    def test_get_miru_cluster(self):
        miru_dict = {
            'key': 'SAM001',
            'cluster': 'BC278',
            'collection_date': datetime.date(1970, 1, 1),
            'acc_num':'ACC001',
            'miru_02':'2',
            'miru_24':'2',
            'miru_26':'2',
            'miru_pattern':'123456789'
        }
        created_sample = crud.create_miru_profile(self.session,'SAM001', miru_dict)
        miru_id = crud.get_miru_cluster_by_sample_id(self.session, 'SAM001')
        self.assertIsNotNone(created_sample)
        self.assertEqual(miru_id,['BC278'])

    def test_get_cgmlst_cluster(self):
        cgmlst_cluster_dict = {
            'key': 'SAM001',
            'cluster': 'BC300'
        }
        sample_dict = {
            'sample_id': 'SAM001',
            'accession': 'ACC001',
            'collection_date': datetime.date(1970, 1, 1)            
        }

        created_sample = crud.create_sample(self.session, sample_dict)
        #self.assertEqual(created_sample.id,1)
        
        result = crud.add_sample_to_cgmlst_cluster(self.session,'SAM001', cgmlst_cluster_dict)

        cgmlst_id = crud.get_cgmlst_cluster_by_sample_id(self.session, 'SAM001')

        self.assertIsNotNone(created_sample)
        self.assertEqual(cgmlst_id,['BC300'])


    def test_get_miru_cluster(self):
        miru_dict = {
            'key': 'SAM001',
            'cluster': 'BC278',
            'collection_date': datetime.date(1970, 1, 1),
            'acc_num':'ACC001',
            'miru_02':'2',
            'miru_24':'2',
            'miru_26':'2',
            'miru_pattern':'123456789'
        }
        created_sample = crud.create_miru_profile(self.session,'SAM001', miru_dict)
        miru_id = crud.get_miru_cluster_by_sample_id(self.session, 'SAM001')
        self.assertIsNotNone(created_sample)
        self.assertEqual(miru_id,['BC278'])

    def test_get_cgmlst_cluster(self):
        cgmlst_cluster_dict = {
            'key': 'SAM001',
            'cluster': 'BC300'
        }
        sample_dict = {
            'sample_id': 'SAM001',
            'accession': 'ACC001',
            'collection_date': datetime.date(1970, 1, 1)            
        }

        created_sample = crud.create_sample(self.session, sample_dict)
        #self.assertEqual(created_sample.id,1)
        
        result = crud.add_sample_to_cgmlst_cluster(self.session,'SAM001', cgmlst_cluster_dict)

        cgmlst_id = crud.get_cgmlst_cluster_by_sample_id(self.session, 'SAM001')

        
        self.assertEqual(cgmlst_id,['BC300'])
        #self.assertEqual(result.id, 2)

    def test_delete_sample(self):
        sample_dict = {
            'sample_id': 'SAM001',
            'accession': 'ACC001',
            'collection_date': datetime.date(1970, 1, 1),
        }

        created_sample = crud.create_sample(self.session, sample_dict)
        sample_id = created_sample.sample_id
        deleted_sample_records = crud.delete_sample(self.session, sample_id)

        for deleted_sample_record in deleted_sample_records:
            self.assertEqual(sample_id, deleted_sample_record.sample_id)

        
class SampleCrudMachine(RuleBasedStateMachine):
    def __init__(self):
        super(SampleCrudMachine, self).__init__()
        
        alembic.command.upgrade(alembic_cfg, 'head')
        
        self.engine = create_engine(connection_uri)
        self.session = Session(self.engine)
        models.Base.metadata.create_all(self.engine)

    Samples = Bundle('samples')

    @rule(target=Samples,
          sample_id=st.text(alphabet=(ASCII_ALPHANUMERIC + ASCII_ACCEPTABLE_IDENTIFIER_SYMBOLS)),
          accession=st.text(alphabet=ASCII_ALPHANUMERIC),
          collection_date=st.dates(min_value=datetime.date(1800,1,1), max_value=datetime.date(2200,1,1)))
    def create_sample(self, sample_id, accession, collection_date):
        sample_dict = {
            'sample_id': sample_id,
            'accession': accession,
            'collection_date': collection_date,
        }

        created_sample = crud.create_sample(self.session, sample_dict)
        
        if created_sample:
            json_serializable_sample = utils.row2dict(created_sample)
            date_fields = [
                'collection_date',
                #'valid_until',
                #'created_at',
            ]
            for date_field in date_fields: 
                json_serializable_sample[date_field] = str(json_serializable_sample[date_field])
            log.debug("Created sample: " + json.dumps(json_serializable_sample))
        else:
            log.debug("Sample creation for sample_id: \"" + sample_id + "\" returned None")
            existing_db_sample = crud.get_sample(self.session, sample_id)
            if existing_db_sample is not None:
                log.debug("Sample \"" + sample_id + "\" exists in db.")
            else:
                log.debug("Sample \"" + sample_id + "\" does not exist in db.")
        note(utils.row2dict(created_sample))
        if created_sample is not None:
            all_fields_match_after_insertion = all([
                created_sample.sample_id == sample_id,
                created_sample.accession == accession,
                created_sample.collection_date == collection_date,
            ])
        assert(created_sample == None or all_fields_match_after_insertion)
        return created_sample


    @rule(sample=Samples.filter(lambda x: x is not None))
    def delete_sample(self, sample):
        log.debug("Attempting to delete sample: \"" + sample.sample_id + "\"")
        deleted_samples = crud.delete_sample(self.session, sample.sample_id)
        log.debug("Deleted samples: " + str([sample.sample_id for sample in deleted_samples]))
        for deleted_sample in deleted_samples:
            assert(deleted_sample.sample_id == sample.sample_id)
            assert(deleted_sample.accession == sample.accession)


class CgmlstAlleleProfileCrudMachine(RuleBasedStateMachine):
    def __init__(self):
        super(CgmlstAlleleProfileCrudMachine, self).__init__()
        
        alembic.command.upgrade(alembic_cfg, 'head')
        
        self.engine = create_engine(connection_uri)
        self.session = Session(self.engine)
        models.Base.metadata.create_all(self.engine)

    # TODO: The section below copied from the SampleCrudMachine class.
    #       Find a way to abstract it out(?)
    Samples = Bundle('samples')

    @rule(target=Samples,
          sample_id=st.text(alphabet=(ASCII_ALPHANUMERIC + ASCII_ACCEPTABLE_IDENTIFIER_SYMBOLS)),
          accession=st.text(alphabet=ASCII_ALPHANUMERIC),
          collection_date=st.dates(min_value=datetime.date(1800,1,1), max_value=datetime.date(2200,1,1)))
    def create_sample(self, sample_id, accession, collection_date):
        sample_dict = {
            'sample_id': sample_id,
            'accession': accession,
            'collection_date': collection_date,
        }

        created_sample = crud.create_sample(self.session, sample_dict)
        
        if created_sample:
            json_serializable_sample = utils.row2dict(created_sample)
            date_fields = [
                'collection_date',
                #'valid_until',
                #'created_at',
            ]
            for date_field in date_fields: 
                json_serializable_sample[date_field] = str(json_serializable_sample[date_field])
            log.debug("Created sample: " + json.dumps(json_serializable_sample))
        else:
            json_serializable_sample = created_sample
            log.debug("Sample creation for sample_id: \"" + sample_id + "\" returned None")
            existing_db_sample = crud.get_sample(self.session, sample_id)
            if existing_db_sample is not None:
                log.debug("Sample \"" + sample_id + "\" exists in db.")
            else:
                log.debug("Sample \"" + sample_id + "\" does not exist in db.")
        note(json_serializable_sample)
        if created_sample is not None:
            all_fields_match_after_insertion = all([
                created_sample.sample_id == sample_id,
                created_sample.accession == accession,
                created_sample.collection_date == collection_date,
            ])
        assert(created_sample == None or all_fields_match_after_insertion)
        return created_sample

    # TODO: The section above is copied from the SampleCrudMachine class.
    #       Find a way to abstract it out(?)


    @rule(sample=Samples.filter(lambda x: x is not None),
          profile=st.lists(st.text(ASCII_NUMERIC + ['-'], min_size=1, max_size=1), min_size=4, max_size=4))
    def create_cgmlst_profile(self, sample, profile):
        sample_id = sample.sample_id
        locus_ids = [
            "Rv0001",
            "Rv0002",
            "Rv0003",
            "Rv0004",
        ]
        num_alleles_called = sum([0 if x == '-' else 1 for x in profile])
        percent_called = num_alleles_called / len(profile)
        profile_dict = {
            'sample_id': sample_id,
            'percent_called': percent_called,
            'profile': {}
        }
        for idx, locus_id in enumerate(locus_ids):
            profile_dict['profile'][locus_id] = profile[idx]
        
        created_cgmlst_profile = crud.create_cgmlst_allele_profile(self.session, profile_dict)
        json_serializable_cgmlst_profile = utils.row2dict(created_cgmlst_profile)
        #date_fields = [
        #    'valid_until',
        #    'created_at',
        #]
        #for date_field in date_fields: 
        #    json_serializable_cgmlst_profile[date_field] = str(json_serializable_cgmlst_profile[date_field])
        #log.debug("Created cgMLST Profile for sample \"" + sample_id + "\": " + json.dumps(json_serializable_cgmlst_profile))

        note(json_serializable_cgmlst_profile)
        assert(created_cgmlst_profile.sample_id == sample.id)
          

        
TestSampleCrudMachine = SampleCrudMachine.TestCase
TestCgmlstAlleleProfileCrudMachine = CgmlstAlleleProfileCrudMachine.TestCase
