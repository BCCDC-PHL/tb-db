import datetime
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

connection_uri = "sqlite:///:memory:"

project_root_path = os.path.join(os.path.dirname(__file__), '..', '..')
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


    def test_create_sample(self):
        """
        Test that a sample can be created, and when a single sample is inserted
        into an empty database, it is assigned a primary key id of 1.
        """
        
        sample_dict = {
            'sample_id': 'SAM001',
            'accession': 'ACC001',
            'collection_date': datetime.date(1970, 1, 1),          
        }

        created_sample = crud.create_sample(self.session, sample_dict)

        self.assertIsNotNone(created_sample)
        self.assertEqual(created_sample.id, 1)


    def test_create_miru_profile(self):
        """
        Test that the foreign key to the sample is kept consistent
        after adding a miru profile to a sample.
        """
        sample_dict = {
            'sample_id': 'SAM001',
            'accession': 'ACC001',
            'collection_date': datetime.date(1970, 1, 1),          
        }

        miru_profile_dict = {
            'key': 'SAM001',
            'cluster': 'BC278',
            'acc_num':'ACC001',
            'miru_02':'1',
            'miru_24':'2',
            'miru_26':'3',
            'miru_pattern':'123'
        }

        sample_before_adding_miru_profile = crud.create_sample(self.session, sample_dict)
        miru_profile = crud.create_miru_profile(self.session, sample_dict['sample_id'], miru_profile_dict)
        sample_after_adding_miru_profile = crud.get_sample(self.session, sample_dict['sample_id'])
        
        self.assertEqual(miru_profile.sample_id, sample_after_adding_miru_profile.id)
        

    def test_get_miru_cluster(self):
        """
        Test that a MIRU cluster can be retrieved from the database.
        """
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
        created_miru_profile = crud.create_miru_profile(self.session,'SAM001', miru_dict)
        miru_id = crud.get_miru_cluster_by_sample_id(self.session, 'SAM001')
        self.assertIsNotNone(created_miru_profile)
        self.assertEqual(miru_id,'BC278')


    def test_get_cgmlst_cluster(self):
        """
        Test that a cgMLST cluster can be retrieved from the database.
        """
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
        self.assertEqual(created_sample.id,1)
        
        result = crud.add_sample_to_cgmlst_cluster(self.session,'SAM001', cgmlst_cluster_dict)

        cgmlst_id = crud.get_cgmlst_cluster_by_sample_id(self.session, 'SAM001')

        
        self.assertEqual(cgmlst_id,'BC300')
        self.assertEqual(result.id, 2)


    def test_delete_sample(self):
        """
        Test that a sample can be deleted from the database.
        """
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
