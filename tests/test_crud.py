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

class TestCrudSample(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format = '%(asctime)s %(module)s %(levelname)s: %(message)s',
                            datefmt = '%m/%d/%Y %I:%M:%S %p', level = logging.INFO)

        connection_uri = "sqlite:///:memory:"

        project_root_path = os.path.join(os.path.dirname(__file__), '..')
        alembic_dir_path = os.path.join(project_root_path, 'alembic')
        alembic_config_file_path = os.path.join(project_root_path, "alembic.ini")
        alembic_cfg = alembic.config.Config(file_=alembic_config_file_path)
        alembic_cfg.set_main_option('script_location', alembic_dir_path)
        alembic_cfg.set_main_option('sqlalchemy.url', connection_uri)
        alembic.command.upgrade(alembic_cfg, 'head')
        
        self.engine = create_engine(connection_uri)
        self.session = Session(self.engine)
        models.Base.metadata.create_all(self.engine)


    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)


    def test_create_sample(self):
        
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
        self.assertEqual(miru_id,'BC278')

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

        result = crud.add_sample_to_cgmlst_cluster(self.session,'SAM001', cgmlst_cluster_dict)

        cgmlst_id = crud.get_cgmlst_cluster_by_sample_id(self.session, 'SAM001')
        self.assertEqual(cgmlst_id,'BC300')
        self.assertEqual(result.id, 2)

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

        
