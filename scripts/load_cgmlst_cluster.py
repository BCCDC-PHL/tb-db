#!/usr/bin/env python

import argparse
import csv
import json

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

import tb_db.parsers as parsers
import tb_db.crud as crud

from tb_db.models import Sample
from tb_db.models import CgmlstAlleleProfile
from tb_db.models import MiruProfile


def main(args):
    with open(args.config, 'r') as f:
        config = json.load(f)
    connection_uri = config['connection_uri']
    engine = create_engine(connection_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    cgmlst_cluster_by_sample = parsers.parse_cgmlst_cluster(args.input)
    # print(json.dumps(cgmlst_by_sample_id, indent=2))
    # exit()
    #cgmlst_profiles = list(cgmlst_by_sample_id.values())
    created_cgmlst_clusters = crud.add_samples_to_cgmlst_clusters(session, cgmlst_cluster_by_sample)

    for sample in created_cgmlst_clusters:
        print("added cluster to sample: " + sample.sample_id)
        #print("Updating Parent links..")
        #crud.update_link_foreign_keys(session,sample.sample_id, CgmlstAlleleProfile,Sample)
        #crud.update_link_foreign_keys(session,sample.sample_id, MiruProfile,Sample)




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('-c', '--config', help="config file (JSON format))")
    args = parser.parse_args()
    main(args)
