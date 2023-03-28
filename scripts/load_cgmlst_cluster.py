#!/usr/bin/env python

import argparse
import csv
import json

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

import tb_db.parsers as parsers
import tb_db.crud as crud

from tb_db.models import Sample
from tb_db.models import Library
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

    sample_run = parsers.parse_run_ids(args.locations)

    created_cgmlst_clusters = crud.add_samples_to_cgmlst_clusters(session, cgmlst_cluster_by_sample,sample_run)

    for sample in created_cgmlst_clusters:
        print("added cluster to sample: " + sample.samples.sample_id)




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('--locations')
    parser.add_argument('-c', '--config', help="config file (JSON format))")
    args = parser.parse_args()
    main(args)
