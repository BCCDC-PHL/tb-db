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
from tb_db.models import MiruProfile
from tb_db.models import CgmlstScheme


def main(args):
    with open(args.config, 'r') as f:
        config = json.load(f)
    connection_uri = config['connection_uri']
    engine = create_engine(connection_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    cgmlst_by_sample_id = parsers.parse_cgmlst(args.input)
    cgmlst_profiles = list(cgmlst_by_sample_id.values())
    cgmlst_scheme = {'name':'Ridom cgMLST.org','version':'2.1','num_loci':2891} 

    sample_run = parsers.parse_run_ids(args.locations)

    print(sample_run)



    created_profiles = crud.create_cgmlst_allele_profiles(session, cgmlst_scheme, cgmlst_profiles, sample_run)

    for profile in created_profiles:
        stmt = select(Library).where(Library.id == profile.library_id)
        library = session.scalars(stmt).one()
        print("Created profile for sample: " + library.samples.sample_id)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('--locations')
    parser.add_argument('-c', '--config', help="config file (JSON format))")
    args = parser.parse_args()
    main(args)
