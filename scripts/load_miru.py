#!/usr/bin/env python

import argparse
import json

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

import tb_db.parsers as parsers
import tb_db.crud as crud

from tb_db.models import Sample
from tb_db.models import CgmlstAlleleProfile
    
def main(args):

    with open(args.config, 'r') as f:
        config = json.load(f)
    connection_uri = config['connection_uri']
    engine = create_engine(connection_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    miru_profiles_by_sample_id = parsers.parse_miru(args.input)
    
    created_profiles = crud.create_miru_profiles(session, miru_profiles_by_sample_id)
    

    for profile in created_profiles:
        stmt = select(Sample).where(Sample.id == profile.sample_id)
        sample = session.scalars(stmt).one()
        print("Created profile for sample: " + sample.sample_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('-c', '--config', help="config file (JSON format))")
    args = parser.parse_args()
    main(args)
