#!/usr/bin/env python

import argparse
import csv
import json

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

import tb_db.parsers as parsers
import tb_db.crud as crud

from tb_db.models import Sample

def main(args):
    with open(args.config, 'r') as f:
        config = json.load(f)
    connection_uri = config['connection_uri']
    engine = create_engine(connection_uri)
    Session = sessionmaker(bind=engine)
    session = Session()


    parsed_amr = parsers.parse_amr_summary(args.amr_summary)
    #print(parsed_amr)
    created_amr_summary = crud.create_amr_summary(session, parsed_amr)

    for amr in created_amr_summary:
        stmt = select(Sample).where(Sample.id == amr.sample_id)
        sample = session.scalars(stmt).one()
        print("Created amr for: " + sample.sample_id)
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--amr_summary')
    #parser.add_argument('--speciation')
    parser.add_argument('-c', '--config', help="config file (JSON format))")
    args = parser.parse_args()
    main(args)
