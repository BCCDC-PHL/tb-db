#!/usr/bin/env python

import argparse
import csv
import json

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

import tb_db.parsers as parsers
import tb_db.crud as crud


def main(args):
    with open(args.config, 'r') as f:
        config = json.load(f)
    connection_uri = config['connection_uri']
    engine = create_engine(connection_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    samples = parsers.parse_samples(args.input)
    
    created_samples = crud.create_samples(session, samples)

    for sample in created_samples:
        print("Created sample: " + sample.sample_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('-c', '--config', help="config file (JSON format))")
    args = parser.parse_args()
    main(args)
