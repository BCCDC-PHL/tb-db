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

def main(args):
    with open(args.config, 'r') as f:
        config = json.load(f)
    connection_uri = config['connection_uri']
    engine = create_engine(connection_uri)
    Session = sessionmaker(bind=engine)
    session = Session()


    parsed_species = parsers.parse_species(args.input)

   
    sample_run = parsers.parse_run_ids(args.locations)
    created_species = crud.create_species(session, parsed_species,sample_run)

    for created_speci in created_species:
        stmt = select(Library).where(Library.id == created_speci.library_id)
        sample = session.scalars(stmt).one()
        print("Created species for: " + sample.samples.sample_id)





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('--locations')
    parser.add_argument('-c', '--config', help="config file (JSON format))")
    args = parser.parse_args()
    main(args)
