#!/usr/bin/env python

import argparse
import json

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

import tb_db.api as api
            
    
def main(args):

    with open(args.config, 'r') as f:
        config = json.load(f)
    connection_uri = config['connection_uri']
    engine = create_engine(connection_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    miru_profiles_by_sample_id = api.parse_miru(args.input)
    # print(json.dumps(miru_profiles_by_sample_id, indent=2))
    api.store_miru_profiles(session, miru_profiles_by_sample_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('-c', '--config', help="config file (JSON format))")
    args = parser.parse_args()
    main(args)
