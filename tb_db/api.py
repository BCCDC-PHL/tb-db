
import argparse
import csv
import json

from datetime import date, datetime

from sqlalchemy.orm import Session
from sqlalchemy import create_engine, select

from .model import *

def parse_cgmlst(cgmlst_path, uncalled='-'):
    cgmlst = []
    with open(cgmlst_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sample_id = row.pop('sample_id')
            profile = row
            num_total_loci = len(row)
            num_uncalled_loci = 0
            for k, v in profile.items():
                if v == uncalled:
                    num_uncalled_loci += 1
            if num_total_loci > 0:
                percent_called = (1 - (float(num_uncalled_loci) / num_total_loci)) * 100
            else:
                percent_called = None
            cgmlst.append({
                'sample_id': sample_id,
                'profile': profile,
                'percent_called': percent_called
            })

    return cgmlst

def store_cgmlst_allele_profiles(session, cgmlst_allele_profiles):
    existing_samples = session.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    cgmlst_profiles_to_store = []
    for c in cgmlst_allele_profiles:
        if c['sample_id'] not in existing_sample_ids:
            sample = Sample(sample_id = c['sample_id'])
            session.add(sample)
            session.commit()
        stmt = select(Sample).where(Sample.sample_id == c['sample_id'])
        sample = session.scalars(stmt).one()
        cgmlst_allele_profile = CgMlstAlleleProfile(
            sample_id = sample.id,
            profile = json.dumps(c['profile']),
            percent_called = c['percent_called'],
        )
        cgmlst_profiles_to_store.append(cgmlst_allele_profile)

    session.add_all(cgmlst_profiles_to_store)
    session.commit()
        

def parse_samples(samples_path):
    samples = []
    with open(samples_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt = datetime.strptime(row['collection_date'], "%Y-%m-%d")
            d = date(dt.year,dt.month,dt.day)
            sample = {
                'sample_id': row['sample_id'],
                'collection_date': d,
            }
            samples.append(sample)

    return samples


def store_samples(session, samples):
    existing_samples = session.query(Sample).all()
    existing_sample_ids = set([sample.sample_id for sample in existing_samples])

    samples_to_store = []
    for s in samples:
        if s['sample_id'] not in existing_sample_ids:
            sample = Sample(
                sample_id = s['sample_id'],
                collection_date = s['collection_date'],
            )
            samples_to_store.append(sample)

    session.add_all(samples_to_store)
    session.commit()
