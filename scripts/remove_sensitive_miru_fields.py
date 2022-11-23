#!/usr/bin/env python

import argparse
import csv
import json
import sys

def main(args):
    excluded_fields = set([
        'Patient Name',
        'PHN',
        'Date of Birth',
    ])
    output_lines = []
    with open(args.input, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            output = {}
            for k, v in row.items():
                if k.strip() not in excluded_fields:
                    output[k.strip()] = v.strip()
            output_lines.append(output)

    fieldnames = list(output_lines[0].keys())
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, dialect='unix', quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    for line in output_lines:
        writer.writerow(line)
    
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    args = parser.parse_args()
    main(args)
