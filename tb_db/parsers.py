import csv
import datetime

### Samples
def parse_samples(samples_path):
    samples = []
    with open(samples_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt = datetime.datetime.strptime(row['collection_date'], "%Y-%m-%d")
            d = datetime.date(dt.year,dt.month,dt.day)
            sample = {
                'sample_id': row['sample_id'],
                'collection_date': d,
            }
            samples.append(sample)

    return samples
    
# cgmlst cluster
def parse_cgmlst_cluster(samples_path):
    samples = []
    with open(samples_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:

            sample = {
                'sample_id': row['sample_id'],
                'cluster':row['clusters_cgmlst'],
            }
            samples.append(sample)

    return samples


### MIRU
def _miru_convert_quarterly_format(quarter_input: str):
    """
    Convert a string like `2009 4th QTR` to `2009-Q4`
    """
    year = quarter_input.split(' ')[0]
    quarter = 'Q' + quarter_input.split(' ')[1][0]
    quarter = '-'.join([year, quarter])

    return quarter


def _miru_convert_date(date_input: str) -> str:
    """
    :param date_input:
    :type date_input: str
    :return:
    :rtype: str
    """
    year, month, date = date_input.split('-')
    month = month[0:3]
    date_input = '-'.join([year, month, date])
    date_output = datetime.datetime.strptime(date_input, '%Y-%b-%d').strftime('%Y-%m-%d')
    return date_output


def _miru_clean_fieldname(fieldname: str) -> str:
    """
    :param fieldname:
    :type fieldname: str
    :return:
    :rtype: str
    """
    clean_fieldname = fieldname.strip()
    clean_fieldname = clean_fieldname.lower()
    clean_fieldname = clean_fieldname.replace(' ', '_')
    clean_fieldname = clean_fieldname.replace('#', '_num')

    return clean_fieldname

    
def parse_miru(miru_path: str) -> dict[str, object]:
    """
    Parse a MIRU csv file.

    :param miru_path: Path to MIRU csv file. 
    :type miru_path: str
    :return: Dict of MIRU profiles, indexed by Sample ID
    :rtype: dict[str, object]
    """
    # doi:10.1371/journal.pone.0149435.t001
    # Table 1
    miru_alias_by_position = {
        154: 'MIRU2',
        424: None,
        577: 'ETR-C',
        580: 'MIRU4',
        802: 'MIRU40',
        960: 'MIRU10',
        1644: 'MIRU16',
        1955: None,
        2059: 'MIRU20',
        2163: None,
        2165: 'ETR-A',
        2347: None,
        2401: None,
        2461: 'ETR-B',
        2531: 'MIRU23',
        2687: 'MIRU24',
        2996: 'MIRU26',
        3007: 'MIRU27',
        3171: None,
        3192: 'MIRU31',
        3690: None,
        4052: None,
        4156: None,
        4348: 'MIRU39',
    }

    fieldname_translation = {
        'key': 'sample_id',
        'acc_num': 'accession',
        'miru_02': 'vntr_locus_position_154',
        'miru_04': 'vntr_locus_position_580',
        'miru_10': 'vntr_locus_position_960',
        'miru_16': 'vntr_locus_position_1644',
        'miru_20': 'vntr_locus_position_2059',
        'miru_23': 'vntr_locus_position_2531',
        'miru_24': 'vntr_locus_position_2687',
        'miru_26': 'vntr_locus_position_2996',
        'miru_27': 'vntr_locus_position_3007',
        'miru_31': 'vntr_locus_position_3192',
        'miru_39': 'vntr_locus_position_4348',
        'miru_40': 'vntr_locus_position_802',
        '424': 'vntr_locus_position_424',
        '577': 'vntr_locus_position_577',
        "1955": 'vntr_locus_position_1955',
        "2163": 'vntr_locus_position_2163',
        "2165": 'vntr_locus_position_2165',
        "2347": 'vntr_locus_position_2347',
        "2401": 'vntr_locus_position_2401',
        "2461": 'vntr_locus_position_2461',
        "3171": 'vntr_locus_position_3171',
        "3690": 'vntr_locus_position_3690',
        "4052": 'vntr_locus_position_4052',
        "4156": 'vntr_locus_position_4156',
    }
    miru_by_sample_id = {}
    with open(miru_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            miru = {}
            for k, v in row.items():
                cleaned_key = _miru_clean_fieldname(k)
                if cleaned_key in fieldname_translation:
                    cleaned_key = fieldname_translation[cleaned_key]
                elif cleaned_key == "year_tested":
                    cleaned_key = "quarter_tested"
                    v = _miru_convert_quarterly_format(v)
                    try:
                        year_tested = v.split('-')[0]
                    except IndexError as e:
                        year_tested = None
                    miru['year_tested'] = year_tested
                elif cleaned_key == 'collection_date':
                    v = _miru_convert_date(v)

                v = v.strip()
                if v == "":
                    v = None
                miru[cleaned_key] = v
            miru_by_sample_id[miru['sample_id']] = miru

    return miru_by_sample_id


### cgMLST
def parse_cgmlst(cgmlst_path: str, uncalled='-'):
    """
    Parse a cgMLST csv file.

    :param cgmlst_path: Path to cgMLST csv file. 
    :type cgmlst_path: str
    :return: Dictionary of cgMLST profiles, indexed by Sample ID.
    :rtype: dict[str, object]
    """
    cgmlst_by_sample_id = {}
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

            cgmlst_by_sample_id[sample_id] = {
                'sample_id': sample_id,
                'profile': profile,
                'percent_called': percent_called
            }

    return cgmlst_by_sample_id
