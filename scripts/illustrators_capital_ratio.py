import pandas as pd
import argparse
import sys


def preprocess(in_put, status):
    collab_group = in_put[[status, 'volume', 'years']]
    collab_group['status'] = status
    collab_group.rename(columns={status: 'name'}, inplace=True)
    return collab_group


def prepare_data(collab, biblio):
    authors = preprocess(collab, 'author')
    illustrators = preprocess(collab, 'illustrator')
    works = pd.concat([authors, illustrators], axis=0, ignore_index=True)
    works = works[['name', 'status', 'volume', 'years']]
    works = pd.merge(left=works, right=biblio, how='inner', left_on='volume', right_on='num')
    return works


def calculate_sum(works, pattern):
    aggregate = works.groupby(['name', 'status'])['city'].apply(lambda x: x[x.str.contains(pattern)].count())
    aggregate = aggregate.to_frame()
    aggregate.reset_index(inplace=True)
    return aggregate


def calculate_capital_ratio(works, out_put_directory, years):
    aggregate_reg = calculate_sum(works, 'М.|Москва')
    aggregate_all_cities = calculate_sum(works, '.*')
    capital_ratios = pd.concat([aggregate_reg, aggregate_all_cities], axis=1, ignore_index=True)
    capital_ratios = capital_ratios.drop(labels=[3, 4], axis=1)
    capital_ratios = capital_ratios.rename(columns={0: 'name', 1: 'status', 2: 'publishings_mos',
                                                    5: 'publishings_tot'})
    capital_ratios['capital_ratio'] = capital_ratios['publishings_mos'] / capital_ratios['publishings_tot']
    capital_ratios['period'] = years
    capital_ratios = capital_ratios[['name', 'status', 'period', 'publishings_mos', 'publishings_tot', 'capital_ratio']]
    capital_ratios.to_csv(path_or_buf=out_put_directory)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Calculate capitol ratios for authors and illustrators listed in bibliographic reference book(s)',
                                     epilog="""Data is preprocessed and capital ratio is calculated for
                                     every author and illustrator in data""")
    parser.add_argument('in_collab', nargs='?', help='Input file (csv)',
                        type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('in_biblio', nargs='?', help='Input file (csv)',
                        type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('-y', '--years',
                        help='years of publishing covered in bibliographic reference book')
    parser.add_argument('--out_dir', nargs='?', help='Output file (png)',
                        action='store')
    return parser.parse_args()


def main():
    args = parse_arguments()
    collab = pd.read_csv(args.in_collab, encoding='utf-8')
    biblio = pd.read_csv(args.in_biblio, encoding='utf-8')
    works = prepare_data(collab, biblio)
    calculate_capital_ratio(works, args.out_dir, args.years)


if __name__ == '__main__':
    main()
