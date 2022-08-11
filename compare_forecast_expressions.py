from time import sleep

import pandas as pd

from task import _TOPLINE_FILENAMES


def compare_forecast_expressions(chamber: str) -> None:
    chamber = chamber.lower()
    url = f'https://projects.fivethirtyeight.com/2022-general-election-forecast-data/{_TOPLINE_FILENAMES[chamber]}'

    df = pd.read_csv(url, usecols=['district', 'expression', 'winner_Dparty'])
    df = df.drop_duplicates(subset=['district', 'expression'], keep='first')
    df = df.rename(columns=dict(winner_Dparty='probD'))
    df.probD = df.probD.round(2)

    func = lambda x: df[df.expression == x].drop(columns='expression')
    merged = func('_lite').merge(func('_classic'), on='district', suffixes=('', '_classic')).merge(
        func('_deluxe'), on='district', suffixes=('', '_deluxe'))
    merged = merged.rename(columns=dict(district='seat', probD='probD_lite'))

    if chamber in ('senate', 'governor'):
        merged.seat = merged.seat.apply(lambda x: x[:2])

    merged['probD_lite_minus_deluxe'] = (merged.probD_deluxe - merged.probD_lite).round(2)
    merged = merged.sort_values('probD_lite_minus_deluxe')
    merged.to_csv(f'forecast_expression_comparisons/{chamber}.csv', index=False)


def main():
    for chamber in _TOPLINE_FILENAMES.keys():
        compare_forecast_expressions(chamber)
        sleep(2)


if __name__ == '__main__':
    main()
