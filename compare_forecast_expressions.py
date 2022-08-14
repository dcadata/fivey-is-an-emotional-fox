from time import sleep

import pandas as pd

from task import _DISTRICT_TOPLINE_FILENAMES


def compare_forecast_expressions(chamber: str) -> None:
    chamber = chamber.lower()
    url = f'https://projects.fivethirtyeight.com/2022-general-election-forecast-data/{_DISTRICT_TOPLINE_FILENAMES[chamber]}'

    df = pd.read_csv(url, usecols=['district', 'expression', 'winner_Dparty'])
    df = df.drop_duplicates(subset=['district', 'expression'], keep='first')
    df = df.rename(columns=dict(winner_Dparty='probD'))
    df.probD = df.probD.round(2)

    func = lambda x: df[df.expression == x].drop(columns='expression')
    merged = func('_deluxe').merge(func('_classic'), on='district', suffixes=('', '_classic')).merge(
        func('_lite'), on='district', suffixes=('', '_lite'))
    merged = merged.rename(columns=dict(district='seat', probD='probD_deluxe'))

    if chamber in ('senate', 'governor'):
        merged.seat = merged.seat.apply(lambda x: x[:2])

    merged['probD_deluxe_minus_lite'] = merged.probD_deluxe - merged.probD_lite
    merged['probD_deluxe_minus_lite_abs'] = merged.probD_deluxe_minus_lite.apply(abs).round(2)
    merged.probD_deluxe_minus_lite = merged.probD_deluxe_minus_lite.round(2)
    merged = merged.sort_values('probD_deluxe_minus_lite_abs', ascending=False)
    merged.to_csv(f'forecast_expression_comparisons/{chamber}.csv', index=False)


def main():
    for chamber in _DISTRICT_TOPLINE_FILENAMES.keys():
        compare_forecast_expressions(chamber)
        sleep(2)


if __name__ == '__main__':
    main()
