import pandas as pd

from task import _DISTRICT_TOPLINE_FILENAMES


def rejoin_expressions(chamber: str) -> pd.DataFrame:
    chamber = chamber.lower()

    df = pd.read_csv(f'data/{_DISTRICT_TOPLINE_FILENAMES[chamber]}', usecols=[
        'district', 'forecastdate', 'expression', 'winner_Dparty']).rename(columns=dict(winner_Dparty='probD'))
    df.probD = df.probD.round(2)

    _separate_expression = lambda x: df[df.expression == x].drop(columns='expression')
    merged = (
        _separate_expression('_deluxe')
            .merge(_separate_expression('_classic'), on=['district', 'forecastdate'], suffixes=('', '_classic'))
            .merge(_separate_expression('_lite'), on=['district', 'forecastdate'], suffixes=('', '_lite'))
    )
    merged = merged.rename(columns=dict(district='seat', probD='probD_deluxe'))

    merged.forecastdate = merged.forecastdate.apply(lambda x: pd.to_datetime(x).date())
    merged = merged.sort_values('forecastdate')

    if chamber in ('senate', 'governor'):
        merged.seat = merged.seat.apply(lambda x: x[:2])

    return merged
