import pandas as pd

from task import _NATIONAL_TOPLINE_FILENAMES, _DISTRICT_TOPLINE_FILENAMES, _FTE_FORECAST_BASE_URL


def rejoin_expressions(chamber: str, district: bool = True) -> pd.DataFrame:
    chamber = chamber.lower()
    data_filename = _DISTRICT_TOPLINE_FILENAMES[chamber] if district else _NATIONAL_TOPLINE_FILENAMES[chamber]
    data_filepath = _FTE_FORECAST_BASE_URL + data_filename

    if district:
        usecols = ['expression', 'forecastdate', 'district', 'winner_Dparty']
        rename_mapper = dict(winner_Dparty='probD')
        merge_cols = ['district', 'forecastdate']
    else:
        usecols = ['expression', 'forecastdate', 'chamber_Dparty']
        rename_mapper = dict(chamber_Dparty='probD')
        merge_cols = ['forecastdate']

    df = pd.read_csv(data_filepath, usecols=usecols).rename(columns=rename_mapper)
    df.probD = df.probD.round(2)

    _separate_expression = lambda x: df[df.expression == x].drop(columns='expression')
    merged = (
        _separate_expression('_deluxe')
            .merge(_separate_expression('_classic'), on=merge_cols, suffixes=('', '_classic'))
            .merge(_separate_expression('_lite'), on=merge_cols, suffixes=('_deluxe', '_lite'))
    )

    merged.forecastdate = merged.forecastdate.apply(lambda x: pd.to_datetime(x).date())
    merged = merged.sort_values('forecastdate')

    return merged
