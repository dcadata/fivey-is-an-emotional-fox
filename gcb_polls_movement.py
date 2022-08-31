import re
from datetime import date

import pandas as pd

from task import _FTE_POLLS_BASE_URL, _GCB_FILENAMES


def _read_polls() -> pd.DataFrame:
    df = pd.read_csv(_FTE_POLLS_BASE_URL + _GCB_FILENAMES['polls'])
    return df


def _filter_polls(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df.election_date == '11/8/22') & df.start_date.str.endswith('/22')].copy()
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.population = df.population.fillna('Not Specified')
    df.fte_grade = df.fte_grade.fillna('Unrated')
    df.partisan = df.partisan.fillna('')
    for col in ('sponsor_ids', 'sponsors'):
        df[col] = df[col].fillna('')
    for col in ('start_date', 'end_date'):
        df[col] = df[col].apply(lambda x: re.sub('/22$', '/2022', x, count=1)).apply(lambda x: pd.to_datetime(x).date())
    df = df.rename(columns=dict(display_name='pollsterName', fte_grade='pollsterRating', poll_id='polls'))
    return df


def _remerge_data(df: pd.DataFrame, split_date: tuple, first_date: tuple = (2022, 1, 1)) -> pd.DataFrame:
    data = df.copy()
    merge_cols = ['pollsterName', 'pollsterRating', 'sponsors', 'population', 'partisan']

    def _filter_on_date_condition(series_condition: pd.Series) -> pd.DataFrame:
        filtered = data[series_condition].groupby(merge_cols, as_index=False).agg(dict(
            dem='mean', rep='mean', polls='count')).round(1)
        filtered['margin'] = (filtered.dem - filtered.rep).round(1)
        return filtered

    data = data[data.start_date.apply(lambda x: x >= date(*first_date))].copy()
    pre = _filter_on_date_condition(data.end_date.apply(lambda x: x < date(*split_date)))
    post = _filter_on_date_condition(data.start_date.apply(lambda x: x > date(*split_date)))
    result = pre.merge(post, on=merge_cols, suffixes=('Pre', 'Post'))

    result['demChange'] = (result.demPost - result.demPre).round(1)
    result['repChange'] = (result.repPost - result.repPre).round(1)
    result['marginChange'] = (result.marginPost - result.marginPre).round(1)
    return result


def get_data() -> pd.DataFrame:
    return _normalize_columns(_filter_polls(_read_polls()))


def remerge_and_save(df: pd.DataFrame, label: str, *args) -> None:
    _remerge_data(df, *args).to_csv(f'gcb_polls_movement/{label}.csv', index=False)


def main() -> None:
    df = get_data()
    remerge_and_save(df, 'Dobbs (YTD split at 6.24)', (2022, 6, 24))
    remerge_and_save(df, 'MAL Raid (YTD split at 8.9)', (2022, 8, 9))
    remerge_and_save(df, 'MAL Raid (Split 6.24-8.9 vs 8.9-Today)', (2022, 8, 9), (2022, 6, 24))


if __name__ == '__main__':
    main()
