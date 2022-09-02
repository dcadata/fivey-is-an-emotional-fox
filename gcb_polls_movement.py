import os
import re
from datetime import date

import pandas as pd


def _filter_polls(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df.election_date == '11/8/22') & df.start_date.str.endswith(('/21', '/22'))].copy()
    return df


def _normalize_date(x) -> date:
    x = re.sub('/21$', '/2021', x, count=1)
    x = re.sub('/22$', '/2022', x, count=1)
    return pd.to_datetime(x).date()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.population = df.population.fillna('Not Specified').str.upper()
    df.fte_grade = df.fte_grade.fillna('Unrated')
    df.partisan = df.partisan.fillna('')
    for col in ('sponsor_ids', 'sponsors'):
        df[col] = df[col].fillna('')
    for col in ('start_date', 'end_date'):
        df[col] = df[col].apply(_normalize_date)
    df = df.rename(columns=dict(display_name='pollsterName', fte_grade='fteRating', poll_id='polls'))
    return df


def _remerge_data(df: pd.DataFrame, split_date: tuple, first_date: tuple = (2022, 1, 1)) -> pd.DataFrame:
    data = df.copy()
    merge_cols = ['pollsterName', 'fteRating', 'sponsors', 'population', 'partisan']

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


def _remerge_and_save(df: pd.DataFrame, label: str, *args, **kwargs) -> None:
    _remerge_data(df, *args, **kwargs).to_csv(f'gcb_polls_movement/{label}.csv', index=False)


def create_gcb_polls_movement_trackers(df: pd.DataFrame) -> None:
    df = _normalize_columns(_filter_polls(df))
    _remerge_and_save(df, 'Dobbs Leak (YTD split at 5.10)', (2022, 5, 10))
    _remerge_and_save(df, 'Dobbs (YTD split at 6.24)', (2022, 6, 24))
    _remerge_and_save(df, 'MAL Raid (YTD split at 8.9)', (2022, 8, 9))
    _remerge_and_save(df, 'MAL Raid (Split 6.24-8.9 vs 8.9-Today)', first_date=(2022, 6, 24), split_date=(2022, 8, 9))
    _remerge_and_save(df, 'Student Loan Forgiveness (Split 6.24-8.24 vs 8.24-Today)', first_date=(
        2022, 6, 24), split_date=(2022, 8, 24))


def create_filenames_list():
    filenames = [fn for fn in os.listdir('gcb_polls_movement') if fn.endswith('.csv')]
    filenames.insert(0, 'SELECT')
    with open('gcb_polls_movement/filenames.txt', 'w') as f:
        f.write('\n'.join(filenames))


if __name__ == '__main__':
    create_filenames_list()
