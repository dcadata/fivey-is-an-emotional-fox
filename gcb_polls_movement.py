import datetime
import re

import pandas as pd

FOLDER = 'gcb_movement/'


def _read_gcb_polls(additional_cols: list = None) -> pd.DataFrame:
    df = pd.read_csv('data/generic_ballot_polls.csv', usecols=[
        'poll_id', 'sponsors', 'display_name', 'fte_grade', 'methodology', 'partisan', 'population',
        'election_date', 'start_date', 'end_date',
        'dem', 'rep',
        *(additional_cols if additional_cols else []),
    ])
    return df


def _filter_gcb_polls(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df.election_date == '11/8/22') & df.start_date.str.endswith(('/21', '/22'))].drop(
        columns='election_date')
    return df


def _normalize_gcb_polls(df: pd.DataFrame) -> pd.DataFrame:
    def _convert_date_str_to_dttm(x) -> datetime.date:
        x = re.sub('/2$', '/202', x, count=1)
        return pd.to_datetime(x).date()

    df.population = df.population.fillna('Not Specified').str.upper()
    df.fte_grade = df.fte_grade.fillna('Unrated')
    df.partisan = df.partisan.fillna('')
    df.sponsors = df.sponsors.fillna('')
    for col in ('start_date', 'end_date'):
        df[col] = df[col].apply(_convert_date_str_to_dttm)
    df = df.rename(columns=dict(
        display_name='pollsterName', fte_grade='fteGrade', poll_id='polls', sponsors='sponsor'))
    return df


def _split_on_date_and_merge_again(
        df: pd.DataFrame, split_date: tuple, first_date: tuple = (2022, 1, 1)) -> pd.DataFrame:
    data = df.copy()
    merge_cols = ['pollsterName', 'fteGrade', 'sponsor', 'population', 'partisan']

    def _filter_on_date_condition(series_condition: pd.Series) -> pd.DataFrame:
        filtered = data[series_condition].groupby(merge_cols, as_index=False).agg(dict(
            dem='mean', rep='mean', polls='count')).round(1)
        filtered['margin'] = (filtered.dem - filtered.rep).round(1)
        return filtered

    data = data[data.start_date.apply(lambda x: x >= datetime.date(*first_date))].copy()
    before = _filter_on_date_condition(data.end_date.apply(lambda x: x < datetime.date(*split_date)))
    after = _filter_on_date_condition(data.start_date.apply(lambda x: x > datetime.date(*split_date)))
    result = before.merge(after, on=merge_cols, suffixes=('Before', 'After'), how='left')

    result.pollsBefore = result.pollsBefore.fillna(0).apply(int)
    result.pollsAfter = result.pollsAfter.fillna(0).apply(int)
    result = result.sort_values('pollsAfter', ascending=False)

    result['demChange'] = (result.demAfter - result.demBefore).round(1)
    result['repChange'] = (result.repAfter - result.repBefore).round(1)
    result['marginChange'] = (result.marginAfter - result.marginBefore).round(1)
    return result


def _split_on_date_and_merge_again_and_save(df: pd.DataFrame, label: str, *args, **kwargs) -> None:
    _split_on_date_and_merge_again(df, *args, **kwargs).to_csv(f'{FOLDER}{label}.csv', index=False)
    return


def create_gcb_polls_movement_trackers(df: pd.DataFrame) -> None:
    df = _normalize_gcb_polls(_filter_gcb_polls(df))
    _split_on_date_and_merge_again_and_save(df, '1-Dobbs (YTD split at 6.24)', (2022, 6, 24))
    _split_on_date_and_merge_again_and_save(df, '2-Student Loan Forgiveness (6.24-8.24 vs 8.24-Today)', first_date=(
        2022, 6, 24), split_date=(2022, 8, 24))
    return


def create_gcb_polls_trimmed() -> None:
    df = _normalize_gcb_polls(_filter_gcb_polls(_read_gcb_polls(['cycle'])))[[
        'pollsterName', 'sponsor', 'fteGrade', 'methodology', 'start_date', 'end_date', 'population', 'partisan',
        'dem', 'rep', 'cycle',
    ]]
    df = df[df.start_date.apply(lambda x: x.year) == 2022].rename(columns=dict(
        start_date='startDate', end_date='endDate'))
    df['endMonth'] = df.endDate.apply(lambda x: x.month)
    for col in ('startDate', 'endDate'):
        df[col] = df[col].apply(lambda x: x.strftime('%m/%d/%Y'))
    df.to_csv(f'{FOLDER}generic_ballot_polls.trimmed.csv', index=False)
    return
