import datetime
import re

import pandas as pd

FOLDER = 'gcb_movement/'


def _read_data() -> pd.DataFrame:
    df = pd.read_csv('data/generic_ballot_polls.csv', usecols=[
        'poll_id', 'sponsors', 'display_name', 'fte_grade', 'methodology', 'partisan', 'population',
        'election_date', 'start_date', 'end_date',
        'dem', 'rep',
    ])
    return df


def _filter_polls(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df.election_date == '11/8/22') & df.start_date.str.endswith(('/21', '/22'))].drop(
        columns='election_date')
    return df


def _normalize_date(x) -> datetime.date:
    x = re.sub('/21$', '/2021', x, count=1)
    x = re.sub('/22$', '/2022', x, count=1)
    return pd.to_datetime(x).date()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.population = df.population.fillna('Not Specified').str.upper()
    df.fte_grade = df.fte_grade.fillna('Unrated')
    df.partisan = df.partisan.fillna('')
    df.sponsors = df.sponsors.fillna('')
    for col in ('start_date', 'end_date'):
        df[col] = df[col].apply(_normalize_date)
    df = df.rename(columns=dict(
        display_name='pollsterName', fte_grade='fteGrade', poll_id='polls', sponsors='sponsor'))
    return df


def _remerge_data(df: pd.DataFrame, split_date: tuple, first_date: tuple = (2022, 1, 1)) -> pd.DataFrame:
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


def _remerge_and_save(df: pd.DataFrame, label: str, *args, **kwargs) -> None:
    _remerge_data(df, *args, **kwargs).to_csv(f'{FOLDER}{label}.csv', index=False)


def create_gcb_polls_movement_trackers(df: pd.DataFrame) -> None:
    df = _normalize_columns(_filter_polls(df))
    _remerge_and_save(df, '1-Dobbs (YTD split at 6.24)', (2022, 6, 24))
    _remerge_and_save(df, '2-MAL Raid (6.24-8.9 vs 8.9-Today)', first_date=(2022, 6, 24), split_date=(2022, 8, 9))
    _remerge_and_save(df, '3-Student Loan Forgiveness (6.24-8.24 vs 8.24-Today)', first_date=(
        2022, 6, 24), split_date=(2022, 8, 24))
    _remerge_and_save(df, '4-\'Soul of the Nation\' Speech (6.24-9.1 vs 9.1-Today)', first_date=(
        2022, 6, 24), split_date=(2022, 9, 1))
    _remerge_and_save(df, '5-\'Commitment to America\' (6.24-9.23 vs 9.23-Today)', first_date=(
        2022, 6, 24), split_date=(2022, 9, 23))
    _remerge_and_save(df, '6-Marijuana Reform Announcement (6.24-10.6 vs 10.6-Today)', first_date=(
        2022, 6, 24), split_date=(2022, 10, 6))


def create_gcb_polls_population_diff_trackers(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_columns(_filter_polls(df))
    df = df[df.start_date >= datetime.date(2022, 1, 1)].copy()

    def _separate_pop(pop) -> pd.DataFrame:
        temp = df[df.population == pop].drop(columns='population')
        temp['margin'] = temp.dem - temp.rep
        return temp

    df = _separate_pop('RV').merge(_separate_pop('LV')[[
        'polls', 'dem', 'rep', 'margin']], on='polls', suffixes=('RV', 'LV'))
    df = df.rename(columns=dict(start_date='startDate', end_date='endDate'))
    df = df[[
        'pollsterName', 'fteGrade', 'sponsor', 'partisan', 'startDate', 'endDate',
        'demRV', 'repRV', 'marginRV',
        'demLV', 'repLV', 'marginLV',
    ]]
    for col in ('startDate', 'endDate'):
        df[col] = df[col].apply(lambda x: x.strftime('%m/%d/%Y'))
    df['marginDiff'] = df.marginLV - df.marginRV

    df.to_csv(f'{FOLDER}Z-Population Difference.csv', index=False)
    return df


def create_gcb_polls_trimmed() -> None:
    df = _normalize_columns(_filter_polls(_read_data()))[[
        'pollsterName', 'sponsor', 'fteGrade', 'methodology', 'start_date', 'end_date', 'population', 'partisan',
        'dem', 'rep',
    ]]
    df = df[df.start_date.apply(lambda x: x.year) == 2022].rename(columns=dict(
        start_date='startDate', end_date='endDate'))
    for col in ('startDate', 'endDate'):
        df[col] = df[col].apply(lambda x: x.strftime('%m/%d/%Y'))
    df.to_csv(f'{FOLDER}generic_ballot_polls.trimmed.csv', index=False)


def main() -> None:
    df = _read_data()
    create_gcb_polls_movement_trackers(df)


if __name__ == '__main__':
    main()
