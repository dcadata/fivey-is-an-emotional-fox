import configparser
import json
import re
from email.mime.text import MIMEText
from os import environ
from smtplib import SMTP_SSL
from time import sleep

import pandas as pd
import requests
from bs4 import BeautifulSoup
from twilio.rest import Client

_CONFIG = configparser.ConfigParser()
_CONFIG.read('config.ini')


def _send_email(subject: str, body: str) -> None:
    sender = environ['EMAIL_SENDER']
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = environ['EMAIL_RECIPIENT']

    server = SMTP_SSL(host='smtp.gmail.com', port=465)
    server.login(sender, environ['EMAIL_PASSWORD'])
    server.send_message(msg)
    server.quit()


def _send_text(body: str) -> None:
    client = Client(environ['ACCT_SID'], environ['TOKEN'])
    client.messages.create(to=environ['PHONE_NUMBER'], body=body, messaging_service_sid=environ['SERV_SID'])


def _read_latest() -> dict:
    data_from_file = json.load(open('data/latest.json'))
    return data_from_file


def _update_latest(new_data: dict) -> None:
    data_from_file = _read_latest()
    data_from_file.update(new_data)
    json.dump(data_from_file, open('data/latest.json', 'w'), indent=2)


def _get_gcb(session: requests.Session) -> str:
    if not _CONFIG['gcb'].getboolean('notify'):
        return ''

    data_filepath = 'data/generic_ballot_averages.csv'
    url = 'https://projects.fivethirtyeight.com/polls/data/generic_ballot_averages.csv'
    open(data_filepath, 'wb').write(session.get(url).content)

    data = pd.read_csv(data_filepath, usecols=['candidate', 'pct_estimate', 'election'])
    data = data[data.election == '2022-11-08'].drop(columns=['election']).iloc[-2:]
    data['party'] = data.candidate.apply(lambda x: x[0])

    unrounded_estimates = data.groupby('party').pct_estimate.sum()
    unrounded_lead = unrounded_estimates['D'] - unrounded_estimates['R']

    change_from_previous = unrounded_lead - _read_latest().get('gcb', 0)
    if abs(change_from_previous) < _CONFIG['gcb'].getfloat('threshold'):
        return ''
    _update_latest(dict(gcb=unrounded_lead))

    data.pct_estimate = data.pct_estimate.round(2)
    return 'GCB\nD:{D} R:{R}\n{leader}+{lead} (chg: {change_gainer}+{change})'.format(
        lead=abs(round(unrounded_lead, 2)),
        leader='D' if unrounded_lead > 0 else 'R',
        change=abs(round(change_from_previous, 2)),
        change_gainer='D' if change_from_previous > 0 else 'R',
        **data.groupby('party').pct_estimate.sum(),
    )


def _get_chamber_forecast(session: requests.Session, chamber: str) -> str:
    if not _CONFIG['forecasts_national'].getboolean(chamber):
        return ''

    data_filepath = f'data/{chamber}_national_toplines_2022.csv'
    url = f'https://projects.fivethirtyeight.com/2022-general-election-forecast-data/{chamber}_national_toplines_2022.csv'
    open(data_filepath, 'wb').write(session.get(url).content)

    expression_choice = _CONFIG['forecasts_national'].get('expression', '_deluxe')
    data = pd.read_csv(data_filepath, usecols=[
        'expression', 'chamber_Dparty', 'chamber_Rparty', 'median_seats_Dparty', 'median_seats_Rparty'])
    data = data[data.expression == expression_choice].iloc[0]
    current = dict(
        probD=int(data.chamber_Dparty.round(2) * 100),
        probR=int(data.chamber_Rparty.round(2) * 100),
        seatsD=int(data.median_seats_Dparty),
        seatsR=int(data.median_seats_Rparty),
        expression=data.expression[1:],
    )
    try:
        latest = _read_latest()[chamber]
        if current == latest:
            return ''
        threshold = _CONFIG['forecasts_national'].getfloat('threshold')
        if threshold and threshold > abs(current['probD'] - latest['probD']):
            return ''
    except KeyError:
        pass
    _update_latest({chamber: current})
    return '{chamber} ({expression})\nControl: D:{probD}% R:{probR}%\nSeats: D:{seatsD} R:{seatsR}'.format(
        chamber=chamber.upper(), **current)


def _get_one_seat_status(data: pd.DataFrame, chamber: str, seat: str) -> str:
    seat_data = data[data.district.str.startswith(seat)].iloc[0]
    margin = seat_data.mean_netpartymargin.round(1)
    current = dict(
        nameD=seat_data.name_D1.rsplit(None, 1)[1],
        nameR=seat_data.name_R1.rsplit(None, 1)[1],
        probD=int(seat_data.winner_Dparty.round(2) * 100),
        probR=int(seat_data.winner_Rparty.round(2) * 100),
        margin=abs(margin),
        margin_leader='D' if margin > 0 else 'R',
    )
    try:
        latest = _read_latest()[f'{chamber}_{seat}']
        if current['probD'] == latest['probD']:
            return ''
        threshold = _CONFIG['forecasts_seats'].getfloat('threshold')
        if threshold and threshold > abs(current['probD'] - latest['probD']):
            return ''
    except KeyError:
        pass
    _update_latest({f'{chamber}_{seat}': current})
    return '{seat}: {nameD}(D):{probD}% {nameR}(R):{probR}% ({margin_leader}+{margin})'.format(
        **current, seat=seat.upper())


def _get_seat_forecasts(session: requests.Session, chamber: str) -> str:
    seats = _CONFIG['forecasts_seats'].get(chamber)
    if not seats:
        return ''

    data_filename = dict(
        senate='senate_state_toplines_2022.csv',
        house='house_district_toplines_2022.csv',
        governor='governor_state_toplines_2022.csv',
    )[chamber]
    data_filepath = f'data/{data_filename}'
    url = f'https://projects.fivethirtyeight.com/2022-general-election-forecast-data/{data_filename}'
    open(data_filepath, 'wb').write(session.get(url).content)

    expression_choice = _CONFIG['forecasts_seats'].get('expression', '_deluxe')
    data = pd.read_csv(data_filepath, usecols=[
        'district', 'expression', 'name_D1', 'name_R1', 'winner_Dparty', 'winner_Rparty', 'mean_netpartymargin'])
    data = data[data.expression == expression_choice].drop_duplicates(subset=['district'], keep='first')

    current = list(filter(None, [_get_one_seat_status(data, chamber, seat) for seat in seats.upper().split()]))
    if not current:
        return ''
    current.insert(0, '{chamber} DETAILS ({expression_choice})'.format(
        chamber=chamber.upper(), expression_choice=expression_choice[1:]))
    return '\n'.join(current)


def _get_polls_from_twitter() -> str:
    twitter_username = _CONFIG['twitter_polls'].get('username')
    if not twitter_username:
        return ''

    response = requests.get('https://nitter.net/{}/rss'.format(twitter_username))
    feed = BeautifulSoup(response.text, 'xml')
    tweets = feed.select('item')
    if not tweets:
        return ''

    previous_latest_link = _read_latest().get('twitter_polls')
    polls = []
    for tweet in tweets:
        if tweet.find('link').text == previous_latest_link:
            break
        title, pubdate = map(lambda x: tweet.find(x).text.strip(), ('title', 'pubDate'))
        if re.search(_CONFIG['twitter_polls']['pattern'], title):
            polls.append(dict(title=title, pubdate=pubdate))

    _update_latest(dict(twitter_polls=tweets[0].find('link').text))
    return '\n\n--\n\n'.join('{title}\n\nPubDate: {pubdate}'.format(**poll) for poll in polls)


def _get_matching_gcb_polls_for_one_row(full_data: pd.DataFrame, unseen_row: pd.Series) -> str:
    data = full_data.copy()
    for match_col in ('pollster_id', 'sponsor_ids', 'methodology', 'population', 'internal', 'partisan'):
        data = data[data[match_col] == unseen_row[match_col]].copy()

    data.population = data.population.apply(lambda x: x.upper())
    data['margin'] = (data.dem - data.rep).round(1)
    data['leader_margin'] = data.margin.apply(lambda x: f'{"D" if x > 0 else "R"}+{abs(x)}')
    try:
        data = data.iloc[:2].assign(order=('Recent', 'Previous'))
    except ValueError:  # if len(data) < 2, length of assigned won't match
        return ''
    records = data.to_dict('records')
    change = data.margin.iloc[1] - data.margin.iloc[0]

    first_record = records[0]
    first_line = 'Pollster: {display_name} | Grade: {fte_grade} | Method: {methodology}'
    second_line = ['Sponsor(s): {sponsors}']
    if first_record['partisan']:
        second_line.append('Partisan: {partisan}')
    if first_record['internal']:
        second_line.append('Internal: {internal}')

    lines = [
        '{order}: {start_date}-{end_date} ({sample_size} {population}): D:{dem} R:{rep} => {leader_margin} | [details]({url})'.format(
            **record) for record in records]
    lines.insert(0, first_line.format(**first_record))
    lines.insert(1, ' | '.join(second_line).format(**first_record))
    lines.append('Change: {gainer}+{change}'.format(change=abs(change), gainer='R' if change > 0 else 'D'))
    return '\n'.join(lines)


def _get_matching_gcb_polls(session: requests.Session) -> str:
    if not _CONFIG['gcb'].getboolean('notify_with_matching_polls'):
        return ''

    data_filepath = 'data/generic_ballot_polls.csv'
    url = 'https://projects.fivethirtyeight.com/polls/data/generic_ballot_polls.csv'

    existing_content = open(data_filepath, 'rb').read()
    new_content = session.get(url).content
    if existing_content == new_content:
        return ''
    seen_poll_ids = pd.read_csv(data_filepath).poll_id.unique()
    open(data_filepath, 'wb').write(new_content)
    full_data = pd.read_csv(data_filepath)

    full_data = full_data.dropna(subset=['pollster_id', 'display_name'])
    for col in ('methodology', 'population'):
        full_data[col] = full_data[col].fillna('Not Specified')
    for col in ('sponsor_ids', 'sponsors'):
        full_data[col] = full_data[col].fillna('No Sponsor')
    full_data.internal = full_data.internal.fillna(False)
    full_data.partisan = full_data.partisan.fillna(False)
    full_data.fte_grade = full_data.fte_grade.fillna('Unrated')
    full_data.sample_size = full_data.sample_size.fillna(0).apply(int)

    unseen_data = full_data[~full_data.poll_id.isin(seen_poll_ids)].copy()
    if not len(unseen_data):
        return ''

    lines = [_get_matching_gcb_polls_for_one_row(full_data, unseen_row) for _, unseen_row in unseen_data.iterrows()]
    match_col_names = (
        'Pollster', 'Sponsor(s)', 'Methodology (Online, IVR, etc.)', 'Population (LV, RV, A)', 'Partisan/Internal',
    )
    lines.append('Matched poll must match on {0}'.format(', '.join(match_col_names)))
    return '\n\n'.join(filter(None, lines))


def _get_fte_messages(session: requests.Session) -> list:
    funcs = (
        _get_gcb,
        lambda x: _get_chamber_forecast(x, 'senate'),
        lambda x: _get_chamber_forecast(x, 'house'),
        lambda x: _get_seat_forecasts(x, 'senate'),
        lambda x: _get_seat_forecasts(x, 'house'),
        lambda x: _get_seat_forecasts(x, 'governor'),
    )
    messages = []
    for func in funcs:
        if message := func(session):
            messages.append(message)
        sleep(1)
    return messages


def main():
    session = requests.Session()

    if fte_messages := '\n\n'.join(_get_fte_messages(session)):
        if environ.get('PHONE_NUMBER'):
            _send_text(fte_messages)
        else:
            _send_email('FTE GCB/Forecast Alert', fte_messages)

    if matching_gcb_polls_message := _get_matching_gcb_polls(session):
        _send_email('FTE GCB Polls Alert', matching_gcb_polls_message)

    session.close()

    if twitter_polls_messages := _get_polls_from_twitter():
        _send_email('Twitter Polls Alert', twitter_polls_messages)


if __name__ == '__main__':
    main()
