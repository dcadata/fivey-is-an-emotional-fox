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

_CONFIG = configparser.ConfigParser()
_CONFIG.read('config.ini')


def _send_email(subject: str, body: str, to: str) -> None:
    sender = environ['EMAIL_SENDER']
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = to

    server = SMTP_SSL(host='smtp.gmail.com', port=465)
    server.login(sender, environ['EMAIL_PASSWORD'])
    server.send_message(msg)
    server.quit()


def _read_latest() -> dict:
    data_from_file = json.load(open('data/latest.json'))
    return data_from_file


def _update_latest(new_data: dict) -> None:
    data_from_file = _read_latest()
    data_from_file.update(new_data)
    json.dump(data_from_file, open('data/latest.json', 'w'))


def _get_gcb(session: requests.Session) -> str:
    data_filepath = 'data/generic_ballot_averages.csv'

    existing_content = open(data_filepath, 'rb').read()
    new_content = session.get('https://projects.fivethirtyeight.com/polls/data/generic_ballot_averages.csv').content
    if existing_content == new_content:
        return ''
    open(data_filepath, 'wb').write(new_content)

    data = pd.read_csv(data_filepath, usecols=['candidate', 'pct_estimate', 'election'])
    data = data[data.election == '2022-11-08'].drop(columns=['election']).iloc[-2:]
    data['party'] = data.candidate.apply(lambda x: x[0])

    unrounded_estimates = data.groupby('party').pct_estimate.sum()
    unrounded_lead = unrounded_estimates['D'] - unrounded_estimates['R']

    change_since_latest = unrounded_lead - _read_latest()['gcb']
    if abs(change_since_latest) < float(_CONFIG['config']['gcb_notification_threshold']):
        return ''
    _update_latest(dict(gcb=unrounded_lead))

    data.pct_estimate = data.pct_estimate.apply(lambda x: round(x, 2))
    return 'D: {D} / R: {R}\n{leader}+{lead} / chg: {change_since_latest_gainer}+{change_since_latest}'.format(
        lead=abs(round(unrounded_lead, 2)),
        leader='D' if unrounded_lead > 0 else 'R',
        change_since_latest=abs(round(change_since_latest, 2)),
        change_since_latest_gainer='D' if change_since_latest > 0 else 'R',
        **data.groupby('party').pct_estimate.sum(),
    )


def _get_senate_national_forecast(session: requests.Session) -> str:
    data_filepath = 'data/senate_national_toplines_2022.csv'
    url = 'https://projects.fivethirtyeight.com/2022-general-election-forecast-data/senate_national_toplines_2022.csv'

    existing_content = open(data_filepath, 'rb').read()
    new_content = session.get(url).content
    if existing_content == new_content:
        return ''
    open(data_filepath, 'wb').write(new_content)

    expression_choice = _CONFIG['config'].get('fte_forecast', '_deluxe')
    data = pd.read_csv(data_filepath, usecols=[
        'expression', 'chamber_Dparty', 'chamber_Rparty', 'median_seats_Dparty', 'median_seats_Rparty'])
    data = data[data.expression == expression_choice].drop(columns=['expression']).iloc[0]
    current = dict(
        probD=int(data.chamber_Dparty.round(2) * 100),
        probR=int(data.chamber_Rparty.round(2) * 100),
        seatsD=int(data.median_seats_Dparty),
        seatsR=int(data.median_seats_Rparty),
    )
    if current == _read_latest()['senate_national']:
        return ''
    _update_latest(dict(senate_national=current))

    return 'SENATE ({expression_choice})\nControl: D:{probD}% R:{probR}%\nSeats: D:{seatsD} R:{seatsR}'.format(
        **current, expression_choice=expression_choice[1:])


def _get_polls() -> str:
    if not _CONFIG['config'].get('polls_pattern'):
        return ''

    response = requests.get('https://nitter.net/PollTrackerUSA/rss')
    feed = BeautifulSoup(response.text, 'xml')
    tweets = feed.select('item')
    if not tweets:
        return ''

    previous_latest_link = _read_latest()['polls']
    polls = []
    for tweet in tweets:
        if tweet.find('link').text == previous_latest_link:
            break
        title, pubdate = map(lambda x: tweet.find(x).text.strip(), ('title', 'pubDate'))
        if re.search(_CONFIG['config']['polls_pattern'], title):
            polls.append(dict(title=title, pubdate=pubdate))

    _update_latest(dict(polls=tweets[0].find('link').text))
    return '\n\n--\n\n'.join('{title}\n\nPubDate: {pubdate}'.format(**poll) for poll in polls)


def main():
    # FTE
    fte_messages = []

    session = requests.Session()

    if gcb_summary := _get_gcb(session):
        fte_messages.append(gcb_summary)
    sleep(1)

    session.close()

    if fte_messages:
        _send_email('FTE GCB/Forecast Alert', '\n\n---\n\n'.join(fte_messages), environ['TEXT_RECIPIENT'])

    # polls
    if polls_summary := _get_polls():
        _send_email('Polls Alert', polls_summary, environ['EMAIL_RECIPIENT'])


if __name__ == '__main__':
    main()
