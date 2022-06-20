import re
from time import sleep

import pandas as pd
import requests
from bs4 import BeautifulSoup

from messaging import send_email

_POLLS_PATTERN = '#MI|#..(Sen|SEN|Gov|GOV) General'


def _get_gcb(session: requests.Session) -> str:
    filepath = 'data/generic_ballot_averages.csv'

    existing_content = open(filepath, 'rb').read()
    new_content = session.get('https://projects.fivethirtyeight.com/polls/data/generic_ballot_averages.csv').content
    if existing_content == new_content:
        return ''
    open(filepath, 'wb').write(new_content)

    data = pd.read_csv(filepath, usecols=['candidate', 'pct_estimate', 'election'])
    data = data[data.election == '2022-11-08'].drop(columns=['election'])
    data = data.iloc[-2:]
    data.candidate = data.candidate.apply(lambda x: x[0])
    estimates = data.groupby('candidate').pct_estimate.sum()
    difference = round(estimates['R'] - estimates['D'], 2)
    data.pct_estimate = data.pct_estimate.apply(lambda x: round(x, 2))
    estimates = data.groupby('candidate').pct_estimate.sum()

    gcb_summary = 'D: {D}\nR: {R}\nR+{difference}'.format(difference=difference, **estimates)
    return gcb_summary if gcb_summary != open('data/gcb_summary.txt').read() else ''


def _get_feed(session: requests.Session) -> str:
    response = session.get('https://fivethirtyeight.com/politics/feed/')
    feed = BeautifulSoup(response.text, 'xml')
    data = []
    for item in feed.find_all('item'):
        if 'forecast' in item.find('title').text.lower():
            data.append([item.find(field).text for field in ('title', 'link', 'pubDate')])
    forecast_summary = '\n\n___\n\n'.join('\n'.join(i) for i in data)
    return forecast_summary if forecast_summary != open('data/forecast_summary.txt').read() else ''


def _get_polls() -> str:
    previous_latest_link = open('data/polls.txt').read()
    response = requests.get('https://nitter.net/PollTrackerUSA/rss')
    soup = BeautifulSoup(response.text, 'xml')
    tweets = soup.select('item')
    open('data/polls.txt', 'w').write(tweets[0].find('link').text)

    polls = []
    for tweet in tweets:
        if tweet.find('link').text == previous_latest_link:
            break
        title, pubdate = map(lambda x: tweet.find(x).text.strip(), ('title', 'pubDate'))
        if re.search(_POLLS_PATTERN, title):
            polls.append(dict(title=title, pubdate=pubdate))

    return '\n\n'.join('{title}\n\nPubDate: {pubdate}'.format(**poll) for poll in polls) if polls else ''


def main():
    messages = []

    # FTE
    session = requests.Session()

    if gcb_summary := _get_gcb(session):
        messages.append(gcb_summary)
        open('data/gcb_summary.txt', 'w').write(gcb_summary)

    sleep(1)

    if forecast_summary := _get_feed(session):
        messages.append(forecast_summary)
        open('data/forecast_summary.txt', 'w').write(forecast_summary)

    session.close()

    # polls
    if polls_summary := _get_polls():
        messages.append(polls_summary)

    send_email('FTE/Polls Alert', '\n\n__\n\n'.join(messages))


if __name__ == '__main__':
    main()
