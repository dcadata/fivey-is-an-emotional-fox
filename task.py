import json
import re
from time import sleep

import pandas as pd
import requests
from bs4 import BeautifulSoup

from messaging import send_email

_POLLS_PATTERN = '#MI|#..(Sen|SEN|Gov|GOV) General'


def _read_latest() -> dict:
    data_from_file = json.load(open('data/latest.txt'))
    return data_from_file


def _update_latest(new_data: dict) -> None:
    data_from_file = _read_latest()
    data_from_file.update(new_data)
    json.dump(data_from_file, open('data/latest.txt', 'w'))


def _get_gcb(session: requests.Session) -> str:
    data_filepath = 'data/generic_ballot_averages.csv'

    existing_content = open(data_filepath, 'rb').read()
    new_content = session.get('https://projects.fivethirtyeight.com/polls/data/generic_ballot_averages.csv').content
    if existing_content == new_content:
        return ''
    open(data_filepath, 'wb').write(new_content)

    data = pd.read_csv(data_filepath, usecols=['candidate', 'pct_estimate', 'election'])
    data = data[data.election == '2022-11-08'].drop(columns=['election']).iloc[-2:]
    data.candidate = data.candidate.apply(lambda x: x[0])
    estimates = data.groupby('candidate').pct_estimate.sum()
    difference = round(estimates['R'] - estimates['D'], 2)
    data.pct_estimate = data.pct_estimate.apply(lambda x: round(x, 2))
    estimates = data.groupby('candidate').pct_estimate.sum()

    output = 'D: {D}\nR: {R}\nR+{difference}'.format(difference=difference, **estimates)
    if output == _read_latest()['gcb']:
        return ''
    _update_latest(dict(gcb=output))
    return output


def _get_feed(session: requests.Session) -> str:
    response = session.get('https://fivethirtyeight.com/politics/feed/')
    feed = BeautifulSoup(response.text, 'xml')
    data = [
        list(map(lambda x: item.find(x).text, ('title', 'link', 'pubDate')))
        for item in feed.find_all('item') if 'forecast' in item.find('title').text.lower()
    ]
    output = '\n\n'.join('\n'.join(i) for i in data)
    if output == _read_latest()['feed']:
        return ''
    _update_latest(dict(feed=output))
    return output


def _get_polls() -> str:
    response = requests.get('https://nitter.net/PollTrackerUSA/rss')
    feed = BeautifulSoup(response.text, 'xml')
    tweets = feed.select('item')
    previous_latest_link = _read_latest()['polls']

    polls = []
    for tweet in tweets:
        if tweet.find('link').text == previous_latest_link:
            break
        title, pubdate = map(lambda x: tweet.find(x).text.strip(), ('title', 'pubDate'))
        if re.search(_POLLS_PATTERN, title):
            polls.append(dict(title=title, pubdate=pubdate))

    _update_latest(dict(polls=tweets[0].find('link').text))
    return '\n\n--\n\n'.join('{title}\n\nPubDate: {pubdate}'.format(**poll) for poll in polls)


def main():
    output = []

    # FTE
    session = requests.Session()

    if gcb_summary := _get_gcb(session):
        output.append('FTE GCB')
        output.append(gcb_summary)

    sleep(1)
    if feed_summary := _get_feed(session):
        output.append('FTE Feed')
        output.append(feed_summary)

    session.close()

    # polls
    if polls_summary := _get_polls():
        output.append('Polls')
        output.append(polls_summary)

    # send email
    if output:
        send_email('FTE/Polls Alert', '\n\n'.join(output))


if __name__ == '__main__':
    main()
