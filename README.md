# fivey-is-an-emotional-fox

Notifies user of changes in FiveThirtyEight's generic congressional ballot and forecasts

To set this up for yourself:

* Fork this repo.
* Go to Settings > Secrets > Actions (`https://github.com/YOUR_USERNAME/fivey-is-an-emotional-fox/settings/secrets/actions`) and add the following repository secrets:
    * `TOKEN`: pushbullet instructions to be added later
* Update `config.ini` to your notification preferences.
* In the `data` directory, delete `latest.json`. Rename `latest.json.example` to `latest.json`.

This is an unofficial project - it's not affiliated with FiveThirtyEight.