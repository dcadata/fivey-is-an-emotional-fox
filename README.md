# fivey-is-an-emotional-fox

Notifies user of changes in FiveThirtyEight's generic congressional ballot and forecasts

To set this up for yourself:

* Fork this repo.
* Go to Settings > Secrets > Actions (`https://github.com/YOUR_USERNAME/fivey-is-an-emotional-fox/settings/secrets/actions`) and add the following repository secrets:
    * `EMAIL_SENDER`: the email address that will send the notification (I recommend making a new Gmail account)
    * `EMAIL_PASSWORD`: the password associated with `EMAIL_SENDER` (if possible, use an app-specific password)
    * `EMAIL_RECIPIENT`: the email address that will receive the notification
