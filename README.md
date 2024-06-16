# ESB Smart Meter integration for Home Assistant

Heavily inspired by https://github.com/badger707/esb-smart-meter-reading-automation

## Requirements

- Account at https://myaccount.esbnetworks.ie/
- Your meter's MPRN

## Setup

1. Install into your Home Assistant's `custom_components` folder
2. Activate the integration
3. In the popup, enter your ESB account's username, password, and MPRN

If all went well, you should now have the following entities in Home Assistant:
- `sensor.esb_electricity_usage_today`
- `sensor.esb_electricity_usage_last_24_hours`
- `sensor.esb_electricity_usage_this_week`
- `sensor.esb_electricity_usage_last_7_days`
- `sensor.esb_electricity_usage_this_month`
- `sensor.esb_electricity_usage_last_30_days`
