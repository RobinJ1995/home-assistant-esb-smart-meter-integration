# ESB Smart Meter integration for Home Assistant

Inspired by [https://github.com/badger707/esb-smart-meter-reading-automation](https://github.com/RobinJ1995/home-assistant-esb-smart-meter-integration)

## Pre-Requirements

- Account at https://myaccount.esbnetworks.ie/
- Your meter's MPRN

## Installation

1. Search for and install `esb-smart-meter-integration` follow the steps here [HACS](https://www.hacs.xyz/docs/faq/custom_repositories/). This will download to your Home Assistant's `custom_components` folder
2. Restart Home Assistant
3. Enable the integration
4. In the popup, enter your ESB account's username, password, and MPRN

If all went well, you should now have the following entities in Home Assistant:
- `sensor.esb_electricity_usage_today`
- `sensor.esb_electricity_usage_last_24_hours`
- `sensor.esb_electricity_usage_this_week`
- `sensor.esb_electricity_usage_last_7_days`
- `sensor.esb_electricity_usage_this_month`
- `sensor.esb_electricity_usage_last_30_days`
