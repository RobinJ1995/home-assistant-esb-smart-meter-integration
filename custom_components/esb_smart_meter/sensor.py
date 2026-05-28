import logging
import asyncio
import time
from datetime import timedelta, datetime, timezone
from random import randint
from zoneinfo import ZoneInfo
import requests
import csv
import re
import json
from bs4 import BeautifulSoup
from io import StringIO

from homeassistant.components.sensor import SensorEntity, SensorDeviceClass
from homeassistant.const import UnitOfEnergy
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    StatisticData,
    StatisticMetaData,
)

try:
    from homeassistant.components.recorder.models import StatisticMeanType
    _STATISTIC_MEAN_TYPE_NONE = StatisticMeanType.NONE
except ImportError:
    # Older HA versions don't have StatisticMeanType; the field is optional pre-2026.11.
    _STATISTIC_MEAN_TYPE_NONE = None

from .const import DOMAIN

IRELAND_TZ = ZoneInfo("Europe/Dublin")

LOGGER = logging.getLogger(__name__)

# HA's default 30s sensor poll interval is far shorter than a cold ESB fetch (which
# sleeps 15-33s to avoid tripping the captcha lockout), and the data only refreshes
# upstream once a day anyway. Most polls hit the in-memory cache and return instantly.
SCAN_INTERVAL = timedelta(hours=1)

MIN_TIME_BETWEEN_UPDATES = timedelta(hours=12)
# After a failure, hold off for an hour before trying again. ESB / Azure B2C trips a
# captcha challenge on rapid retries that takes ~6 hours to clear, so retrying the
# moment a fetch fails (which HA otherwise does every poll cycle for every sensor)
# only makes the problem worse.
MIN_TIME_BETWEEN_ERROR_RETRIES = timedelta(hours=1)

async def async_setup_entry(hass, entry, async_add_entities):
    """Set up the ESB Smart Meter sensor based on a config entry."""
    username = entry.data["username"]
    password = entry.data["password"]
    mprn = entry.data["mprn"]

    LOGGER.info("Setting up ESB Smart Meter integration for MPRN %s", mprn)

    async def push_statistics(esb_data):
        start = datetime.now()
        metadata, stats = await hass.async_add_executor_job(
            esb_data.build_hourly_statistics, mprn
        )
        if stats:
            async_add_external_statistics(hass, metadata, stats)
            LOGGER.info(
                "Pushed %d hourly statistics rows for MPRN %s in %.2fs",
                len(stats), mprn, (datetime.now() - start).total_seconds(),
            )
        else:
            LOGGER.warning("MPRN %s: no statistics rows built (empty CSV?)", mprn)

    esb_api = ESBCachingApi(
        ESBDataApi(hass=hass, username=username, password=password, mprn=mprn),
        on_refresh=push_statistics,
    )

    sensors = [
        ESBEnergySumSensor(esb_api=esb_api, mprn=mprn,
                           key="yesterday", value_attr="yesterday",
                           name="Yesterday"),
        ESBEnergySumSensor(esb_api=esb_api, mprn=mprn,
                           key="this_month_so_far", value_attr="this_month_so_far",
                           name="This month so far"),
        ESBEnergySumSensor(esb_api=esb_api, mprn=mprn,
                           key="this_year_so_far", value_attr="this_year_so_far",
                           name="This year so far"),
        ESBLatestReadingSensor(esb_api=esb_api, mprn=mprn),
    ]
    async_add_entities(sensors, False)

    async def initial_fetch():
        # Triggers a real fetch immediately after setup so entities populate within
        # ~30s instead of waiting up to a full SCAN_INTERVAL with no state. Runs as
        # a background task so setup itself doesn't block on the ~20s login delays.
        LOGGER.info("MPRN %s: triggering initial fetch; entities will populate when it completes", mprn)
        try:
            await esb_api.fetch()
        except Exception as err:
            LOGGER.warning(
                "MPRN %s: initial fetch failed (%s) — entities will remain unknown "
                "until the next scheduled poll", mprn, err,
            )
            return
        for sensor in sensors:
            sensor.async_schedule_update_ha_state(force_refresh=True)
        LOGGER.info("MPRN %s: initial fetch complete; entities updated", mprn)

    hass.async_create_background_task(
        initial_fetch(), name=f"esb_smart_meter_{mprn}_initial_fetch"
    )
    LOGGER.info("MPRN %s: setup complete", mprn)


def _device_info(mprn):
    return DeviceInfo(
        identifiers={(DOMAIN, mprn)},
        name=f"ESB Smart Meter {mprn}",
        model="Smart Meter (HDF)",
        configuration_url="https://myaccount.esbnetworks.ie/",
    )


class ESBEnergySumSensor(SensorEntity):
    """A kWh sum over a calendar period in Europe/Dublin local time."""

    _attr_has_entity_name = True
    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR

    def __init__(self, *, esb_api, mprn, key, value_attr, name):
        self._esb_api = esb_api
        self._value_attr = value_attr
        self._attr_name = name
        self._attr_unique_id = f"esb_smart_meter_{mprn}_{key}"
        self._attr_device_info = _device_info(mprn)

    async def async_update(self):
        esb_data = await self._esb_api.fetch()
        self._attr_native_value = getattr(esb_data, self._value_attr)


class ESBLatestReadingSensor(SensorEntity):
    """Timestamp of the most recent 30-min interval reported by ESB."""

    _attr_has_entity_name = True
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(self, *, esb_api, mprn):
        self._esb_api = esb_api
        self._attr_name = "Latest reading"
        self._attr_unique_id = f"esb_smart_meter_{mprn}_latest_reading"
        self._attr_device_info = _device_info(mprn)

    async def async_update(self):
        esb_data = await self._esb_api.fetch()
        self._attr_native_value = esb_data.latest_reading_timestamp


class ESBData:
    """Wraps the parsed CSV and exposes the values the sensors and statistics need.

    The CSV's `Read Date and End Time` column is a naive datetime in Europe/Dublin
    local time, marking the *end* of a 30-minute interval (so the interval's start
    is end - 30 minutes). All sums treat the interval's start instant as the timestamp
    that decides which calendar day/month/year it belongs to.
    """

    def __init__(self, *, data):
        self._data = data
        self._parsed = None  # lazy: list of (start_naive_local, kwh) tuples

    def _iter_parsed(self):
        """Parse the CSV rows once into (interval_start_naive_local, kWh) pairs."""
        if self._parsed is None:
            parsed = []
            for row in self._data:
                try:
                    end_naive = datetime.strptime(row['Read Date and End Time'], '%d-%m-%Y %H:%M')
                    start_naive = end_naive - timedelta(minutes=30)
                    value = float(row['Read Value'])
                except (KeyError, ValueError):
                    continue
                parsed.append((start_naive, value))
            self._parsed = parsed
        return self._parsed

    def _sum_local_range(self, since_local, until_local=None):
        total = 0.0
        for start_naive, value in self._iter_parsed():
            if start_naive < since_local:
                continue
            if until_local is not None and start_naive >= until_local:
                continue
            total += value
        return total

    @staticmethod
    def _local_midnight_today():
        """Naive Europe/Dublin midnight of *today*."""
        now_local = datetime.now(tz=IRELAND_TZ)
        return now_local.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    @property
    def yesterday(self):
        today_local = self._local_midnight_today()
        return self._sum_local_range(since_local=today_local - timedelta(days=1),
                                     until_local=today_local)

    @property
    def this_month_so_far(self):
        today_local = self._local_midnight_today()
        return self._sum_local_range(since_local=today_local.replace(day=1))

    @property
    def this_year_so_far(self):
        today_local = self._local_midnight_today()
        return self._sum_local_range(since_local=today_local.replace(month=1, day=1))

    @property
    def latest_reading_timestamp(self):
        """End time of the most recent CSV row, as a UTC datetime, or None if empty."""
        latest_naive_end = None
        for start_naive, _ in self._iter_parsed():
            end_naive = start_naive + timedelta(minutes=30)
            if latest_naive_end is None or end_naive > latest_naive_end:
                latest_naive_end = end_naive
        if latest_naive_end is None:
            return None
        return latest_naive_end.replace(tzinfo=IRELAND_TZ).astimezone(timezone.utc)

    def build_hourly_statistics(self, mprn):
        """Bucket the 30-min readings into hourly stats for HA's long-term-statistics store.

        Returns (metadata, stats_list). Each StatisticData has UTC `start` and a
        cumulative `sum` from the earliest available reading.
        """
        # Bucket by UTC hour. We can't bucket by local-naive hour because on the
        # DST spring-forward day two distinct local hours map to the same UTC hour,
        # which would emit duplicate `start` rows in the stats output.
        hourly = {}
        for start_naive, value in self._iter_parsed():
            start_local = start_naive.replace(tzinfo=IRELAND_TZ)
            start_utc = start_local.astimezone(timezone.utc)
            hour_utc = start_utc.replace(minute=0, second=0, microsecond=0)
            hourly[hour_utc] = hourly.get(hour_utc, 0.0) + value

        stats = []
        cumulative = 0.0
        for hour_utc in sorted(hourly):
            cumulative += hourly[hour_utc]
            stats.append(StatisticData(start=hour_utc, sum=cumulative))

        metadata_kwargs = dict(
            has_mean=False,
            has_sum=True,
            name=f"ESB Smart Meter {mprn} Consumption",
            source=DOMAIN,
            statistic_id=f"{DOMAIN}:consumption_{mprn}",
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        )
        if _STATISTIC_MEAN_TYPE_NONE is not None:
            metadata_kwargs["mean_type"] = _STATISTIC_MEAN_TYPE_NONE
        return StatisticMetaData(**metadata_kwargs), stats


class ESBCachingApi:
    """To not poll ESB constantly. The data only updates like once a day anyway."""

    def __init__(self, esb_api, on_refresh=None) -> None:
        self._esb_api = esb_api
        self._cached_data = None
        self._cached_data_timestamp = None
        self._last_error = None
        self._last_error_timestamp = None
        # The sensors call fetch() concurrently on startup. Serialize so only one
        # full login + download runs; the others then see a warm cache.
        self._lock = asyncio.Lock()
        # Optional async callback invoked exactly once per successful upstream refresh
        # (not on cache hits). Used to push fresh data into HA's statistics store.
        self._on_refresh = on_refresh

    async def fetch(self):
        async with self._lock:
            now = datetime.now()
            cache_is_stale = (self._cached_data_timestamp is None or
                              self._cached_data_timestamp < now - MIN_TIME_BETWEEN_UPDATES)
            if not cache_is_stale:
                LOGGER.debug(
                    "Cache hit (age %s, expires in %s)",
                    now - self._cached_data_timestamp,
                    (self._cached_data_timestamp + MIN_TIME_BETWEEN_UPDATES) - now,
                )
                return self._cached_data

            # If we recently failed, don't hammer the upstream — that's what
            # triggers the multi-hour captcha lockout. Re-raise the cached error.
            if self._last_error_timestamp is not None and \
                self._last_error_timestamp > now - MIN_TIME_BETWEEN_ERROR_RETRIES:
                next_retry = self._last_error_timestamp + MIN_TIME_BETWEEN_ERROR_RETRIES
                LOGGER.debug(
                    "Suppressing fetch — in error cooldown until %s (last error: %s)",
                    next_retry, self._last_error,
                )
                raise RuntimeError(
                    'ESB fetch is in cooldown after a recent failure (will retry after %s). '
                    'Last error: %s' % (next_retry, self._last_error)
                )

            LOGGER.info("Cache is stale (last refreshed: %s) — fetching from ESB",
                        self._cached_data_timestamp or "never")
            fetch_start = datetime.now()
            try:
                self._cached_data = await self._esb_api.fetch()
                duration = (datetime.now() - fetch_start).total_seconds()
                row_count = len(self._cached_data._data)
                LOGGER.info("Fetched %d rows from ESB in %.1fs", row_count, duration)
                self._cached_data_timestamp = datetime.now()
                self._last_error = None
                self._last_error_timestamp = None
            except Exception as err:
                duration = (datetime.now() - fetch_start).total_seconds()
                LOGGER.error("Fetch failed after %.1fs: %s", duration, err)
                self._cached_data = None
                self._cached_data_timestamp = None
                self._last_error = err
                self._last_error_timestamp = datetime.now()
                raise err

            if self._on_refresh is not None:
                # Side-effect on fresh data; don't fail the fetch if the hook errors.
                try:
                    await self._on_refresh(self._cached_data)
                except Exception as hook_err:
                    LOGGER.error('on_refresh hook failed: %s', hook_err)

            return self._cached_data
    

class ESBDataApi:
    """Class for handling the data retrieval."""

    def __init__(self, *, hass, username, password, mprn):
        """Initialize the data object."""
        self._hass = hass
        self._username = username
        self._password = password
        self._mprn = mprn

    def __login(self):
        LOGGER.debug("MPRN %s: starting ESB login flow", self._mprn)
        session = requests.Session()

        # Get CSRF token and stuff
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:142.0) Gecko/20100101 Firefox/142.0'
        })
        login_page = session.get('https://myaccount.esbnetworks.ie/',
                                 allow_redirects=True,
                                 timeout=10)
        LOGGER.debug("Step 1/7: loaded portal page (status=%d, url=%s)",
                     login_page.status_code, login_page.url)
        settings_var = re.findall(r"(?<=var SETTINGS = )\S*;", str(login_page.content))[0][:-1]
        settings = json.loads(settings_var)
        LOGGER.debug("Extracted SETTINGS (csrf=%s..., transId=%s...)",
                     settings['csrf'][:12], settings['transId'][:30])

        session.headers.update({
            'x-csrf-token': settings['csrf'],
        })

        # The Azure B2C flow trips a captcha lockout (taking ~6 hours to clear) if you
        # rush through it, so pause before submitting credentials.
        delay = randint(10, 20)
        LOGGER.debug("Sleeping %ds to avoid Azure B2C bot-detection", delay)
        time.sleep(delay)

        # Login
        LOGGER.debug("Step 2/7: POST SelfAsserted (login with credentials)")
        login_response = session.post(
            'https://login.esbnetworks.ie/esbntwkscustportalprdb2c01.onmicrosoft.com/B2C_1A_signup_signin/SelfAsserted?tx=' + settings['transId'] + '&p=B2C_1A_signup_signin',
            data={
                'signInName': self._username,
                'password': self._password,
                'request_type': 'RESPONSE'
            },
            timeout=10)
        login_response.raise_for_status()
        # SelfAsserted always returns HTTP 200; the real status is in the JSON body.
        try:
            login_status = json.loads(login_response.text).get('status')
        except ValueError:
            login_status = None
        if login_status != '200':
            raise RuntimeError(
                'ESB SelfAsserted login did not succeed (status=%r, body=%r)' %
                (login_status, login_response.text[:200])
            )
        LOGGER.debug("SelfAsserted credentials accepted (status=200)")

        LOGGER.debug("Step 3/7: GET CombinedSigninAndSignup/confirmed (fetch auth redirect form)")
        confirm_login_response = session.get('https://login.esbnetworks.ie/esbntwkscustportalprdb2c01.onmicrosoft.com/B2C_1A_signup_signin/api/CombinedSigninAndSignup/confirmed',
                                             params={
                                                'rememberMe': False,
                                                'csrf_token': settings['csrf'],
                                                'tx': settings['transId'],
                                                'p': 'B2C_1A_signup_signin'
                                             },
                                             timeout=10)
        confirm_login_response.raise_for_status()
        soup = BeautifulSoup(confirm_login_response.content, 'html.parser')
        form = soup.find('form', {'id': 'auto'})
        if form is None:
            raise RuntimeError(
                'ESB confirmed-login response did not contain the expected auth form '
                '(url=%s, body starts with: %r)' %
                (confirm_login_response.url, confirm_login_response.text[:200])
            )
        state = form.find('input', {'name': 'state'})['value']
        client_info = form.find('input', {'name': 'client_info'})['value']
        code = form.find('input', {'name': 'code'})['value']
        LOGGER.debug("Extracted auth form (action=%s)", form['action'])
        delay = randint(2, 5)
        LOGGER.debug("Sleeping %ds before posting auth form", delay)
        time.sleep(delay)
        LOGGER.debug("Step 4/7: POST auth form to %s", form['action'])
        session.post(
            form['action'],
            data={
                'state': state,
                'client_info': client_info,
                'code': code
            },
            timeout=10
        ).raise_for_status()

        # Establish the post-login portal session that the data endpoint requires.
        LOGGER.debug("Step 5/7: GET portal home")
        session.get('https://myaccount.esbnetworks.ie', timeout=10).raise_for_status()
        delay = randint(3, 8)
        LOGGER.debug("Sleeping %ds before loading historic consumption page", delay)
        time.sleep(delay)
        LOGGER.debug("Step 6/7: GET /Api/HistoricConsumption")
        session.get('https://myaccount.esbnetworks.ie/Api/HistoricConsumption',
                    headers={'Referer': 'https://myaccount.esbnetworks.ie/'},
                    timeout=10).raise_for_status()

        # The CSRF token is only valid for the login endpoints; drop it before the data call.
        session.headers.pop('x-csrf-token', None)

        LOGGER.debug("Login flow complete (session has %d cookies)", len(session.cookies))
        return session

    def __fetch_data(self, requests_session):
        """Fetch the power usage data from ESB"""
        LOGGER.debug("Step 7a/7: GET /af/t (XSRF token for data download)")
        token_response = requests_session.get(
            'https://myaccount.esbnetworks.ie/af/t',
            headers={
                'X-Returnurl': 'https://myaccount.esbnetworks.ie/Api/HistoricConsumption',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
            },
            timeout=10)
        token_response.raise_for_status()
        xsrf_token = json.loads(token_response.text)['token']
        LOGGER.debug("Got XSRF token (len=%d)", len(xsrf_token))

        LOGGER.debug("Step 7b/7: POST /DataHub/DownloadHdfPeriodic for MPRN %s (intervalkwh)", self._mprn)
        csv_data_response = requests_session.post(
            'https://myaccount.esbnetworks.ie/DataHub/DownloadHdfPeriodic',
            json={'mprn': self._mprn, 'searchType': 'intervalkwh'},
            headers={
                'X-Xsrf-Token': xsrf_token,
                'X-Returnurl': 'https://myaccount.esbnetworks.ie/Api/HistoricConsumption',
                'Referer': 'https://myaccount.esbnetworks.ie/Api/HistoricConsumption',
                'Origin': 'https://myaccount.esbnetworks.ie',
                'Content-Type': 'application/json',
            },
            timeout=30)
        csv_data_response.raise_for_status()
        csv_data = csv_data_response.content.decode('utf-8')
        LOGGER.debug("Downloaded CSV (%d bytes)", len(csv_data))

        return csv_data

    def __csv_to_dict(self, csv_data):
        reader = csv.DictReader(StringIO(csv_data))
        rows = [r for r in reader]
        LOGGER.debug("Parsed CSV into %d rows", len(rows))
        return rows

    async def fetch(self):
        session = await self._hass.async_add_executor_job(self.__login)
        csv_data = await self._hass.async_add_executor_job(self.__fetch_data, session)
        data = await self._hass.async_add_executor_job(self.__csv_to_dict, csv_data)

        return ESBData(data=data)
