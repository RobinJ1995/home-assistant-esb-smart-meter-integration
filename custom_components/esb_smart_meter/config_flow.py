import logging

import voluptuous as vol
from homeassistant import config_entries
from .const import DOMAIN
from .sensor import ESBDataApi, InvalidAuth

LOGGER = logging.getLogger(__name__)

USER_SCHEMA = vol.Schema({
    vol.Required("username"): str,
    vol.Required("password"): str,
    vol.Required("mprn"): str,
})

REAUTH_SCHEMA = vol.Schema({
    vol.Required("password"): str,
})


class ESBSmartMeterConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for ESB Smart Meter."""

    VERSION = 1

    async def _async_validate(self, data):
        """Log in and download the data; return an error key, or None on success.

        On success the downloaded data is left in hass.data for the entry's setup.
        """
        api = ESBDataApi(hass=self.hass,
                         username=data["username"],
                         password=data["password"],
                         mprn=data["mprn"])
        try:
            esb_data = await api.fetch()
        except InvalidAuth:
            return "invalid_auth"
        except Exception:
            LOGGER.exception("Could not validate ESB credentials")
            return "cannot_connect"
        self.hass.data.setdefault(DOMAIN, {})[data["mprn"]] = esb_data
        return None

    async def async_step_user(self, user_input=None):
        """Handle the initial step."""
        errors = {}

        if user_input is not None:
            await self.async_set_unique_id(user_input["mprn"])
            self._abort_if_unique_id_configured()
            # Entries created before unique IDs were set are only identifiable by data.
            if any(entry.data.get("mprn") == user_input["mprn"]
                   for entry in self._async_current_entries()):
                return self.async_abort(reason="already_configured")
            error = await self._async_validate(user_input)
            if error:
                errors["base"] = error
            else:
                return self.async_create_entry(title="ESB Smart Meter", data=user_input)

        return self.async_show_form(step_id="user", data_schema=USER_SCHEMA, errors=errors)

    async def async_step_reauth(self, entry_data):
        """Handle ESB rejecting the stored credentials."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input=None):
        """Ask for a new password and validate it."""
        errors = {}
        entry = self._get_reauth_entry()

        if user_input is not None:
            error = await self._async_validate({**entry.data, "password": user_input["password"]})
            if error:
                errors["base"] = error
            else:
                return self.async_update_reload_and_abort(
                    entry, data_updates={"password": user_input["password"]}
                )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=REAUTH_SCHEMA,
            description_placeholders={"username": entry.data["username"],
                                      "mprn": entry.data["mprn"]},
            errors=errors,
        )
