import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback
from .const import DOMAIN

@callback
def configured_instances(hass):
    """Return a set of configured instances."""
    return set(entry.data['mprn']
               for entry
               in hass.config_entries.async_entries(DOMAIN))

class ESBSmartMeterConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for ESB Smart Meter."""

    VERSION = 1
    CONNECTION_CLASS = config_entries.CONN_CLASS_CLOUD_POLL

    async def async_step_user(self, user_input=None):
        """Handle the initial step."""
        errors = {}

        if user_input is not None:
            if user_input["mprn"] in configured_instances(self.hass):
                errors["base"] = "mprn_exists"
            else:
                return self.async_create_entry(title="ESB Smart Meter", data=user_input)

        data_schema = vol.Schema({
            vol.Required("username"): str,
            vol.Required("password"): str,
            vol.Required("mprn"): str,
        })

        return self.async_show_form(step_id="user", data_schema=data_schema, errors=errors)
