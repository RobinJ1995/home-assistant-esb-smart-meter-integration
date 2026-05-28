from homeassistant.core import HomeAssistant
from homeassistant.helpers.typing import ConfigType
from .const import DOMAIN


async def async_setup(hass: HomeAssistant, config: ConfigType):
    """Set up the ESB Smart Meter component."""
    return True

async def async_setup_entry(hass: HomeAssistant, entry):
    """Set up ESB Smart Meter from a config entry."""
    await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])
    return True

async def async_unload_entry(hass: HomeAssistant, entry):
    """Unload a config entry."""
    return await hass.config_entries.async_unload_platforms(entry, ["sensor"])
