from homeassistant.core import HomeAssistant
from homeassistant.helpers.typing import ConfigType
from .const import DOMAIN


async def async_setup(hass: HomeAssistant, config: ConfigType):
    """Set up the ESB Smart Meter component."""
    return True

async def async_setup_entry(hass: HomeAssistant, entry):
    """Set up ESB Smart Meter from a config entry."""
    hass.async_create_task(
        hass.config_entries.async_forward_entry_setup(entry, "sensor")
    )
    return True

async def async_unload_entry(hass: HomeAssistant, entry):
    """Unload a config entry."""
    await hass.config_entries.async_forward_entry_unload(entry, "sensor")
    return True
