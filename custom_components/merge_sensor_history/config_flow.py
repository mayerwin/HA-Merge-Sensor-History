"""Config flow for Merge Sensor History."""

from homeassistant import config_entries
from .const import DOMAIN


class MergeSensorHistoryConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Merge Sensor History."""

    VERSION = 1

    async def async_step_user(self, user_input=None):
        """Handle the initial step - just enable the integration."""
        if self._async_current_entries():
            return self.async_abort(reason="already_configured")

        if user_input is not None:
            return self.async_create_entry(
                title="Merge Sensor History",
                data={},
            )

        return self.async_show_form(step_id="user")
