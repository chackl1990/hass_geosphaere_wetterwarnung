from __future__ import annotations

from datetime import timedelta
from typing import List, Tuple

from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import (
    DOMAIN,
    CONF_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL,
    CONF_EXTRA_COORDS,
    DEFAULT_EXTRA_COORDS,
    CONF_GRACE_PERIOD,
    DEFAULT_GRACE_PERIOD,
)

class _NoopLogger:
    def __getattr__(self, name):
        return lambda *args, **kwargs: None


_NOOP_LOGGER = _NoopLogger()


def _parse_extra_coords(text: str) -> List[Tuple[float, float]]:
    """Parse Eingabe 'lat1,lon1;lat2,lon2' zu Float-Tupeln."""
    if not text:
        return []
    coords: List[Tuple[float, float]] = []
    for part in text.split(";"):
        part = part.strip()
        if not part:
            continue
        pieces = part.split(",")
        if len(pieces) != 2:
            continue
        try:
            lat = float(pieces[0].strip())
            lon = float(pieces[1].strip())
        except (TypeError, ValueError):
            continue
        coords.append((lat, lon))
    return coords


def _warning_key(warning: dict) -> str:
    raw = warning.get("properties", {}).get("rawinfo", {})
    for key in ("id", "awcode", "warnid", "wcode"):
        val = raw.get(key)
        if val:
            return f"{key}:{val}"
    return "|".join(
        [
            str(raw.get("wtype", "")),
            str(raw.get("wlevel", "")),
            str(raw.get("start", "")),
            str(raw.get("end", "")),
        ]
    )


def _get_end_ts(warning: dict) -> int:
    raw = warning.get("properties", {}).get("rawinfo", {})
    try:
        return int(raw.get("end", 0))
    except (TypeError, ValueError):
        return 0


def _copy_with_end(warning: dict, new_end: int) -> dict:
    props = dict(warning.get("properties", {}))
    raw = dict(props.get("rawinfo", {}))
    raw["end"] = new_end
    props["rawinfo"] = raw
    copy = dict(warning)
    copy["properties"] = props
    return copy


def _extend_if_grace_applies(
    warning: dict, now_ts: int, grace_seconds: int, allow_invalid_end: bool
) -> dict | None:
    end_ts = _get_end_ts(warning)
    if end_ts <= 0:
        return warning if allow_invalid_end else None
    if now_ts <= end_ts:
        return warning
    if now_ts <= end_ts + grace_seconds:
        return _copy_with_end(warning, end_ts + grace_seconds)
    return None


class geosphereCoordinator(DataUpdateCoordinator):
    """Coordinator für Geosphere Wetterwarnung."""

    def __init__(self, hass: HomeAssistant, config_entry):
        self.hass = hass
        self.config_entry = config_entry

        # Felder für API-Status
        self.last_http_status: int | None = None
        self.last_http_response: str | None = None
        self.had_partial_failure: bool = False
        self._last_successful_data: dict | None = None
        self._last_non_empty_data: dict | None = None
        self._last_non_empty_utc = None
        self._warning_cache: dict[str, dict] = {}
        self.last_request_utc = None

        scan_interval = self._get_entry_value(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)

        super().__init__(
            hass,
            _NOOP_LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=scan_interval),
        )

    async def _async_update_data(self):
        """Daten von der ZAMG / Geosphere API holen."""
        self.last_request_utc = dt_util.utcnow()
        grace_seconds = self._get_entry_value(
            CONF_GRACE_PERIOD, DEFAULT_GRACE_PERIOD
        )

        zone = self.hass.states.get("zone.home")
        if zone is None:
            self.last_http_status = None
            self.last_http_response = "zone.home not found"
            raise UpdateFailed("zone.home not found")

        lon = zone.attributes.get("longitude")
        lat = zone.attributes.get("latitude")
        if lon is None or lat is None:
            self.last_http_status = None
            self.last_http_response = "zone.home has no coordinates"
            raise UpdateFailed("zone.home has no coordinates")

        coords: List[Tuple[float, float]] = []
        try:
            coords.append((float(lat), float(lon)))
        except (TypeError, ValueError):
            self.last_http_status = None
            self.last_http_response = "zone.home has invalid coordinates"
            raise UpdateFailed("zone.home has invalid coordinates")

        extra = self._get_entry_value(CONF_EXTRA_COORDS, DEFAULT_EXTRA_COORDS)
        coords.extend(_parse_extra_coords(extra))
        if not coords:
            self.last_http_status = None
            self.last_http_response = "no coordinates to query"
            raise UpdateFailed("no coordinates to query")

        session = async_get_clientsession(self.hass)
        combined_warnings: list = []
        any_success = False
        max_http_status: int | None = None
        error_messages: list[str] = []

        for lat_val, lon_val in coords:
            url = (
                "https://warnungen.zamg.at/wsapp/api/getWarningsForCoords"
                f"?lon={lon_val}&lat={lat_val}&lang=de"
            )
            try:
                async with session.get(url, timeout=10) as resp:
                    status = resp.status
                    if max_http_status is None or status > max_http_status:
                        max_http_status = status

                    if status != 200:
                        try:
                            text = await resp.text()
                        except Exception:  # noqa: BLE001
                            text = "<no body>"
                        error_messages.append(
                            f"{lat_val},{lon_val}: HTTP {status} {text}"
                        )
                        continue

                    data = await resp.json()
                    any_success = True

                    props = data.get("properties", {}) or {}
                    warnings = props.get("warnings", []) or []
                    combined_warnings.extend(warnings)

            except Exception as err:  # noqa: BLE001
                error_messages.append(f"{lat_val},{lon_val}: {err!r}")
                continue

        self.had_partial_failure = bool(error_messages)
        if max_http_status is None:
            max_http_status = 200 if any_success else None
        self.last_http_status = max_http_status
        self.last_http_response = "; ".join(error_messages) if error_messages else None

        if any_success:
            now_ts = int(self.last_request_utc.timestamp())
            current_keys: set[str] = set()
            warnings_with_grace: list = []

            for warning in combined_warnings:
                key = _warning_key(warning)
                current_keys.add(key)
                self._warning_cache[key] = {
                    "warning": warning,
                    "last_seen_ts": now_ts,
                }
                extended = _extend_if_grace_applies(
                    warning, now_ts, grace_seconds, allow_invalid_end=True
                )
                if extended is not None:
                    warnings_with_grace.append(extended)

            expired_keys: list[str] = []
            for key, entry in self._warning_cache.items():
                if key in current_keys:
                    continue
                cached = entry.get("warning", {})
                last_seen_ts = entry.get("last_seen_ts", 0)
                if grace_seconds <= 0 or not last_seen_ts:
                    expired_keys.append(key)
                    continue
                if now_ts - last_seen_ts > grace_seconds:
                    expired_keys.append(key)
                    continue
                extended = _extend_if_grace_applies(
                    cached, now_ts, grace_seconds, allow_invalid_end=False
                )
                if extended is not None:
                    warnings_with_grace.append(extended)
                else:
                    expired_keys.append(key)

            for key in expired_keys:
                self._warning_cache.pop(key, None)

            result = {"properties": {"warnings": warnings_with_grace}}
            self._last_successful_data = result
            if warnings_with_grace:
                self._last_non_empty_data = result
                self._last_non_empty_utc = self.last_request_utc
            return result

        if self._last_successful_data is not None:
            return self._last_successful_data

        raise UpdateFailed("Error fetching data: all requests failed")

    def set_update_interval(self, seconds: int) -> None:
        """Update-Intervall ändern (falls du später doch Optionen nutzt)."""
        self.update_interval = timedelta(seconds=seconds)

    def _get_entry_value(self, key: str, default):
        """Hole Wert bevorzugt aus Optionen, sonst aus Daten."""
        return self.config_entry.options.get(
            key, self.config_entry.data.get(key, default)
        )

