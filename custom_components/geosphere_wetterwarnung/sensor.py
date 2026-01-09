from __future__ import annotations

from datetime import datetime
from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import (
    DOMAIN,
    WARNING_TYPES,
    ATTR_REMAINING_HOURS,
    ATTR_UNTIL,
)
from .coordinator import geosphereCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    data = hass.data[DOMAIN][entry.entry_id]
    coordinator: geosphereCoordinator = data["coordinator"]

    entities: list[SensorEntity] = []

    # Level-Sensor pro Warnungstyp (1..7)
    for wtype, name in WARNING_TYPES.items():
        if wtype == 0:
            continue
        entities.append(
            WarningLevelSensor(
                coordinator=coordinator,
                entry_id=entry.entry_id,
                wtype=wtype,
            )
        )

    async_add_entities(entities)


def _get_warnings(data: dict[str, Any]) -> list[dict[str, Any]]:
    return data.get("properties", {}).get("warnings", []) or []


def _now_ts() -> int:
    return int(dt_util.utcnow().timestamp())


def _split_warnings_by_time(
    data: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    warnings = _get_warnings(data)
    now_ts = _now_ts()
    active: list[dict[str, Any]] = []
    future: list[dict[str, Any]] = []
    for w in warnings:
        raw = w.get("properties", {}).get("rawinfo", {})
        start = int(raw.get("start", 0))
        end = int(raw.get("end", 0))
        if start <= now_ts <= end:
            active.append(w)
        elif start > now_ts:
            future.append(w)
    return active, future


def _filter_by_type(
    warnings: list[dict[str, Any]], wtype: int
) -> list[dict[str, Any]]:
    res: list[dict[str, Any]] = []
    for w in warnings:
        raw = w.get("properties", {}).get("rawinfo", {})
        try:
            wt = int(raw.get("wtype", 0))
        except (TypeError, ValueError):
            wt = 0
        if wt == wtype:
            res.append(w)
    return res


def _highest_level(warnings: list[dict[str, Any]]) -> int:
    level = 0
    for w in warnings:
        raw = w.get("properties", {}).get("rawinfo", {})
        try:
            lv = int(raw.get("wlevel", 0))
        except (TypeError, ValueError):
            lv = 0
        if lv > level:
            level = lv
    return level


def _last_end(warnings: list[dict[str, Any]]) -> int | None:
    last: int | None = None
    for w in warnings:
        raw = w.get("properties", {}).get("rawinfo", {})
        try:
            end = int(raw.get("end", 0))
        except (TypeError, ValueError):
            continue
        if last is None or end > last:
            last = end
    return last


def _icon_color_for_level(level: int) -> str:
    if level <= 0:
        return "green"
    if level == 1:
        return "yellow"
    if level == 2:
        return "orange"
    return "red"


def _icon_for_type(wtype: int) -> str:
    if wtype == 1:  # Wind
        return "mdi:weather-windy"
    if wtype == 2:  # Regen
        return "mdi:weather-pouring"
    if wtype == 3:  # Schnee
        return "mdi:weather-snowy-heavy"
    if wtype == 4:  # Glatteis
        return "mdi:snowflake-alert"
    if wtype == 5:  # Gewitter
        return "mdi:weather-lightning-rainy"
    if wtype == 6:  # Hitze
        return "mdi:heat-wave"
    if wtype == 7:  # Kälte
        return "mdi:snowflake-melt"
    return "mdi:alert-circle"


class WarningLevelSensor(CoordinatorEntity, SensorEntity):
    _attr_has_entity_name = True

    def __init__(self, coordinator: geosphereCoordinator, entry_id: str, wtype: int):
        super().__init__(coordinator)
        self._entry_id = entry_id
        self._wtype = wtype

        typename = WARNING_TYPES.get(wtype, f"Typ {wtype}")
        self._attr_unique_id = f"{entry_id}_wlevel_{wtype}"
        self._attr_name = f"{typename} Warnungslevel"

    @property
    def icon(self) -> str:
        return _icon_for_type(self._wtype)

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry_id)},
            name="GeoSphere Wetterwarnungen",
            manufacturer="ZAMG / Geosphere Austria",
        )

    @property
    def native_value(self) -> int:
        data = self.coordinator.data or {}
        active, _ = _split_warnings_by_time(data)
        active_for_type = _filter_by_type(active, self._wtype)
        level = _highest_level(active_for_type)
        return level

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        data = self.coordinator.data or {}
        active, _ = _split_warnings_by_time(data)
        active_for_type = _filter_by_type(active, self._wtype)

        attrs: dict[str, Any] = {}

        level = _highest_level(active_for_type)

        last_end = _last_end(active_for_type)
        now_ts = _now_ts()

        if last_end is not None and last_end > now_ts and level > 0:
            remaining_seconds = last_end - now_ts
            remaining_hours = round(remaining_seconds / 3600.0, 2)
            attrs[ATTR_REMAINING_HOURS] = remaining_hours

            until_dt = dt_util.as_local(
                datetime.fromtimestamp(last_end, tz=dt_util.UTC)
            )
            attrs[ATTR_UNTIL] = until_dt.isoformat()
        else:
            attrs[ATTR_REMAINING_HOURS] = 0.0

        attrs["icon_color"] = _icon_color_for_level(level)

        return attrs


