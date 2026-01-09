from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from homeassistant.components.binary_sensor import (
    BinarySensorEntity,
    BinarySensorDeviceClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import (
    DOMAIN,
    WARNING_TYPES,
    ATTR_FIRST_START,
    ATTR_VORWARNUNG_DATEN,
    ATTR_VORWARNUNG_TEXT,
    ATTR_WARNUNG_DATEN,
    ATTR_WARNUNG_TEXT,
    ATTR_HTTP_CODE,
    ATTR_HTTP_RESPONSE,
    ATTR_LAST_REQUEST,
)
from .coordinator import geosphereCoordinator



async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    data = hass.data[DOMAIN][entry.entry_id]
    coordinator: geosphereCoordinator = data["coordinator"]

    entities: list[BinarySensorEntity] = []

    # Reihenfolge:
    # 1. Vorwarnung
    entities.append(
        UpcomingSummaryBinarySensor(coordinator=coordinator, entry_id=entry.entry_id)
    )

    # 2. Warnung (aktuelle Summenwarnung)
    entities.append(
        CurrentSummaryBinarySensor(coordinator=coordinator, entry_id=entry.entry_id)
    )

    # 3. API-Warnung
    entities.append(
        ApiStatusBinarySensor(coordinator=coordinator, entry_id=entry.entry_id)
    )

    # 4..: Typ-Binary-Sensoren Wind/Regen/... in WTYPE-Reihenfolge 1..7
    for wtype in range(1, 8):
        typename = WARNING_TYPES.get(wtype)
        if typename is None:
            continue
        entities.append(
            WarningTypeBinarySensor(
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


def _first_start(warnings: list[dict[str, Any]]) -> int | None:
    first: int | None = None
    for w in warnings:
        raw = w.get("properties", {}).get("rawinfo", {})
        try:
            start = int(raw.get("start", 0))
        except (TypeError, ValueError):
            continue
        if first is None or start < first:
            first = start
    return first


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


def _group_by_type_with_max_level(
    warnings: list[dict[str, Any]]
) -> Dict[int, Dict[str, Any]]:
    result: Dict[int, Dict[str, Any]] = {}
    for w in warnings:
        props = w.get("properties", {})
        raw = props.get("rawinfo", {})
        try:
            wtype = int(raw.get("wtype", 0))
            level = int(raw.get("wlevel", 0))
        except (TypeError, ValueError):
            continue

        if wtype == 0:
            continue

        entry = result.get(wtype)
        if entry is None or level > entry["level"]:
            result[wtype] = {
                "level": level,
                "text": props.get("text", ""),
                "start": int(raw.get("start", 0)),
                "end": int(raw.get("end", 0)),
            }
    return result


def _build_summary_lines(grouped: Dict[int, Dict[str, Any]]) -> List[str]:
    items = [
        (wtype, data["level"], data.get("text", ""))
        for wtype, data in grouped.items()
    ]
    items.sort(key=lambda x: x[1], reverse=True)

    lines: List[str] = []
    for wtype, level, text in items:
        typename = WARNING_TYPES.get(wtype, str(wtype))
        if text:
            lines.append(f"Level {level}: {typename} – {text}")
        else:
            lines.append(f"Level {level}: {typename}")
    return lines


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


class WarningTypeBinarySensor(CoordinatorEntity, BinarySensorEntity):
    """Binary sensor Ein/Aus je Warnungstyp."""

    _attr_has_entity_name = True
    _attr_device_class = BinarySensorDeviceClass.SAFETY

    def __init__(self, coordinator: geosphereCoordinator, entry_id: str, wtype: int):
        super().__init__(coordinator)
        self._entry_id = entry_id
        self._wtype = wtype

        typename = WARNING_TYPES.get(wtype, f"Typ {wtype}")
        self._attr_unique_id = f"{entry_id}_wtype_{wtype}"
        self._attr_name = f"{typename} Warnung"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry_id)},
            name="GeoSphere Wetterwarnungen",
            manufacturer="ZAMG / Geosphere Austria",
        )

    @property
    def icon(self) -> str:
        return _icon_for_type(self._wtype) if self.is_on else "mdi:check-circle"

    @property
    def is_on(self) -> bool:
        data = self.coordinator.data or {}
        active, _ = _split_warnings_by_time(data)
        active_for_type = _filter_by_type(active, self._wtype)
        return len(active_for_type) > 0

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        data = self.coordinator.data or {}
        active, future = _split_warnings_by_time(data)

        relevant: list[dict[str, Any]] = []
        relevant.extend(_filter_by_type(active, self._wtype))
        relevant.extend(_filter_by_type(future, self._wtype))

        attrs: dict[str, Any] = {}
        first_start = _first_start(relevant)

        if first_start is not None:
            dt = dt_util.as_local(
                datetime.fromtimestamp(first_start, tz=dt_util.UTC)
            )
            attrs[ATTR_FIRST_START] = dt.isoformat()

        # Level-Attribut: höchstes Level aus allen relevanten Warnungen (aktuell + zukünftig)
        attrs["Level"] = _highest_level(relevant) if relevant else 0

        attrs["icon_color"] = "red" if self.is_on else "green"

        return attrs


class CurrentSummaryBinarySensor(CoordinatorEntity, BinarySensorEntity):
    """Summen-Warnung aktuell -> Name: Warnung."""

    _attr_has_entity_name = True
    _attr_device_class = BinarySensorDeviceClass.SAFETY

    def __init__(self, coordinator: geosphereCoordinator, entry_id: str):
        super().__init__(coordinator)
        self._entry_id = entry_id
        self._attr_unique_id = f"{entry_id}_warnung_aktuell"
        self._attr_name = "Warnung"
        self._last_is_on: bool | None = None
        self._last_level: int | None = None

    def _handle_coordinator_update(self) -> None:
        data = self.coordinator.data or {}
        active, _ = _split_warnings_by_time(data)
        grouped = _group_by_type_with_max_level(active)
        is_on = len(active) > 0
        level = max((info["level"] for info in grouped.values()), default=0)

        self._last_is_on = is_on
        self._last_level = level
        super()._handle_coordinator_update()

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry_id)},
            name="GeoSphere Wetterwarnungen",
            manufacturer="ZAMG / Geosphere Austria",
        )

    @property
    def is_on(self) -> bool:
        data = self.coordinator.data or {}
        active, _ = _split_warnings_by_time(data)
        return len(active) > 0

    @property
    def icon(self) -> str:
        return "mdi:alert" if self.is_on else "mdi:check-circle"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        data = self.coordinator.data or {}
        active, _ = _split_warnings_by_time(data)

        grouped = _group_by_type_with_max_level(active)
        items = _build_summary_lines(grouped)

        active_list: List[Dict[str, Any]] = []
        for wtype, info in grouped.items():
            typename = WARNING_TYPES.get(wtype, str(wtype))
            start_dt = dt_util.as_local(
                datetime.fromtimestamp(info["start"], tz=dt_util.UTC)
            )
            end_dt = dt_util.as_local(
                datetime.fromtimestamp(info["end"], tz=dt_util.UTC)
            )
            active_list.append(
                {
                    "type": typename,
                    "level": info["level"],
                    "text": info.get("text", ""),
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                }
            )

        # Level: höchstes Level aller aktiven Warnungen, oder 0
        if grouped:
            max_level = max(info["level"] for info in grouped.values())
            text = "\n".join(items)
        else:
            max_level = 0
            text = "Keine"

        attrs: dict[str, Any] = {
            ATTR_WARNUNG_DATEN: active_list,
            ATTR_WARNUNG_TEXT: text,
            "Level": max_level,
            "icon_color": "red" if self.is_on else "green",
        }

        return attrs


class UpcomingSummaryBinarySensor(CoordinatorEntity, BinarySensorEntity):
    """Vorwarnung, sobald eine Warnung bekannt ist (zukünftige Warnungen)."""

    _attr_has_entity_name = True
    _attr_device_class = BinarySensorDeviceClass.SAFETY

    def __init__(self, coordinator: geosphereCoordinator, entry_id: str):
        super().__init__(coordinator)
        self._entry_id = entry_id
        self._attr_unique_id = f"{entry_id}_vorwarnung"
        self._attr_name = "Vorwarnung"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry_id)},
            name="GeoSphere Wetterwarnungen",
            manufacturer="ZAMG / Geosphere Austria",
        )

    @property
    def is_on(self) -> bool:
        data = self.coordinator.data or {}
        _, future = _split_warnings_by_time(data)
        return len(future) > 0

    @property
    def icon(self) -> str:
        return "mdi:alert-outline" if self.is_on else "mdi:check-circle"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        data = self.coordinator.data or {}
        _, future = _split_warnings_by_time(data)

        grouped = _group_by_type_with_max_level(future)
        items = _build_summary_lines(grouped)

        future_list: List[Dict[str, Any]] = []
        for wtype, info in grouped.items():
            typename = WARNING_TYPES.get(wtype, str(wtype))
            start_dt = dt_util.as_local(
                datetime.fromtimestamp(info["start"], tz=dt_util.UTC)
            )
            end_dt = dt_util.as_local(
                datetime.fromtimestamp(info["end"], tz=dt_util.UTC)
            )
            future_list.append(
                {
                    "type": typename,
                    "level": info["level"],
                    "text": info.get("text", ""),
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                }
            )

        if grouped:
            max_level = max(info["level"] for info in grouped.values())
            text = "\n".join(items)
        else:
            max_level = 0
            text = "Keine"

        attrs: dict[str, Any] = {
            ATTR_VORWARNUNG_DATEN: future_list,
            ATTR_VORWARNUNG_TEXT: text,
            "Level": max_level,
            "icon_color": "red" if self.is_on else "green",
        }

        return attrs


class ApiStatusBinarySensor(CoordinatorEntity, BinarySensorEntity):
    """Warnung API – zeigt Fehler beim letzten API-Call."""

    _attr_has_entity_name = True
    _attr_device_class = BinarySensorDeviceClass.PROBLEM

    def __init__(self, coordinator: geosphereCoordinator, entry_id: str):
        super().__init__(coordinator)
        self._entry_id = entry_id
        self._attr_unique_id = f"{entry_id}_api_status"
        self._attr_name = "Warnung API"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry_id)},
            name="GeoSphere Wetterwarnungen",
            manufacturer="ZAMG / Geosphere Austria",
        )

    @property
    def is_on(self) -> bool:
        return (not self.coordinator.last_update_success) or (
            getattr(self.coordinator, "had_partial_failure", False)
        )

    @property
    def icon(self) -> str:
        return "mdi:cloud-alert" if self.is_on else "mdi:cloud-check-variant"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        attrs: dict[str, Any] = {
            ATTR_HTTP_CODE: self.coordinator.last_http_status,
        }

        # Zeitpunkt des letzten API-Requests (lokale Zeit, ISO)
        if getattr(self.coordinator, "last_request_utc", None):
            last_local = dt_util.as_local(self.coordinator.last_request_utc)
            attrs[ATTR_LAST_REQUEST] = last_local.isoformat()

        # Http Response bei Fehlern/Partial Failures
        if (
            getattr(self.coordinator, "had_partial_failure", False)
            or (
                self.coordinator.last_http_status is not None
                and self.coordinator.last_http_status != 200
            )
        ):
            attrs[ATTR_HTTP_RESPONSE] = self.coordinator.last_http_response

        attrs["icon_color"] = "red" if self.is_on else "green"
        return attrs


