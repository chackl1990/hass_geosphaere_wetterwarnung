DOMAIN = "geosphere_wetterwarnung"

CONF_SCAN_INTERVAL = "scan_interval"
CONF_EXTRA_COORDS = "extra_coords"
CONF_GRACE_PERIOD = "grace_period"
DEFAULT_SCAN_INTERVAL = 60  # Sekunden
DEFAULT_EXTRA_COORDS = ""
DEFAULT_GRACE_PERIOD = 600

MIN_SCAN_INTERVAL = 30
MAX_SCAN_INTERVAL = 600
STEP_SCAN_INTERVAL = 30

MIN_GRACE_PERIOD = 0
MAX_GRACE_PERIOD = 3600
STEP_GRACE_PERIOD = 60

# Namen für die Anzeige
WARNING_TYPES = {
    0: "Keine",
    1: "Wind",
    2: "Regen",
    3: "Schnee",
    4: "Glatteis",
    5: "Gewitter",
    6: "Hitze",
    7: "Kälte",
}

# Level-Sensor-Attribute
ATTR_REMAINING_HOURS = "Remaining Hours"
ATTR_UNTIL = "until"

# Binary-Sensor-Attribute
ATTR_FIRST_START = "first_start"

# Vorwarnung / Warnung Attribute (deutsche Namen wie gewünscht)
ATTR_VORWARNUNG_DATEN = "Vorwarnung Daten"
ATTR_VORWARNUNG_TEXT = "Vorwarnung Text"
ATTR_WARNUNG_DATEN = "Warnung Daten"
ATTR_WARNUNG_TEXT = "Warnung Text"

# API-Status
ATTR_HTTP_CODE = "http_code"
ATTR_HTTP_RESPONSE = "http_response"
ATTR_LAST_REQUEST = "last_request"

