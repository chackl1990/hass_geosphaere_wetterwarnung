# WARNUNG
**Das Projekt ist nicht dazu geeignet und darf nicht dazu verwendet werden, Eigentum, Personen oder sonstiges zu schützen.**
Dieses Projekt wurde als reines Just-for-Fun-Projekt entwickelt. Es gibt keinerlei Garantie, keine Gewährleistung und keine Wartung. Für mich persönlich gilt zwar „besser als nichts“, aber nur in Kombination mit eigener, persönlicher Kontrolle vor Ort.

# Geosphaere Wetterwarnung (Home Assistant)
Custom Integration fuer Geosphere Austria (ZAMG) Wetterwarnungen in Oesterreich. Holt Warnungen per API fuer `zone.home` und optionale Zusatzkoordinaten, stellt Sensoren/Binary-Sensoren bereit und bietet Config-Flow sowie konfigurierbares Polling mit Grace-Handling.

## Features
- Warnungen fuer `zone.home` und optional weitere Koordinaten
- Binary Sensoren fuer Vorwarnung, aktuelle Warnung, API-Status und Warnungstypen
- Level-Sensoren je Warnungstyp (Wind, Regen, Schnee, Glatteis, Gewitter, Hitze, Kaelte)
- Konfigurierbarer Scan-Intervall und Grace-Period gegen Flattern
- Volle UI-Konfiguration (Config Flow + Optionen)

## Voraussetzungen
- Home Assistant mit definierter `zone.home` (Latitude/Longitude)
- Internetzugang zu `warnungen.zamg.at`

## Installation ueber HACS (Custom Repository)
<a href="https://my.home-assistant.io/redirect/hacs_repository/?owner=chackl1990&repository=hass_geosphaere_wetterwarnung" target="_blank">
    <img src="https://my.home-assistant.io/badges/hacs_repository.svg" alt="HACS Repository">
</a>


1. HACS > Integrations > Menu (drei Punkte) > Custom repositories
2. Repository-URL einfuegen (dieses GitHub-Repo), Kategorie: Integration
3. Integration "Geosphaere Wetterwarnung" installieren
4. Home Assistant neu starten

## Manuelle Installation
1. Ordner `custom_components/geosphaere_wetterwarnung` in dein `config/custom_components/` kopieren
2. Home Assistant neu starten

## Konfiguration
1. Einstellungen > Geraete & Dienste > Integration hinzufuegen
2. "Geosphaere Wetterwarnung" auswaehlen
3. Optionen:
   - Scan-Intervall (30-600 Sekunden)
   - Zusatzkoordinaten im Format `lat,lon;lat,lon` (Beispiel: `48.2082,16.3738;47.0707,15.4395`)
   - Grace-Period in Sekunden (z.B. 600)

## Entitaeten
### Binary Sensoren
- `Vorwarnung` (zukuenftige Warnungen)
- `Warnung` (aktuelle Summenwarnung)
- `Warnung API` (Fehler/Partial Failure beim API-Call)
- `Wind Warnung`, `Regen Warnung`, `Schnee Warnung`, `Glatteis Warnung`, `Gewitter Warnung`, `Hitze Warnung`, `Kaelte Warnung`

### Sensoren
- `... Warnungslevel` je Typ (Wert 0-3)
- Attribute: `Remaining Hours`, `until`, `icon_color`

## Hinweise
- Datenquelle: Geosphere Austria (ZAMG) Warn-API
- API-Key wird nicht benoetigt
