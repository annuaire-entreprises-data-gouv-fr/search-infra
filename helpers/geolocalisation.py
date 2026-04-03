from functools import lru_cache

import numpy as np
import pandas as pd
import pyproj

from data_pipelines_annuaire.helpers.utils import is_valid_number

EPSG_BY_TERRITORY: dict[str, int] = {
    # La France européenne - RGF93 / Lambert-93
    "default": 2154,
    # Guadeloupe - RRAF 1991 / UTM zone 20N
    "971": 5490,
    # Martinique - RRAF 1991 / UTM zone 20N
    "972": 5490,
    # Guyane - RGFG95 / UTM zone 22N
    "973": 2972,
    # La Réunion - RGR92 / UTM zone 40S
    "974": 2975,
    # Saint-Pierre-et-Miquelon - RGSPM06 / UTM zone 21N
    "975": 4467,
    # Mayotte - RGM04 / UTM zone 38S
    "976": 4471,
    # Saint-Barthélemy - RRAF 1991 / UTM zone 20N
    "977": 5490,
    # Saint-Martin - RRAF 1991 / UTM zone 20N
    "978": 5490,
    # Terres australes et antartiques françaises (TAAF) - WGS 84 / UTM zone 42S (Kerguelen area)
    "984": 32742,  # Ou 4326 ? À tester.
    # Wallis-et-Futuna - WGS 84 / UTM zone 1S
    "986": 32701,  # Ou 8903 ? À tester.
    # Polynésie française - RGPF / UTM zone 6S
    "987": 3297,
    # Nouvelle-Calédonie - RGNC91-93 / Lambert Nouvelle-Calédonie
    "988": 3163,
    # Île de Clipperton - WGS 84 / World Geodetic System 1984, used in GPS
    "989": 4326,
}

GPS_EPSG = 4326

BOUNDING_BOXES: dict[str, tuple[float, float, float, float]] = {
    # (lat_min, lat_max, lon_min, lon_max)
    "default": (41.0, 51.5, -5.5, 10.0),  # La France européenne
    "971": (15.5, 16.7, -62.0, -60.5),  # Guadeloupe
    "972": (14.3, 15.0, -61.5, -60.5),  # Martinique
    "973": (2.0, 6.0, -55.0, -51.0),  # Guyane
    "974": (-21.5, -20.5, 55.0, 56.0),  # La Réunion
    "975": (46.5, 47.5, -56.5, -55.5),  # Saint-Pierre-et-Miquelon
    "976": (-13.5, -12.5, 44.5, 45.5),  # Mayotte
    "977": (17.85, 18.0, -63.0, -62.75),  # Saint-Barthélemy
    "978": (17.9, 18.2, -63.2, -62.9),  # Saint-Martin
    "986": (-15.0, -12.0, -179.0, -175.0),  # Wallis-et-Futuna
    "987": (-28.0, -7.0, -155.0, -130.0),  # Polynésie française
    "988": (-23.0, -19.0, 163.0, 169.0),  # Nouvelle-Calédonie
    "989": (10.1, 10.4, -109.4, -109.1),  # Île de Clipperton
}

# Terres australes et antartiques françaises
TAAF_BOUNDING_BOXES: list[tuple[float, float, float, float]] = [
    # (lat_min, lat_max, lon_min, lon_max)
    (-50.0, -48.5, 68.0, 71.0),  # Kerguelen
    (-47.0, -45.5, 50.0, 53.0),  # Crozet
    (-39.0, -37.0, 77.0, 78.0),  # Amsterdam et Saint-Paul
    (-23.0, -11.0, 39.0, 64.0),  # Îles Éparses - various locations in Indian Ocean
]


# Cache for transformers
@lru_cache(maxsize=None)
def get_transformer(epsg: int) -> pyproj.Transformer:
    crs_from = pyproj.CRS.from_epsg(epsg)
    crs_to = pyproj.CRS.from_epsg(GPS_EPSG)
    return pyproj.Transformer.from_crs(crs_from, crs_to, always_xy=True)


def convert_lambert_to_gps(
    x: float | None, y: float | None, epsg: int
) -> tuple[float | None, float | None]:
    if x is None or y is None or np.isnan(x) or np.isnan(y):
        return None, None

    try:
        transformer = get_transformer(epsg)
        lon, lat = transformer.transform(x, y)
        return lat, lon
    except Exception:
        return None, None


def is_inside_borders(
    lat: float | None, lon: float | None, departement: str | None
) -> bool | None:
    if lat is None or lon is None:
        return None

    if departement == "984":
        for lat_min, lat_max, lon_min, lon_max in TAAF_BOUNDING_BOXES:
            if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                return True
        return False

    if departement in BOUNDING_BOXES:
        lat_min, lat_max, lon_min, lon_max = BOUNDING_BOXES[departement]
    else:
        lat_min, lat_max, lon_min, lon_max = BOUNDING_BOXES["default"]

    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


def get_epsg_from_code(code_postal: str | None, code_commune: str | None) -> int | None:
    for code in [code_postal, code_commune]:
        if code is None:
            continue
        if code.startswith("97") or code.startswith("98") or code.startswith("99"):
            prefix = code[:3]
            if prefix in EPSG_BY_TERRITORY:
                return EPSG_BY_TERRITORY[prefix]
        else:
            return EPSG_BY_TERRITORY["default"]
    return None


def convert_dataframe_lambert_to_gps(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    code_postal_col: str,
    code_commune_col: str,
) -> pd.DataFrame:
    df = df.copy()

    def safe_float(val):
        if pd.isna(val) or val == "[ND]":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def convert_row(row):
        x = safe_float(row[x_col])
        y = safe_float(row[y_col])

        cp = str(row[code_postal_col]) if pd.notna(row[code_postal_col]) else None
        cc = str(row[code_commune_col]) if pd.notna(row[code_commune_col]) else None
        epsg = get_epsg_from_code(cp, cc) or 2154

        lat, lon = convert_lambert_to_gps(x, y, epsg)
        return pd.Series({"latitude": lat, "longitude": lon})

    coords = df.apply(convert_row, axis=1)
    df["latitude"] = coords["latitude"]
    df["longitude"] = coords["longitude"]

    return df


def transform_coordinates(department_code, x, y):
    if not is_valid_number(x) or not is_valid_number(y):
        return None, None

    epsg = get_epsg_from_code(department_code, None) or 2154
    lat, lon = convert_lambert_to_gps(float(x), float(y), epsg)

    # Return as string to maintain backward compatibility with the datagouv DAG
    return str(lat) if lat is not None else None, str(lon) if lon is not None else None
