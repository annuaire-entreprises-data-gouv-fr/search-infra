from functools import lru_cache

from pyproj import Transformer

from dag_datalake_sirene.helpers.utils import is_valid_number

# Mapping between department codes and EPSG codes
department_epsg_mapping = {
    "971": 5490,  # Guadeloupe
    "972": 5490,  # Martinique
    "973": 2972,  # Guyane
    "974": 2975,  # La Réunion
    "975": 4467,  # Saint-Pierre-et-Miquelon
    "976": 4471,  # Mayotte
    "977": 5490,  # Saint-Barthélemy
    "978": 5490,  # Saint-Martin
    "988": 3163,  # Nouvelle-Calédonie
}

# Default EPSG code for the rest of the departments in France
default_epsg = 2154


# Cache for transformers
@lru_cache(maxsize=None)
def get_transformer(epsg):
    return Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326")


# Function to perform the transformation
def transform_coordinates(department_code, x, y):
    if not is_valid_number(x) or not is_valid_number(y):
        return None, None
    if department_code in department_epsg_mapping:
        epsg = department_epsg_mapping[department_code]
    else:
        epsg = default_epsg
    transformer = get_transformer(epsg)
    lat, lon = transformer.transform(float(x), float(y))
    return str(lat), str(lon)
