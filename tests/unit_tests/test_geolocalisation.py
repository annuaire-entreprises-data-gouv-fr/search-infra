import numpy as np

from data_pipelines_annuaire.helpers.geolocalisation import (
    convert_lambert_to_gps,
    is_inside_borders,
)


class TestConvertLambertToGps:
    def test_valid_metropolitan_france_coordinates(self):
        # Paris area coordinates in Lambert-93 (EPSG 2154)
        x, y = 652469.0, 6862035.0
        lat, lon = convert_lambert_to_gps(x, y, epsg=2154)

        assert lat is not None
        assert lon is not None
        assert 48.8 < lat < 48.9  # Paris latitude ~48.85
        assert 2.3 < lon < 2.4  # Paris longitude ~2.35

    def test_valid_guadeloupe_coordinates(self):
        # Guadeloupe coordinates in RRAF 1991 / UTM zone 20N (EPSG 5490)
        x, y = 649000.0, 1774000.0
        lat, lon = convert_lambert_to_gps(x, y, epsg=5490)

        assert lat is not None
        assert lon is not None
        assert 15.5 < lat < 16.7  # Guadeloupe latitude range
        assert -62.0 < lon < -60.5  # Guadeloupe longitude range

    def test_valid_reunion_coordinates(self):
        # La Réunion coordinates in RGR92 / UTM zone 40S (EPSG 2975)
        x, y = 340000.0, 7660000.0
        lat, lon = convert_lambert_to_gps(x, y, epsg=2975)

        assert lat is not None
        assert lon is not None
        assert -21.5 < lat < -20.5  # Réunion latitude range
        assert 55.0 < lon < 56.0  # Réunion longitude range

    def test_none_x_returns_none(self):
        lat, lon = convert_lambert_to_gps(None, 6862035.0, epsg=2154)
        assert lat is None
        assert lon is None

    def test_none_y_returns_none(self):
        lat, lon = convert_lambert_to_gps(652469.0, None, epsg=2154)
        assert lat is None
        assert lon is None

    def test_nan_x_returns_none(self):
        lat, lon = convert_lambert_to_gps(np.nan, 6862035.0, epsg=2154)
        assert lat is None
        assert lon is None

    def test_nan_y_returns_none(self):
        lat, lon = convert_lambert_to_gps(652469.0, np.nan, epsg=2154)
        assert lat is None
        assert lon is None

    def test_both_none_returns_none(self):
        lat, lon = convert_lambert_to_gps(None, None, epsg=2154)
        assert lat is None
        assert lon is None


class TestIsInsideBorders:
    def test_point_inside_metropolitan_france(self):
        # Paris coordinates
        lat, lon = 48.8566, 2.3522
        result = is_inside_borders(lat, lon, departement=None)
        assert result is True

    def test_point_outside_metropolitan_france(self):
        # London coordinates
        lat, lon = 51.5074, -0.1278
        result = is_inside_borders(lat, lon, departement=None)
        assert result is False

    def test_point_inside_guadeloupe(self):
        # Pointe-à-Pitre coordinates
        lat, lon = 16.2411, -61.5331
        result = is_inside_borders(lat, lon, departement="971")
        assert result is True

    def test_point_outside_guadeloupe(self):
        # Paris coordinates with Guadeloupe department code
        lat, lon = 48.8566, 2.3522
        result = is_inside_borders(lat, lon, departement="971")
        assert result is False

    def test_point_inside_reunion(self):
        # Saint-Denis de La Réunion coordinates
        lat, lon = -20.8789, 55.4481
        result = is_inside_borders(lat, lon, departement="974")
        assert result is True

    def test_point_inside_mayotte(self):
        # Mamoudzou coordinates
        lat, lon = -12.7871, 45.2275
        result = is_inside_borders(lat, lon, departement="976")
        assert result is True

    def test_point_inside_taaf_kerguelen(self):
        # Kerguelen Islands coordinates
        lat, lon = -49.35, 69.5
        result = is_inside_borders(lat, lon, departement="984")
        assert result is True

    def test_point_inside_taaf_crozet(self):
        # Crozet Islands coordinates
        lat, lon = -46.4, 51.9
        result = is_inside_borders(lat, lon, departement="984")
        assert result is True

    def test_point_outside_taaf(self):
        # Paris coordinates with TAAF department code
        lat, lon = 48.8566, 2.3522
        result = is_inside_borders(lat, lon, departement="984")
        assert result is False

    def test_none_lat_returns_none(self):
        result = is_inside_borders(None, 2.3522, departement=None)
        assert result is None

    def test_none_lon_returns_none(self):
        result = is_inside_borders(48.8566, None, departement=None)
        assert result is None

    def test_unknown_department_uses_default(self):
        # Paris coordinates with unknown department - should use default (metropolitan France)
        lat, lon = 48.8566, 2.3522
        result = is_inside_borders(lat, lon, departement="99")
        assert result is True

    def test_point_inside_nouvelle_caledonie(self):
        # Nouméa coordinates
        lat, lon = -22.2758, 166.4580
        result = is_inside_borders(lat, lon, departement="988")
        assert result is True

    def test_point_inside_polynesie(self):
        # Papeete (Tahiti) coordinates
        lat, lon = -17.5516, -149.5585
        result = is_inside_borders(lat, lon, departement="987")
        assert result is True
