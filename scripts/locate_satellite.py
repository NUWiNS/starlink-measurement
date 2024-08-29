import os.path

from skyfield.api import load, wgs84

from scripts.constants import DATASET_DIR

if __name__ == '__main__':
    filename = os.path.join(DATASET_DIR, 'others/gp.php')
    if os.path.exists(filename):
        satellites = load.tle_file(filename)
    else:
        stations_url = "https://celestrak.org/NORAD/elements/gp.php?GROUP=starlink&FORMAT=tle"
        satellites = load.tle_file(stations_url, filename=filename)

    print("Loaded", len(satellites), "satellites")
    by_name = {sat.name: sat for sat in satellites}
    satellite = by_name["STARLINK-1007"]

    # year, month, day, hour, minute, second
    ts = load.timescale()
    t = ts.now()
    a = satellite.at(t)
    lat, lon = wgs84.latlon_of(a)
    print("Latitude:", lat)
    print("Longitude:", lon)
