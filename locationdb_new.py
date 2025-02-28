import os
import mysql.connector
import folium
from flask import Flask, render_template
from math import radians, sin, cos, sqrt, atan2
import googlemaps
import time
from collections import defaultdict

# Database configuration
config_nsmm_trans = {
    "host": "103.211.36.126",
    "user": "gramthejus",
    "password": "gtfm123",
    "database": "NSMM_TRANS"
}

# Google Maps API Key
GOOGLE_API_KEY = "AIzaSyBpF_YKl7yap8SZSUUvTNEco9PtVMXugiU"

# Initialize Google Maps client
gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

# Folium-compatible colors for APN types
APN_COLORS = {
    "Idea IOT": "orange",
    "Idea": "black",
    "Jio": "blue",
    "Airtel": "red",
    "BSNL": "purple",
    "Airtel IOT": "green",
    "default": "gray"  # Default color for unrecognized APN types
}

# Haversine formula to calculate distance
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth's radius in kilometers
    lat1_rad, lon1_rad = radians(lat1), radians(lon1)
    lat2_rad, lon2_rad = radians(lat2), radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c * 1000  # Convert to meters

# Reverse geocoding to get address using Google Maps client
def get_address(lat, lon):
    try:
        result = gmaps.reverse_geocode((lat, lon))
        if result and len(result) > 0:
            return result[0]["formatted_address"]
        else:
            return "Address not found"
    except Exception as e:
        return f"Error: {e}"

# Group locations by proximity
def group_locations_by_proximity(locations, max_distance_km=30):
    groups = []
    visited = set()

    while len(visited) < len(locations):
        current_group = []
        to_visit = [loc for idx, loc in enumerate(locations) if idx not in visited]

        current_location = to_visit.pop(0)
        current_group.append(current_location)
        visited.add(locations.index(current_location))

        i = 0
        while i < len(to_visit):
            loc = to_visit[i]
            if all(
                haversine(loc['latitude'], loc['longitude'], group_loc['latitude'], group_loc['longitude']) <= max_distance_km * 1000
                for group_loc in current_group
            ):
                current_group.append(loc)
                visited.add(locations.index(loc))
                to_visit.pop(i)
            else:
                i += 1

        if len(current_group) > 1:
            groups.append(current_group)

    return groups

# Analyze signal strength by APN type
def analyze_signal_strength_by_apn_type(group):
    analysis = defaultdict(list)
    for loc in group:
        apn_type = loc['apn_type']
        signal_strength = loc['signal_strength']
        try:
            signal_strength = float(signal_strength)
            analysis[apn_type].append(signal_strength)
        except (ValueError, TypeError):
            print(f"Skipping invalid signal_strength: {signal_strength}")

    avg_signal_strength = {
        apn_type: sum(signals) / len(signals) if signals else 0
        for apn_type, signals in analysis.items()
    }
    return avg_signal_strength

# Fetch and process data
def fetch_coordinates_and_group():
    try:
        conn = mysql.connector.connect(**config_nsmm_trans)
        cursor = conn.cursor()

        cursor.execute("SELECT device_id, latitude, longitude, signal_strength, apn_type FROM specific_device_features LIMIT 1029;")
        rows = cursor.fetchall()

        if not rows:
            print("No data returned from the database!")
            return []

        locations = []
        invalid_data_count = 0

        for row in rows:
            device_id, latitude, longitude, signal_strength, apn_type = row
            try:
                if latitude is None or longitude is None:
                    raise ValueError("Latitude or Longitude is None")
                latitude = float(latitude)
                longitude = float(longitude)
                address = get_address(latitude, longitude)
                locations.append({
                    'device_id': device_id,
                    'latitude': latitude,
                    'longitude': longitude,
                    'address': address,
                    'signal_strength': signal_strength,
                    'apn_type': apn_type
                })
                time.sleep(0.1)
            except (ValueError, TypeError):
                invalid_data_count += 1
                print(f"Skipping invalid data: Device ID={device_id}, Latitude={latitude}, Longitude={longitude}")

        print(f"Total valid locations processed: {len(locations)}")
        print(f"Total invalid data skipped: {invalid_data_count}")

        groups = group_locations_by_proximity(locations, max_distance_km=30)
        return groups
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return []
    finally:
        if conn:
            conn.close()

# Flask setup
app = Flask(__name__)

@app.route('/')
def map_view():
    groups = fetch_coordinates_and_group()
    m = folium.Map(location=[17.450636, 78.387154], zoom_start=13, tiles=None)

    folium.TileLayer(
        tiles=f"https://mt1.google.com/vt/lyrs=m&x={{x}}&y={{y}}&z={{z}}&key={GOOGLE_API_KEY}",
        attr="Google Maps",
        name="Google Maps",
        overlay=False,
        control=True
    ).add_to(m)

    for group_index, group in enumerate(groups, start=1):
        cluster_latitudes = [loc['latitude'] for loc in group]
        cluster_longitudes = [loc['longitude'] for loc in group]
        cluster_center = [sum(cluster_latitudes)/len(cluster_latitudes), sum(cluster_longitudes)/len(cluster_longitudes)]

        max_radius = max(haversine(cluster_center[0], cluster_center[1], loc['latitude'], loc['longitude']) for loc in group)

        folium.Circle(
            location=cluster_center,
            radius=max_radius,
            color="black",
            fill=True,
            fill_opacity=0.2
        ).add_to(m)

        for loc in group:
            apn_type = loc['apn_type']
            color = APN_COLORS.get(apn_type, APN_COLORS["default"])
            folium.Marker(
                location=[loc['latitude'], loc['longitude']],
                popup=f"{loc['device_id']}<br>{loc['address']}<br>Signal: {loc['signal_strength']}<br>APN Type: {apn_type}",
                icon=folium.Icon(color=color, icon='info-sign')
            ).add_to(m)

    templates_dir = "templates"
    if not os.path.exists(templates_dir):
        os.makedirs(templates_dir)

    map_html = os.path.join(templates_dir, 'map.html')
    m.save(map_html)

    return render_template('map.html')

if __name__ == "__main__":
    app.run(debug=True)
