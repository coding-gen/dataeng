#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
features = []
# bridge_crossing.tsv
# 65_out_rush_hour_fri.tsv
# 3_65_out_sun_am.tsv
# 4_long_trip.tsv
# 5a_large_trip.tsv
# 5b_small_trip.tsv
# short_routes.tsv
# slow_trips.tsv
# long_route.tsv
# 171076865.tsv
# 170817612.tsv
# 170872807.tsv
# short_trips.tsv
with open('short_trips.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    for line in data[1:len(data)-1]:
        line.strip()
        row = line.split("\\t")
        
        # skip the rows where speed is missing
        print(row)
        x = row[0]
        y = row[1]
        speed = row[2]
        if speed is None or speed == "":
            continue
     
        try:
            latitude, longitude = map(float, (y, x))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open("data.geojson", "w") as f:
    f.write('%s' % collection)