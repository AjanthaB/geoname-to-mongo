from pymongo import MongoClient
import csv

# database configurations
client = MongoClient("mongodb://localhost:27017")
db = client["geoData"]


def save_cities():
    with open('cities.csv') as csvfile:
        spamreader = csv.DictReader(csvfile)
        cities = []
        for row in spamreader:
            city = {
                "continent_name": row['continent_name'],
                'country_name': row['country_name'],
                'subdivision_1': row['subdivision_1_name'],
                'subdivision_2': row['subdivision_2_name'],
                'city_name': row['city_name']
            }
            cities.append(city)
        db.cities.insert_many(cities)

save_cities()