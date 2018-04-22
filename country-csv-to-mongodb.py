from pymongo import MongoClient
import csv

# database configurations
client = MongoClient("mongodb://localhost:27017")
db = client["geoData"]

def save_countries():
    with open('countries.csv') as csvfile:
        spamreader = csv.DictReader(csvfile)
        countries = []
        for row in spamreader:
            country = {
                "continent_name": row['continent_name'],
                'country_name': row['country_name'],
                'country_iso_code': row['country_iso_code'],
                'geoname_id': row['geoname_id']
            }
            countries.append(country)
        db.countries.insert_many(countries)

def save_continents():
    with open('countries.csv') as csvfile:
        spamreader = csv.DictReader(csvfile)
        continents = []
        for row in spamreader:
            if row['continent_name'] in continents:
                continue
            else:
                continet = {'continent_name' : row['continent_name']}
                continents.append(continet)
        db.continents.insert_many(continents)


save_countries()
save_continents()