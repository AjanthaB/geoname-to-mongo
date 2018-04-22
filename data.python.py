from pymongo import MongoClient
import requests
import time
import sys

URL = "http://www.geonames.org/childrenJSON"
client = MongoClient("mongodb://localhost:27017")
db = client["geoData"]
PARAMS = {"geonameId": "6295630", "username": "ajanthab"}


def mapToCOntinet(continet):
    return {
        "name": continet["name"],
        "geonameId": continet["geonameId"],
        "lat": continet["lat"],
        "lng": continet["lng"],
        "type": "continent"
    }

def save_continets_to_db():
    r = requests.get(url=URL, params=PARAMS)
    data = r.json()
    geo_names = data['geonames']
    mapedContinents = []

    for continet in geo_names:
        mapedContinents.append(mapToCOntinet(continet))

    db.continents.insert_many(mapedContinents)

def map_to_country(country, continetId):
    return {
        "name": country["name"],
        "geonameId": country["geonameId"],
        "lat": country["lat"],
        "lng": country["lng"],
        "type": "country",
        "continetId": continetId
    }


def get_countries_by_geonameId(geonameId):
    PARAMS["geonameId"] = geonameId;
    r = requests.get(url = URL, params = PARAMS)
    data = r.json()
    return data['geonames']

def save_countries_to_db(geonameId):
    countries = get_countries_by_geonameId(geonameId)
    maped_countries = []

    for country in countries:
        maped_countries.append(map_to_country(country, geonameId))

    # print maped_countries
    db.countries.insert_many(maped_countries)


def get_continents_from_db():
    continents =  db.continents.find()

    for continent in continents:
        print continent['geonameId']
        save_countries_to_db(continent['geonameId'])

    print "all countries saved"


def map_to_stare(state, countryId):
    return {
        "name": state["name"],
        "geonameId": state["geonameId"],
        "lat": state["lat"],
        "lng": state["lng"],
        "type": "state",
        "continetId": countryId
    }


def get_states_by_geonameId(geonameId):
    PARAMS["geonameId"] = geonameId;
    try:
        r = requests.get(url = URL, params = PARAMS)
        data = r.json()
        if 'geonames' in data:
            return data['geonames']
        else:
            print "geonames does not exist in the response"
            print data
    except requests.exceptions.HTTPError as err:
        print err;
        sys.exit(1)

def save_states_to_db(geonameId):
    states = get_states_by_geonameId(geonameId)
    # maped_states = []

    if (isinstance(states, list)):
        for state in states:
            # maped_states.append(map_to_stare(state, geonameId))
            db.states.insert_one(map_to_stare(state, geonameId))

        print geonameId
        print "done "
    else:
      print "not done"
      print states;



def get_countries_from_db():
    countries =  db.countries.find()

    for country in countries:
        time.sleep(5)
        print country
        if 'geonameId' in country:
            save_states_to_db(country['geonameId'])
        else:
            print "no id"
            print country

    print "all states saved"

def run_the_script():
    # save_continets_to_db()
    get_continents_from_db()
    # save_countries_to_db(6255147)
    get_countries_from_db()


run_the_script()