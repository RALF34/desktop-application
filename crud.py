import time
from datetime import date, datetime, timedelta
from statistics import mean

from bson.code import Code
from pandas import read_csv, read_excel
from pymongo import MongoClient

from .dictionaries import french_departments

mongoClient = MongoClient("mongodb://localhost:27017")

stringToDatetime = lambda x: datetime.strptime(x,"%Y/%m/%d %H:%M:%S")

updateList = Code('''
function (history, data_45_days_ago, data_yesterday){
  if (!data_45_days_ago && !data_yesterday){
    return history;
  } else if (data_45_days_ago && !data_yesterday){
    return history.slice(1);
  } else {
    const new_date = data_yesterday.date
    const new_value = data_yesterday.value
    history.dates.push(new_date);
    history.values.push(new_value);
    if (data_45_days_ago){
      history = history.slice(1)
    }
    return history;
  }
}
''')

def update_database():
    mongoClient.drop_database("air_quality")
    database = mongoClient["air_quality"]
    if "last_update" not in database.list_collection_names():
        # I create the "LCSQA_stations" (line ) collection using the "f" file.
        url = "https://www.lcsqa.org/system/files/media/documents/"+\
        "Liste points de mesures 2021 pour site LCSQA_27072022.xlsx"
        data = read_excel(url.replace(" ", "%20"), sheet_name=1)
        c = data.columns.tolist()
        columns_to_remove = c[3:7]+c[10:]
        labels = data.iloc[1].tolist()
        labels_to_keep = labels[:3]+labels[7:10]
        data = data.drop(
            columns=columns_to_remove
        ).drop(
            [0,1]
        ).set_axis(
            labels_to_keep,
            axis="columns")
        get_department = lambda x: (
            french_departments[x[:2]] if not(x[1].isdigit()) or int(x[:2]) < 97
            else french_departments[x[:3]]
        )
        data["Département"] = data["Code commune"].apply(
            get_department)
        data = data[
            ["Région",
            "Département",
            "Commune",
            "Nom station",
            "Code station",
            "Secteur de la station"]]
        database["LCSQA_stations"].insert_many(data.to_dict("records"))
        database["LCSQA_stations"].aggregate([
            {"$set":
                {"station": {"name": "$Nom station",
                             "code": "$Code station",
                            "zone": "$Secteur de la station"}}},
            {"$group":
                {"_id": "$Commune",
                 "stations": {"$push": "$station"}}},
            {"$out": "cities"}])
        database["LCSQA_stations"].aggregate([
            {"$group":
                {"_id": "$Département",
                 "cities": {"$push": "$Commune"}}},
            {"$out": "departments"}])
        database["LCSQA_stations"].aggregate([
            {"$group":
                {"_id": "$Région",
                 "departments": {"$push": "$Département"}}},
            {"$out": "regions"}])
        
        # I create the "distribution_pollutants" (line ) and "LCSQA_data" (line ) collections
        # using pollution data collected over the last 45 days (line  to ).
        DATE = date.today()-timedelta(days=45)
        while DATE < date.today():
            url = "https://files.data.gouv.fr/lcsqa/concentrations-de"+\
            "-polluants-atmospheriques-reglementes/temps-reel/"+\
            str(DATE.year)+"/FR_E2_"+DATE.isoformat()+".csv"
            data = read_csv(url, sep=";")
            data = data[data["validité"]==1]
            data["pollutant_to_ignore"] = data["Polluant"].apply(
                lambda x: x in ["NO","NOX as NO2"])
            data = data[data["pollutant_to_ignore"]==False]
            records = data.to_dict("records")
            for r in records:
                r["date"] = stringToDatetime(r["Date de début"])
                r.pop("Date de début")
                r["hour"] = r["date"].hour
            database["LCSQA_data"].insert_many(records)
            DATE += timedelta(days=1)
        database["LCSQA_data"].aggregate([
            {"$group":
                {"_id": "$code site",
                 "monitored_pollutants":
                    {"$push": "$Polluant"}}},
            {"$out": "distribution_pollutants"}])
        database["LCSQA_data"].aggregate([
            {"$group":
                {"_id": {"station": "$code station",
                         "pollutant": "$Polluant",
                         "hour": "$hour"},
                 "values": {"$push": "$valeur brute"},
                 "dates": {"$push": "$date"}}},
            {"$project":
                {"history": {"values": "$values",
                             "dates": "dates"}}},
            {"$out": "LCSQA_data"}])
        # Since the present application will be deployed using Docker containers, we     
        # can't update the history of data in a continuous way. We have to keep track of
        # the date when the last update occured, in order to know how many days we will
        # have to add to the history when performing the next update.
        # So I store this information (given by the "DATE" variable) in a new collection.
        database["last_update"].insert_one(
            {"date": datetime(DATE.year, DATE.month, DATE.day)-timedelta(days=1)})
    else:
        # I retrieve the date when the last update occured.
        last_update = database["last_update"].find_one()["date"]
        # For all the days between the last update and the current date, we add the
        # corresponding pollution data to the "LCSQA" collection (line ) and we
        # update the "history" field accordingly (line ).
        following_date = last_update.date() + timedelta(days=1)
        while following_date < date.today() - timedelta(days=45):
            following_date += timedelta(days=1)
        while following_date < date.today():
            url = "https://files.data.gouv.fr/lcsqa/concentrations-de"+\
            "-polluants-atmospheriques-reglementes/temps-reel/"+\
            str(following_date.year)+"/FR_E2_"+following_date.isoformat()+".csv"
            new_records = read_csv(url, sep=";").to_dict("records")
            for r in new_records:
                r["dateTime"] = stringToDatetime(r["Date de début"])
                r["date"] = r["dateTime"].date()
                r["hour"] = r["dateTime"].hour
                r.pop("Date de début")
            database["new_data"].insert_many(new_records)
            database["new_data"].aggregate([
                {"$match": {"validité": 1}},
                {"$project": {"_id": {"station": "$code station",
                                      "pollutant": "$Polluant",
                                      "hour": "$hour"},
                              "history": {"date": "$date",
                                          "value": "$valeur brute"}}}])
            database["LCSQA_data"].aggregate([
                {"$lookup": 
                    {"from": "new_data",
                     "localField": "_id",
                     "foreignField": "_id",
                     "as": "new_data"}},
                {"$addFields":
                    {"history":
                        {"$function": 
                            {"body": updateList,
                             "args": ["$history",
                                      "$data_45_days_ago",
                                      "$new_data"],
                             "lang": "js"}},
                     "data_45_days_ago":
                        {"$eq": ["history.dates.$",
                                 date.today()-timedelta(days=45)]}}},
                {"$out": "LCSQA_data"}])
            database.drop_collection("new_data")
            following_date += timedelta(days=1)
        database["last_update"].replace_one(
            {"date": last_update},
            {"date": datetime(
                following_date.year,
                following_date.month,
                following_date.day)-timedelta(days=1)})

def history_is_updated():
    database = mongoClient["air quality"]
    DATE = date.today()
    return database["last_update"].find_one()["date"] == \
    datetime(DATE.year, DATE.month, DATE.day) - timedelta(days=1)

def get_response(station, pollutant, n_days):
    database = mongoClient["air_quality"]
    averages = {str(x): 0 for x in range(24)}
    if n_days:
        data = database["LCSQA_data"].find(
		    {"_id.station": station,
		    "_id.pollutant": pollutant})
        for document in data:
            hour = document["_id"]["hour"]
            history = list(zip(
                document["history"]["values"],
                document["history"]["dates"]
            )).reversed()
            i = 0
            while date.today()-history[i][1] <= timedelta(days=n_days):
                i += 1
            averages[str(hour)] = mean([e[0] for e in history[:i]])
    return averages
