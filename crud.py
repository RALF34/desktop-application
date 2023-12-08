from datetime import date, datetime, timedelta
from statistics import mean

from bson.code import Code
from pandas import read_csv, read_excel
from pymongo import MongoClient

from dictionaries import french_departments

mongoClient = MongoClient("mongodb://db:27017")

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

def initialize_database():
    database = mongoClient["air_quality"]
    DATE = date.today()
    k = 1
    while k <= 45:
        DATE -= timedelta(days=k)
        url = "https://files.data.gouv.fr/lcsqa/concentrations-de"+\
              "-polluants-atmospheriques-reglementes/temps-reel/"+\
              str(DATE.year)+"/FR_E2_"+DATE.isoformat()+".csv"
        data = read_csv(url, sep=";")
        records = data[data["validité"]==1].to_dict("records")
        for r in records:
            r["date"] = stringToDatetime(r["Date de début"])
            r.pop("Date de début")
            r["hour"] = r["date"].hour
        database["LCSQA_data"].insert_many(records)
        k += 1
    database["LCSQA_data"].aggregate([
        {"$project":
            {"station": "$nom site",
             "pollutant": "$Pollutant"}},
        {"$group":
            {"_id": "$station",
             "monitored_pollutants":
                {"$push": "$pollutant"}}},
        {"$out": "distribution_pollutants"}
    ])
    database["LCSQA_data"].aggregate([
        {"$group": 
			{"_id": 
				{"station": "$nom site",
				 "pollutant": "$Polluant",
				 "hour": "$hour"},
			 "history":
			 	{"values":
					{"$push": "$valeur brute"},
			 	 "dates":
			 		{"push": "$date"}}}},
		{"$out": "LCSQA_data"}])
	file = "Liste points de mesures 2020 pour site LCSQA_221292021.xlsx"
	data = read_excel(
		file,
		sheet_name=1
	).drop(
		columns=data.columns.tolist()[15:],
		inplace=True
	)
	labels = data.iloc[1].tolist()[:3] + [" "] + data.iloc[1].tolist()[3:-1]
	data.set_axis(
		labels,
		axis="columns",
		copy=False
	).drop(
		[0,1],
		inplace=True
	).drop(
		columns=[" "]
	)
	data["Département"] = data["Code commune"].apply(
        lambda x: french_departments[x[:2]]
    )
	data = data[["Région","Département","Commune","Nom station"]]
    database["LCSQA_stations"].insert_many(
        data.to_dict("records")
    ).aggregate([
        {"$group":
            {"_id": "$Commune",
             "stations": 
                {"city": "$Commune"
                 "names": {"$push": "$Nom station"}},
             "department": "$Département",
             "region": "$Région"}},
        {"$out": "LCSQA_stations"}
    ])
    database["LCSQA_stations"].aggregate([
        {"$group":
            {"_id": "$department",
             "cities": 
                {"department": "$department"
                 "names": {"$push": "$stations"}},
             "region": "$region"}},
        {"$out": "LCSQA_stations"}
    ])
    database["LCSQA_stations"].aggregate([
        {"$group":
            {"_id": "$region",
             "departments":
                {"$push": "$cities"}}},
        {"$out": "LCSQA_stations"}
    ])

def update():
    yesterday = date.today() - timedelta(days=1)
    url = "https://files.data.gouv.fr/lcsqa/concentrations-de"+\
          "-polluants-atmospheriques-reglementes/temps-reel/"+\
          str(yesterday.year)+"/FR_E2_"+yesterday.isoformat()+".csv"
    new_data = read_csv(url,sep=";")
    new_records = new_data.to_dict("records")
    for r in new_records:
        r["dateTime"] = stringToDatetime(r["Date de début"])
		r["date"] = r["dateTime"].date()
		r["hour"] = r["dateTime"].hour
		r.pop("Date de début")
    database["data_yesterday"].insert_many(new_records)
    database["data_yesterday"].aggregate([
        {"$match": {"validité": 1}},
        {"$project": {"_id": {"station": "$code station",
                              "pollutant": "$Polluant"
                              "hour": "$hour"},
                      "history": {"date": "$date",
                      "value": "$valeur brute"}}}
    ])

    database["LCSQA_data"].aggregate([
        {"$lookup": 
            {"from": "data_yesterday",
             "localField": "_id",
             "foreignField": "_id",
             "as": "data_yesterday"}},
        {"$addFields": 
            {"history":
                {"$function": 
                    {"body": updateList,
                     "args": ["$history",
                              "$data_45_days_ago",
                              "$data_yesterday"],
                     "lang": "js"}}},
            {"data_45_days_ago": 
                {"$eq": ["history.dates.$",
                         date.today()-timedelta(days=45)]}}},
		{"$out": "LCSQA_data"}
	])

def get_response(
	station,
	pollutants,
	n_days
):
	response = []
    for p in pollutants:
        averages = {str(x): 0 for x in range(24)}
        data = database["LCSQA_data"].find(
		    {"_id.station": station,
		    "_id.pollutant": p}
	    )
        for document in data:
		    hour = document["_id"]["hour"]
            history = list(zip(
                document["history"]["values"],
                document["history"]["dates"]
            )).reversed()
            i = 0
            while date.today()-history[i][1]<=timedelta(days=n_days):
                i += 1
            averages[str(hour)] = mean([e[0] for e in history[:i]])
        response.append(averages.values())
    return response
