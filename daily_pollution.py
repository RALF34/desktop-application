import json
import re
import subprocess
import time

import requests
from matplotlib import pyplot
from pymongo import MongoClient

overseas_departments = [
    "GUADELOUPE",
    "GUYANE",
    "MARTINIQUE",
    "LA REUNION",
    "MAYOTTE",
    "SAINT-MARTIN"
]

symbol_to_name = {
    "O3": "ozone",
    "NO2": "dioxyde d'azote",
    "SO2": "dioxyde de soufre",
    "PM2.5": "particules fines",
    "PM10": "particules",
    "CO": "monoxyde de carbone"
}
name_to_symbol = {
    v: k for (k, v) in symbol_to_name.items()
}

OMS_guidelines = {
    pollutant: value for (pollutant, value) in zip(
        symbol_to_name.keys(),
        [100,25,40,15,45,4]        
    )
}

mongoClient = MongoClient() # mongodb://localhost:8001
database = mongoClient["air_quality"]

def get_choices_from_database(about, search_filter=None):
    match about:
        case "regions":
            items = database["regions"].find().distinct("_id")
            for e in overseas_departments:
                items.remove(e)
        case "departments":
            if search_filter["_id"] == "OUTRE-MER":
                items = overseas_departments
            else:
                items = list(set(database["regions"].find_one(
                    search_filter)["departments"]))
        case "cities":
            items = list(set(database["departments"].find_one(
                search_filter
            )["cities"]))
        case "stations":
            list_of_stations = database["cities"].find_one(
                search_filter
            )["stations"]
            items = list(set([e["name"]+" (zone "+e["zone"]+")"+e["code"] 
            for e in list_of_stations]))
        case "pollutants":
            items = list(map(
                lambda x: symbol_to_name[x],
                list(set(database["distribution_pollutants"].find_one(
                    search_filter
                )["monitored_pollutants"]))))
    choices = list(zip(range(1,len(items)+1), sorted(items)))
    if about == "regions":
        choices.append((len(choices)+1,"OUTRE MER"))
    return choices

def get_input_from_user(about, choices, shorter_period=False):
    french_translation = {
        "regions": "régions",
        "departments": "départements",
        "cities": "communes",
        "stations": "stations"}
    if about in french_translation.keys():
        message_to_user = "Veuillez indiquer le numéro correspondant "+\
            ("à la " if about in ["regions","cities","stations"] else "au ")+\
            french_translation[about][:-1] + " de votre choix."+"\n\n"
        list_choices = [
            str(e[0])+" : "+(e[1] if not(about=="stations") 
            else e[1][:e[1].index(")")+1]) for e in choices]
    elif about == "pollutants":
        message_to_user = "Par quelle type de pollution ètes-vous intéressé ?\n\n"
        list_choices = [
            str(e[0])+" : Pollution "+("à" if e[1]=="ozone" else "au")+e 
            for e in choices]
    elif not(shorter_period):
        message_to_user = "L'analyse de pollution est par défaut réalisée à \
            partir de valeurs de concentration atmosphérique\nobtenues sur les \
            45 derniers jours. Voulez-vous considérer une période plus courte ?\n\n"
        list_choices = ["Si oui, entrez 0.", "Sinon, entrez 1."]
    else:
        message_to_user = "Entrez le nombre de jours souhaité.\n\
            (Indiquez '0' pour revenir aux choix précédents)\n\n"
        list_choices = [""]
    if about != "regions":
        list_choices += ["\n"+str(len(list_choices)+1)+ " : Retour aux choix précédents"]
    
    text =  message_to_user+"\n".join(list_choices)+"\n"
    number = input(text)
    x = 0 if about == "n_days" and not(shorter_period) else 1
    y = 45 if about == "n_days" and shorter_period else len(list_choices)+1
    while not (number.isdigit() and int(number) not in range(x,y)):
        number = input(text)
    if about == "n_days" and not(shorter_period):
        if int(number):
            return "45"
        else:
            return get_input_from_user(about, choices, shorter_period=True)
    else:
        return number

def get_selected_item(about, filter_item=None):
    if filter_item is None:
        choices = get_choices_from_database(about)
    else:
        choices = get_choices_from_database(about, search_filter={"_id": filter_item})
    selected_number = int(get_input_from_user(about, choices))
    item = 0 if selected_number == len(choices) else choices[selected_number-1][1]
    return item

def get_query_parameters(about, item=None, previous_item=None):
    match about:
        case "regions":
            region = get_selected_item(about)
            return get_query_parameters("departments", item=region)
        case "departments":
            department = get_selected_item(about, item)
            if type(department) is int:
                return get_query_parameters("regions")
            else:
                return get_query_parameters("cities", item=department, previous_item=item)
        case "cities":
            city = get_selected_item(about, item)
            if type(city) is int:
                return get_query_parameters("departments", item=previous_item)
            else:
                return get_query_parameters("stations", item=city, previous_item=item)
        case "stations":
            s = get_selected_item(about, item)
            if type(s) is int:
                return get_query_parameters("cities", item=previous_item)
            else:
                station, code = s[:s.index("(")-1], s[s.index(")")+1:]
                return code, get_query_parameters("pollutants", item=station, previous_item=item)
        case "pollutants":
            pollutant = name_to_symbol[get_selected_item("pollutants", item)]
            if type(pollutant) is int:
                return get_query_parameters("stations", item=previous_item)
            else:
                return pollutant, get_query_parameters("n_days", previous_item=item)
        case "n_days":
            n_days = get_input_from_user("n_days", [])
            if not(n_days):
                return get_query_parameters("pollutants", item=previous_item)
            else:
                return n_days

if __name__=="__main__":
    i = 0
    while "last_update" not in database.list_collection_names():
        if i == 4:
            i = 0
        print("Initialisation des données en cours"+"."*i+"\r")
        i += 1
        time.sleep(0.7)
    
    code, pollutant, n_days = get_query_parameters("regions")
    
    data = json.loads(requests.get(
        "http://127.0.0.1:8000/?s=code&p=pollutants&n=n_days"
    ).json())

    fig, ax = pyplot.subplots()
    fig.set_size_inches(17,14)
    for i, p in enumerate(pollutants):
        ax.scatter(
            [x+"h00" for x in range(24)],
            data[i].values())
        max_value = max(data[i].values())
        thresholds = [
            (x/3)*OMS_guidelines[p]
            for x in range(1,5)]
        max_level = 2
        while (max_level < 5 and thresholds[max_level] < max_value):
            max_level += 1
        if max_level == 5:
            max_level -= 1
        space = (0.40)*thresholds[0]
        lim = thresholds[max_level] if max_level == 2 else max_value
        ax.set_ylim(0,lim+(space if max_level == 2 else 0))
        colors = ["limegreen","yellow","orange","red"]
        y_min = 0     
        for j in list(range(max_level+1)):
            ax.fill_between(
                list(range(24)),
                thresholds[j],
                y2=y_min,
                color=colors[j],
                alpha=0.1)
            y_min = thresholds[j]
            if j == 2:
                ax.axhline(
                    y=thresholds[j],
                    color="blueviolet",
                    linestyle="--",
                    linewidth=1.7,
                    label="Concentration \n moyenne \n"+ \
                          "journalière \n recommandée (O.M.S)")
        if max_value > thresholds[max_level]:
            ax.fill_between(
                list(range(24)),
                ax.get_ylim()[1],
                y2=thresholds[max_level],
                color="magenta",
                alpha=0.1)
        ax.set_yticks([0])
        ax.set_yticklabels([" "])
        ax.legend(loc="upper right")
        pyplot.show()
