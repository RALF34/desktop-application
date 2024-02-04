import subprocess
import time
from datetime import date, datetime, timedelta
from statistics import mean

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
            items = list(set([
                e["name"]+" (zone "+e["zone"]+")#"+e["code"]
                for e in list_of_stations]))
        case "pollutants":
            items = list(map(
                lambda x: symbol_to_name[x],
                list(set(database["distribution_pollutants"].find_one(
                    search_filter
                )["monitored_pollutants"]))))
    choices = list(zip(range(1,len(items)+1), sorted(items)))
    if about == "regions":
        choices.append((len(choices)+1,"OUTRE-MER"))
    return choices

french_translation = {
    "regions": "régions",
    "departments": "départements",
    "cities": "communes",
    "stations": "stations"}

all_the_stations = database["distribution_pollutants"].distinct("_id")

def is_number(string):
    try:
        int(string)
        return True
    except ValueError:
        return False

def get_input_from_user(about, choices, shorter_period=False, first_choice=False):
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
            str(e[0])+" : Pollution "+("à l'" if e[1]=="ozone" else "au ")+e[1]
            for e in choices]
    elif not(shorter_period):
        message_to_user = " L'analyse de pollution est par défaut réalisée à "+\
        "partir de valeurs de\nconcentration atmosphérique obtenues sur les "+\
        "180 derniers jours. \nVoulez-vous considérer une période plus courte ?\n\n"
        list_choices = ["Si oui, entrez 0.", "Sinon, entrez 1."]
    else:
        message_to_user = "Entrez le nombre de jours souhaité.\n\
             (Indiquez '0' pour revenir aux choix précédents)\n\n"
        list_choices = [""]
    if about != "regions":
        list_choices += [
            "\n"+str(len(list_choices)+1)+ " : Retour"]
    x = 0 if about == "n_days" and not(shorter_period) else 1
    y = 45 if about == "n_days" and shorter_period else len(list_choices)+1
    text = message_to_user+"\n".join(list_choices)+"\n\n"
    space = "\n" if (about == "regions" and first_choice) else "\n"*4
    number = input(space+text)
    while not(is_number(number) and int(number) in range(x,y)):
        number = input("\n"*4+text)
    if int(number) < y-1:
        if about == "stations":
            item = choices[int(number)-1][1]
            station_found = item[item.index("#")+1:] in all_the_stations
            if not(station_found):
                print("Désolé, aucune données disponibles pour cette station.\n")
                return 0
        if about == "n_days" and not(shorter_period):
            if int(number):
                return "180"
            else:
                return get_input_from_user(about, choices, shorter_period=True)
    return number

steps = ["regions","departments","cities","stations","pollutants","n_days"]

class Interaction_with_user():
    def __init__(self):
        self.parameters = {
            "s": None,
            "station_name": None,
            "p": None,
            "n": None}
        self.i = 0
        self.first_choice = True
        self.previous_filter = None
        self.next_filter = None
        self.go_back = False
        self.station_not_found = False
        self.done = False

    def get_selected_item(self):
        current_step = steps[self.i]
        if not(self.i):
            choices = get_choices_from_database(current_step)
        elif current_step != "n_days":
            choices = get_choices_from_database(
                current_step,
                search_filter=self.next_filter)
        else:
            choices = []
        
        x = get_input_from_user(
            current_step,
            choices,
            first_choice=self.first_choice)
        if type(x) is str:
            if self.i != 5:
                item = None if int(x) == len(choices)+1 else \
                choices[int(x)-1][1]
            else:
                item = int(x)
            if item is None:
                self.go_back = True
            else:
                self.previous_filter = self.next_filter
                if current_step in ["regions","departments","cities"]:
                    self.next_filter = {"_id": item}
                elif current_step == "stations":
                    name, code = item.split(sep="#")
                    self.parameters["s"] = code
                    self.parameters["station_name"] = name
                    self.next_filter = {"_id": code}
                elif current_step == "pollutants":
                    self.parameters["p"] = name_to_symbol[item]
                else:
                    self.parameters["n"] = item
                    self.done = True
        else:
            self.station_not_found = True
    def next_step(self):
        if self.go_back:
            self.i -= 1
            self.next_filter = self.previous_filter
            self.go_back = False
        else:
            if self.station_not_found:
                self.done = True
            self.i += 1
    

def plot_variation(station, pollutant, values):
    fig, ax = pyplot.subplots()
    fig.set_size_inches(17,14)
    ax.scatter([str(x)+"h00" for x in range(24)], values)
    max_value = max(values)
    thresholds = [
        (x/3)*OMS_guidelines[pollutant]
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
    ax.set_title(
        station+"/\nPollution "+
        ("à l'" if pollutant=="O3" else "au ")+
        symbol_to_name[pollutant],
        ha="center")
    pyplot.savefig("image")


def main():
    i = 0
    while "last_update" not in database.list_collection_names():
        if i == 4:
            i = 0
        print(" Initialisation des données en cours"+(
            "    " if not(i) else "."*i), end="\r")
        i += 1
        time.sleep(0.7)
    
    DATE = date.today() - timedelta(days=1)
    DATETIME = datetime(DATE.year, DATE.month, DATE.day)
    if database["last_update"].find_one()["date"] != DATETIME:
        dictionary = database["distribution_pollutants"].find_one()
        parameters = {
            "s": dictionary["_id"],
            "p": dictionary["monitored_pollutants"][0],
            "n": "0"}
        _ = requests.get(
            "http://127.0.0.1:8000",
            params=parameters,
            verify=False)
    
    process = Interaction_with_user()
    while not(process.done):
        process.get_selected_item()
        process.next_step()

    values = requests.get(
        "http://127.0.0.1:8000",
        params=process.parameters,
        verify=False).json()["values"]
    plot_variation(
        process.parameters["station_name"],
        process.parameters["p"],
        values)

if __name__=="__main__":
    main()
