import json
import requests

from matplotlib import pyplot
from pymongo import MongoClient

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

mongoClient = MongoClient(mongodb://localhost:8001)

def get_choices_from_database(about=None, search_filter=None, key_0=None, key_1=None):
    if about == "regions":
        items = mongoClient["LCSQA_stations"].find().distinct("_id.region")
    elif about == "stations" :
        items = mongoClient["LCSQA_stations"].find_one(
            search_filter
        )["names"]
    elif about=="pollutants":
        items = map(
            lambda x: symbol_to_name[x],
            mongoClient["distribution_pollutants"].find_one(
            search_filter
            )["monitored_pollutants"])
    else:
        items = [
            e[key_1] for e in
            mongoClient["LCSQA_stations"].find(
                search_filter
            ).distinct(key_0)]
    return list(zip(range(1,len(items)+1),items))

def get_input_from_user(about, choices, shorter_period=False):
    if len(choices) == 1:
        return "0"
    elif (about in ["région","département","commune","station"]):
        message_to_user = "Veuillez indiquer le numéro correspondant "+\
            ("à la " if about in ["région","commune","station"] else "au ")+\
            about + "de votre choix\n"+(" (puis tapez 'Entrée' pour continuer)."
            if about == "région" else ".")
        list_choices = [str(e[0])+" : "+e[1] for e in choices]
    elif about == "pollutant":
        message_to_user = "Par quelle type de pollution ètes-vous intéressé ? \n\
            (Pour indiquer plusieurs choix, entrez les numéros\n\correspondants \
            séparer d'un espace.)"
        list_choices = [
            str(e[0])+" : "+"Pollution "+("à" if e[1]=="ozone" else "au")+e 
            for e in choices]
    elif not(shorter_period):
        message_to_user = "L'analyse de pollution est par défaut réalisée à \
            partir de valeurs de concentration atmosphérique\nobtenues sur les \
            45 derniers jours. Voulez-vous considérer une période plus courte ?"
        list_choices = ["Si oui, entrez 0.", "Sinon, entrez 1."]
    else:
        message_to_user = "Entrez le nombre de jours souhaité.\n"
        list_choices = [""]
    number = input(
        message_to_user+"\n".join(list_choices))
    if about == "n_days" and not(shorter_period):
        if number:
            return "45"
        else:
            return get_input_from_user(about, choices, shorter_period=True)
    else:
        if not(shorter_period):
            while int(number) not in range(1,len(choices)+1):
                number = get_input_from_user(about, choices)
        else:
            while int(number) not in range(1,45):
                number = get_input_from_user(about, choices, shorter_period=True)
        return number

if __name__ == "__main__":
    choices_regions = get_choices_from_database(about="regions")
    selected_number = int(get_input_from_user(
        "région",
        choices_regions))
    selected_region = choices_regions[selected_number-1][1]

    choices_departments = get_choices_from_database(
        search_filter={"_id": selected_region},
        key_0="departments",
        key_1="department")
    selected_number = int(get_input_from_user(
        "département",
        choices_department))
    selected_department = choices_departments[selected_number-1][1]
    
    choices_cities = get_choices_from_database(
        search_filter={"departments.department": selected_department},
        key_0="names",
        key_1="city")
    selected_number = int(get_input_from_user(
        "commune",
        choices_cities))
    selected_city = choices_cities[selected_number-1][1]

    choices_stations = get_choices_from_database(
        about="stations",
        search_filter={"departments.names.city": selected_city})
    selected_number = int(get_input_from_user(
        "station",
        choices_stations))
    s = choices_stations[selected_number-1][1]

    choices_pollutants = get_choices_from_database(
        about="pollutants",
        search_filter={"_id": selected_station})
    selected_numbers = list(map(
        lambda x: int(x),
        get_input_from_user("pollutant",choices_pollutants).split()))
    pollutants = [
        name_to_symbol[choices_pollutants[i-1][1]]for i in selected_numbers]

    n = get_input_from_user("n_days",[])

    data = json.loads(request.get(
        "http://127.0.0.1/?station=s&pollutants=pollutants&n_days=n"
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
        fig.savefig(
            s+"/"+p+"("+str(n)+" jours)")
        fig.clf()
