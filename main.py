from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
from pandas import read_excel
from pydantic import BaseModel, Field

from .crud import \
get_values, history_is_updated, is_monitored_by, update_database

app = FastAPI()

# Initialization of the database.
update_database(initialization=True)

class averageConcentrations(BaseModel):
    values: list[float] = Field(
        description="The average values of air concentration associated \
        to each of the 24 hours of the day (set to 0 when not enough data)."
    )

url = "https://www.lcsqa.org/system/files/media/documents/"+\
"Liste points de mesures 2021 pour site LCSQA_27072022.xlsx"
    
LCSQA_stations = read_excel(
    url.replace(" ", "%20"),
    sheet_name=1
).iloc[:,0][2:].tolist()

@app.get("/", response_model=averageConcentrations)
async def get_response(
    station: Annotated[
        str,
        Query(
            alias="s",
            description=(
                "Code identifying the air quality monitoring station\
                 whose data will be used by the API."),
            pattern="^FR([0-9]{5}$)")],
    pollutant: Annotated[
        str,
        Query(
            alias="p",
            description=(
                "Pollutant whose average daily variation of air\
                concentration we want to display."))],
    n_days: Annotated[
        str,
        Query(
            alias="n",
            description=(
                "Parameter specifying the size (in number of days) of\
                 the period whose pollution data are being used by the\
                 API (i.e the analysis is based on data collected over\
                 the last 'n_days' days)."),
            pattern="\d+")]):
    if station not in LCSQA_stations:
        raise HTTPException(status_code=400, detail="Station code not valid !")
    if pollutant not in ["03","NO2","SO2","PM2.5","PM10","CO"]:
        raise HTTPException(status_code=400, detail="No data for this pollutant !")
    if not(is_monitored_by(pollutant, station)):
        raise HTTPException(status_code=400, detail="Pollutant not available for this station !")
    if int(n_days) not in list(range(181)):
        raise HTTPException(status_code=400, detail="Number of days too high !")
    if not(history_is_updated):
        update_database()
    return averageConcentrations(
        values=get_values(station, pollutant, int(n_days)))
