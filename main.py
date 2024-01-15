from enum import Enum
from typing import Annotated, Dict

from fastapi import FastAPI, HTTPException, Query
from pandas import read_excel
from pydantic import BaseModel, Field

from .crud import get_response, history_is_updated, update_database

app = FastAPI()

# Initialization of the database.
update_database()

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

@app.get("/", response_model=list[averageConcentrations])
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
        int,
        Query(
            alias="n",
            description=(
                "Parameter specifying the size (in number of days) of\
                 the period whose pollution data are being used by the\
                 API (i.e the analysis is based on data collected over\
                 the last 'n_days' days)."),
            ge=0,
            le=45)]):
    if station not in LCSQA_stations:
        raise HTTPException(status_code=404, detail="The station code is not valid !")
    if not(history_is_updated):
        update_database()
    return get_response(station, pollutant, n_days)
