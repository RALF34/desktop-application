from enum import Enum
from typing import Annotated, Dict

from fastapi import FastAPI, HTTPException, Path, Query
from pandas import read_excel
from pydantic import BaseModel, Field

from .crud import get_response, initialize_or_update_database

app = FastAPI()

initialize_or_update_database()

class averageConcentrations(BaseModel):
    values: list[float] = Field(
        description="The average values of air concentration associated \
        to each of the 24 hours of the day (set to 0 when not enough data)."
    )

LCSQA_stations = read_excel(
"Liste points de mesures 2020 pour site LCSQA_221292021.xlsx"
)["Code station"].tolist()

@app.get("/", response_model=list[averageConcentrations])
async def get_response(
    station: Annotated[
        str,
        Path(
            description=(
                "Code identifying the air quality monitoring station \
                 whose data will be used by the API."),
            pattern="^FR([0-9]{5}$)")],
    pollutants: Annotated[
        list[str],
        Query(
            description=(
                "Pollutant(s) whose average daily variation of air \
                 concentration(s) we want to display."))],
    n_days: Annotated[
        int,
        Query(
            description=(
                "Parameter specifying the size (in number of days) of\
                 the period whose pollution data are being used by the\
                 API (i.e the analysis is based on data collected over\
                 the last 'n_days' days)."),
            ge=1,
            le=44)]):
    if station not in LCSQA_stations:
        raise HTTPException(status_code=404, detail="Station not found")
    else:
        return crud.get_response(station, pollutants, n_days)
