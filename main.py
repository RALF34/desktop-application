from enum import Enum
from typing import Annotated, Dict

from fastapi import FastAPI, HTTPException, Path, Query
from pandas import read_excel
from pydantic import BaseModel, Field

from .crud import get_response, initialize_database
update_database

app = FastAPI()

initialize_database()

class Pollutant(str, Enum):
    O3 = "Ozone",
    NO2 = "Dioxyde d'azote",
    SO2 = "Dioxyde de soufre",
    PM2.5 = "Particules fines",
    PM10 = "Particules",
    CO = "Monoxyde de carbone"

class averageDailyVariationOfPollution(BaseModel):
    pollutant: Pollutant
    average_concentrations: Dict[str, float] = Field(
        description="The average values of air concentration associated \
        to each of the 24 hours of the day."
    )

LCSQA_stations = read_excel(
"Liste points de mesures 2020 pour site LCSQA_221292021.xlsx"
)["Code station"].tolist()

@app.get("/{station}", response_model=list[averageDailyVariationOfPollution])
async def get_response(
    station: Annotated[
        str,
        Path(
            description=(
                "Code identifying the air quality monitoring station \
                 whose data will be used by the API".
            ),
            pattern="^FR([0-9]{5}$)"
        )
    ],
    pollutant: Annotated[
        list[Pollutant],
        Query(
            description=(
                "Pollutant(s) whose average daily variation of air \
                 concentration(s) we want to display."
            )
        )
    ],
    n_days: Annotated[
        int,
        Query(
            description=(
                "If specified, the analysis of the API will be based on \
                 pollution data collected over the 'n_days' last days."
            ),
            ge=1,
            le=44
        )
    ]
):
    if station not in LCSQA_stations:
        raise HTTPException(status_code=404,detail="Station not found")
    else:
        return crud.get_response(
            station,
            pollutants,
            n_days
        )
