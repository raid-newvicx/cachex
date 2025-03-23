from enum import Enum
from typing import Annotated

import pandas as pd
from cachex import cache_value
from fastapi import Depends, FastAPI, Response
from pydantic import AnyHttpUrl
from pydantic_core import Url


app = FastAPI()

class DataSet(Enum):
    UBER1 = "uber1"
    UBER2 = "uber2"
    UBER3 = "uber3"

URLS: dict[DataSet, AnyHttpUrl] = {
    DataSet.UBER1: AnyHttpUrl("https://github.com/plotly/datasets/raw/master/uber-rides-data1.csv"),
    DataSet.UBER2: AnyHttpUrl("https://github.com/plotly/datasets/raw/master/uber-rides-data2.csv"),
    DataSet.UBER3: AnyHttpUrl("https://github.com/plotly/datasets/raw/master/uber-rides-data3.csv"),
}


def get_url(dataset: DataSet) -> AnyHttpUrl:
    """Get the dataset URL from the global mapping."""
    return URLS[dataset]


@cache_value(type_encoders={Url: lambda t: t.unicode_string()})
def download_csv_data(url: Annotated[AnyHttpUrl, Depends(get_url)]) -> pd.DataFrame:
    """Download a CSV file from the given URL and convert it to a DataFrame."""
    return pd.read_csv(url.unicode_string())


@app.get("/datasets/{dataset}")
def get_dataset(df: Annotated[pd.DataFrame, Depends(download_csv_data)]):
    """Download a CSV dataset from the web and return the data in JSON form."""
    data = df.iloc[0:1000, :].to_json(orient="records", indent=2)
    return Response(content=data, media_type="application/json")