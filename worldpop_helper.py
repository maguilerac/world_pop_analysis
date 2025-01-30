import json
from typing import AsyncGenerator

from req_http import JSON, JSONObject, http_get

DATASETS = ["wpgppop", "wpgpas"]


def __generate_query_string(dataset_id: int, year: int, geojson: JSONObject) -> str:
    """
    Generate a WorldPop's advanced data API query string, for the specified
    dataset id, year and geojson string representing the boundary of a U.S. city.
    i.e.,
    api.worldpop.org/v1/services/stats?dataset={dataset_name}&year={year}&geojson={geojson}
    """
    dataset_name = DATASETS[dataset_id - 1]
    query_string = "https://api.worldpop.org/v1/services/stats?dataset="
    query_string = (
        f"{query_string}{dataset_name}&year={year}&geojson={json.dumps(geojson)}"
    )
    return query_string


async def create_query_task(dataset_id: int, year: int, geojson: JSONObject) -> JSON:
    """
    Create a WorldPop's advanced data API query, given a dataset, a year and
    a geojson string representing the boundary of a U.S. city.
    A task starts for this query, because it runs asynchronously on the server.
    It returns the id of the created query task.
    """

    query_string = __generate_query_string(dataset_id, year, geojson)
    results = await http_get(query_string)
    return results["taskid"]


async def next_subquery(
    dataset_id: int, start_year: int, end_year: int, geojson: JSONObject
) -> AsyncGenerator[JSON, None]:
    """
    Create individual WorldPop's advanced data API queries, given a dataset,
    a start and end year and a geojson string representing the boundary of a
    U.S. city.
    A task starts for the current subquery associated to a given year in the
    [start_year, end_year] range.
    It yields the taskid of the currently created subquery.
    """

    for year in range(start_year, end_year + 1):
        taskid = await create_query_task(dataset_id, year, geojson)
        yield taskid


async def perform_us_city_query(
    dataset_id: int, start_year: int, end_year: int, geojson: JSONObject
) -> list[JSON]:
    """
    Perform a set of WorldPop's advanced data API queries, given a dataset,
    a start and end year and a geojson string representing the boundary of a
    U.S. city.
    A task starts for each subquery associated to each of the years in the
    [start_year, end_year] range.
    It returns a list with the ids of all query tasks.
    """

    taskids = [
        taskid
        async for taskid in next_subquery(dataset_id, start_year, end_year, geojson)
    ]
    return taskids
