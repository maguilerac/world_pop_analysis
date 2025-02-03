import json
import time
from dataclasses import dataclass
from typing import AsyncGenerator

from logger_utils import logger
from req_http import JSON, JSONObject, http_get

DATASETS = ["wpgppop", "wpgpas"]
API_URL_BASE = "https://api.worldpop.org/v1"


@dataclass
class AgeSexClass:
    number: int
    ageRange: str
    male: float
    female: float

    def __str__(self) -> str:
        return f"> [Age: {self.ageRange}] / Male: {self.male}, Female: {self.female}"


class WorldPopAdvancedQuery:
    def __init__(
        self, dataset_id: int, start_year: int, end_year: int, geojson: JSONObject
    ):
        """
        Initialize the WorldPopAdvancedQuery with ...
        """
        self.__dataset_id = dataset_id
        self.__start_year = start_year
        self.__end_year = end_year
        self.__geojson = geojson
        self.__query_tasks_by_year: dict[int, str] = {}

    def __generate_query_string(self, year: int) -> str:
        """
        Generate a WorldPop's advanced data API query string, for the specified
        dataset id, year and geojson string representing the boundary of a U.S. city.
        i.e.,
        api.worldpop.org/v1/services/stats?dataset={dataset_name}&year={year}&geojson={geojson}
        """
        dataset_name = DATASETS[self.__dataset_id - 1]
        query_string = f"{API_URL_BASE}/services/stats?dataset="
        query_string = f"{query_string}{dataset_name}&year={year}&geojson={json.dumps(self.__geojson)}"
        return query_string

    async def __create_query_task(self, year: int) -> JSON:
        """
        Create a WorldPop's advanced data API query, given a dataset, a year and
        a geojson string representing the boundary of a U.S. city.
        A task starts for this query, because it runs asynchronously on the server.
        It returns the id of the created query task.
        """

        query_string = self.__generate_query_string(year)
        results = await http_get(query_string)
        if results:
            self.__query_tasks_by_year[year] = str(results["taskid"])
            return results["taskid"]

    async def __next_subquery(self) -> AsyncGenerator[JSON, None]:
        """
        Create individual WorldPop's advanced data API queries, given a dataset,
        a start and end year and a geojson string representing the boundary of a
        U.S. city.
        A task starts for the current subquery associated to a given year in the
        [start_year, end_year] range.
        It yields the taskid of the currently created subquery.
        """

        for year in range(self.__start_year, self.__end_year + 1):
            taskid = await self.__create_query_task(year)
            logger.info(f"Query task created for year {year} with id = {taskid}")
            yield taskid

    async def __monitor_query_task(self, taskid: JSON) -> JSONObject:
        """
        abc
        """

        query_string = f"{API_URL_BASE}/tasks/{taskid}"
        results = await http_get(query_string)
        return results

    async def __next_monitor(
        self, taskids: list[JSON]
    ) -> AsyncGenerator[JSONObject, None]:
        """
        asdf
        """

        while len(taskids) > 0:
            response = await self.__monitor_query_task(taskids[0])
            if response["status"] == "finished":
                if response["error"]:
                    logger.error(response["error_message"])
                else:
                    logger.info(f"Task #{taskids[0]} finished!")
                del taskids[0]
                yield response
            else:
                time.sleep(1)

    def __transform_pyramid_to_objects(
        self, pyramid_data: list[dict[str, JSON]]
    ) -> list[AgeSexClass]:
        pyramid = [
            AgeSexClass(
                int(item["class"]),
                str(item["age"]),
                float(item["male"]),
                float(item["female"]),
            )
            for item in pyramid_data
        ]
        return pyramid

    async def perform_us_city_query(self) -> list[JSON]:
        """
        Perform a set of WorldPop's advanced data API queries, given a dataset,
        a start and end year and a geojson string representing the boundary of a
        U.S. city.
        A task starts for each subquery associated to each of the years in the
        [start_year, end_year] range.
        It returns a list with the ids of all query tasks.
        """

        taskids = [taskid async for taskid in self.__next_subquery()]
        return taskids

    async def retrieve_results(
        self, taskids: list[JSON]
    ) -> dict[int, float | list[AgeSexClass]] | None:
        """
        abc
        """

        raw_results = [response async for response in self.__next_monitor(taskids)]
        if any(result["error"] for result in raw_results):
            error_messages = (
                result["error_message"] for result in raw_results if result["error"]
            )
            print(next(error_messages))
            return None
        else:
            if self.__dataset_id == 1:
                raw_results = {
                    result["taskid"]: float(result["data"]["total_population"])
                    for result in raw_results
                }
            else:
                raw_results = {
                    result["taskid"]: self.__transform_pyramid_to_objects(
                        result["data"]["agesexpyramid"]
                    )
                    for result in raw_results
                }
            results = {
                year: raw_results[taskid]
                for year, taskid in self.__query_tasks_by_year.items()
            }
            return results
