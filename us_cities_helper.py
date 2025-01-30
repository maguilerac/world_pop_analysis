import csv
import json
import os
from dataclasses import asdict, dataclass, field

from req_http import JSONObject, http_get_sync


@dataclass
class CityInfo:
    name: str
    path: str
    url: str

    @property
    def raw_url(self) -> str:
        return self.url.replace("blob", "raw/refs/heads")

    @property
    def raw_data(self) -> dict[str, str]:
        return asdict(self)

    @raw_data.setter
    def raw_data(self, data: tuple[str, str, str]) -> None:
        self.name, self.path, self.url = data

    @property
    def boundary_data(self) -> JSONObject:
        return split_multipolygon(http_get_sync(self.raw_url))

    def __str__(self) -> str:
        return self.name.title()


@dataclass
class StateInfo:
    abbrev: str
    name: str
    path: str
    url: str
    cities: dict[str, CityInfo] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.abbrev.upper()}: {self.name}"


def get_github_repo_contents(owner: str, repo_name: str, path: str = "") -> JSONObject:
    """
    Perform GitHub API request to obtain the contents of a given path, in a
    given repository of a given owner.
    """

    query_url = f"https://api.github.com/repos/{owner}/{repo_name}/contents/{path}"
    contents = http_get_sync(query_url)
    return contents


def get_github_repo_contents_2(file_name: str) -> JSONObject:
    with open(f"data/{file_name}", mode="r") as file:
        data = json.load(file)
    return data


def split_multipolygon(input_data: JSONObject) -> JSONObject:
    """
    Split geojson's multipolygon geometry features into several polygon
    geometry features.
    """

    new_data: JSONObject = {"type": "FeatureCollection", "features": []}
    has_multipolygon_geometry = False

    if input_data["features"]:
        features = input_data["features"]
        for feature in features:
            if (feature["geometry"] is not None) and (
                feature["geometry"]["type"] == "MultiPolygon"
            ):
                has_multipolygon_geometry = True
                for poly in feature["geometry"]["coordinates"]:
                    xfeature = {
                        "type": "Feature",
                        "properties": {},
                        "geometry": {"type": "Polygon"},
                    }
                    xfeature["geometry"]["coordinates"] = poly
                    new_data["features"].append(xfeature)

    if has_multipolygon_geometry:
        return new_data
    return input_data


class CityDataProvider:
    def __init__(self):
        """
        Initialize the CityDataProvider with an empty city information catalog.
        Populate the catalog depending on whether or not data is locally available.
        """
        self.__city_info_catalog: dict[str, StateInfo] = {}
        self.__state_name_mapping: dict[str, str] = self.__get_state_name_mapping()
        if os.path.exists("./data/cities/cities.json"):
            print("cities.json file exists! Importing data...")
            self.__import_city_info_catalog()
        else:
            print("cities.json file does not exist! Retrieving data from server...")
            self.__generate_city_info_catalog()
            self.__export_city_info_catalog()

    def __get_state_name_mapping(self) -> dict[str, str]:
        """
        Get a dictionary mapping state names with their abbreviations.
        """
        with open("./data/US_States.csv", mode="r") as file:
            csvFile = csv.DictReader(file)
            state_data = {
                state_data["Abbreviation"]: state_data["State"]
                for state_data in csvFile
            }
            return state_data

    def __fill_city_info(self) -> None:
        """
        Fill the cities member of every StateInfo class in the city information catalog.
        """
        for abbrev, state in self.__city_info_catalog.items():
            # Get city-related contents from GitHub repo
            city_contents = get_github_repo_contents(
                "generalpiston", "geojson-us-city-boundaries", f"cities/{abbrev}"
            )

            # Keep only items linked to json files
            city_contents = [
                item for item in city_contents if item["name"].endswith(".json")
            ]

            cities = {
                item["name"].replace(".json", ""): CityInfo(
                    item["name"].replace(".json", ""),
                    item["path"],
                    item["_links"]["html"],
                )
                for item in city_contents
            }
            state.cities = cities

    def __generate_city_info_catalog(self) -> None:
        """
        Generate the city information catalog from data requested from the
        geojson-us-city-boundaries GitHub repository.
        """
        # Get state-related contents from GitHub repo
        state_contents = get_github_repo_contents(
            "generalpiston", "geojson-us-city-boundaries", "cities"
        )

        # Keep only directory items
        state_contents = [item for item in state_contents if item["type"] == "dir"]

        # Create dictionary of StateInfo objects
        self.__city_info_catalog = {
            item["name"]: StateInfo(
                item["name"],
                self.__state_name_mapping[item["name"].upper()],
                item["path"],
                item["_links"]["html"],
            )
            for item in state_contents
        }

        self.__fill_city_info()
        print(self.__city_info_catalog["ny"].url)

    def __export_city_info_catalog(self) -> None:
        """
        Export the city information catalog to ./data/cities/cities.json to
        avoid having to request data from the GitHub server next time.
        """

        # Generate dictionary
        data_to_export = [
            asdict(state_info) for state_info in self.__city_info_catalog.values()
        ]

        # Serialize json
        json_object = json.dumps(data_to_export, indent=4)

        # Create cities directory if it does not exist
        file_location = "./data/cities"
        if not os.path.isdir(file_location):
            os.makedirs(file_location)

        # Write to cities.json
        with open(f"{file_location}/cities.json", "w") as file:
            file.write(json_object)

    def __import_city_info_catalog(self) -> None:
        """
        Import data for the city information catalog from ./data/cities/cities.json
        """

        # Open and read the JSON file
        with open("./data/cities/cities.json", "r") as file:
            imported_data = json.load(file)

        # Generate dict of StateInfo objects with key = state abbreviation
        self.__city_info_catalog: dict[str, StateInfo] = {}
        for state_info in imported_data:
            self.__city_info_catalog[state_info["abbrev"]] = StateInfo(
                *state_info.values()
            )
            # Assign dict of CityInfo objects to cities member of current StateInfo object
            self.__city_info_catalog[state_info["abbrev"]].cities = {
                city_info["name"]: CityInfo(*city_info.values())
                for city_info in state_info["cities"].values()
            }

    def get_states(self, start_expression: str = "") -> list[str]:
        """
        Returns the list of states (names) starting with the expression
        specified by the start_expression argument.
        """

        states = [str(state_info) for state_info in self.__city_info_catalog.values()]
        states = [state for state in states if state.startswith(start_expression)]
        return states

    def get_cities(self, state_code: str, start_expression: str = "") -> list[str]:
        """
        Returns the list of cities (names) on a given state (argument state_code)
        that start with the expression specified by the start_expression argument.
        """

        cities = list(self.__city_info_catalog[state_code.lower()].cities.keys())
        cities = [city for city in cities if city.startswith(start_expression)]
        return cities

    def get_state_info_by_code(self, state_code: str = "") -> StateInfo | None:
        """
        Returns the StateInfo object associated to the provided state code.
        """

        rv = None
        if state_code in self.__city_info_catalog.keys():
            rv = self.__city_info_catalog[state_code]
        return rv

    def find_city_info(self, state_code: str, search_expr: str) -> CityInfo | None:
        """
        Returns the CityInfo on the StateInfo object associated to the given state code,
        and whose city name starts with the specified search expression.
        """

        rv = next(
            (
                city_info
                for city, city_info in self.__city_info_catalog[
                    state_code
                ].cities.items()
                if city.startswith(search_expr.lower())
            ),
            None,
        )
        return rv
