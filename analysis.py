import asyncio

from us_cities_helper import CityDataProvider, CityInfo
from worldpop_helper import perform_us_city_query


def select_state(city_data_provider: CityDataProvider) -> str | None:
    """
    Prompt the user for a state in the U.S. It returns None if the user's input
    is invalid, or the code of the state (string) if the user's choice is valid.
    """

    # Prompt user for the first letter of state
    first_letter = input("Enter the first letter of a U.S. state: ").strip().upper()

    # Filter states starting with the given letter
    filtered_states = city_data_provider.get_states(first_letter)

    if not filtered_states:
        print(f"No states found starting with '{first_letter}'. Try again.")
        return None

    # Print the filtered list
    print("\nStates starting with '{}':".format(first_letter))
    for state in filtered_states:
        print(str(state))

    # Prompt user for the code of a state
    chosen_state_code = input("\nEnter the code of your chosen state: ").strip()
    chosen_state = city_data_provider.get_state_info_by_code(chosen_state_code)
    if chosen_state:
        print(f"You chose: {str(chosen_state)}\n")
        return chosen_state_code
    else:
        print(f"No state found with code {chosen_state_code}. Try again.\n")
    return None


def select_city(city_data_provider: CityDataProvider) -> CityInfo | None:
    """
    Prompt the user for a state and a city in the U.S. It returns a CityInfo
    object if the user's choice for state and city are valid.
    """

    # Prompt user for the state
    state_code = select_state(city_data_provider)
    if not state_code:
        return None

    # Prompt user for the first letter of city
    first_letter = (
        input("Enter the first letter of a city in that state: ").strip().lower()
    )

    # Filter cities starting with the given letter
    filtered_cities = city_data_provider.get_cities(state_code, first_letter)

    if not filtered_cities:
        print(f"No cities found starting with '{first_letter}'. Try again.")
        return None

    # Print the filtered list
    print("\nCities starting with '{}':".format(first_letter))
    for city in filtered_cities:
        print(city.title())

    # Prompt user for the code of a state
    city_search_expression = input(
        "\nEnter the first letters of your chosen city: "
    ).strip()
    chosen_city = city_data_provider.find_city_info(state_code, city_search_expression)
    if chosen_city:
        print(f"You chose: {str(chosen_city)}\n")
        return chosen_city
    else:
        print(f"No city found with expression {city_search_expression}. Try again.\n")
    return None


async def main() -> None:
    city_data_provider = CityDataProvider()

    # City variable options available in WorldPop advanced API (i.e. datasets)
    city_variables = {"1": "Total Population", "2": "Age and Sex Structures"}

    # Initialize variables containing user choices
    selected_city = None
    selected_variable = None

    while True:
        print("\nChoose an option:")
        print("1. Choose a city")
        print("2. Indicate variable to be determined")
        print("3. Execute query")
        print("4. Export results to file")
        print("5. Exit the program")

        choice = input("Enter your choice (1-5): ").strip()

        if choice == "1":
            selected_city = select_city(city_data_provider)

        elif choice == "2":
            if not selected_city:
                print("Please choose a city first.")
                continue
            print("\nAvailable variables:")
            for index, variable in city_variables.items():
                print(f"{index}. {variable}")
            selected_variable = input("\nEnter the variable to be determined: ").strip()
            if selected_variable not in city_variables.keys():
                print(f"Variable '{selected_variable}' not found. Try again.")
                selected_variable = None
            else:
                print(f"Variable '{city_variables[selected_variable]}' selected.")

        elif choice == "3":
            if not selected_city or not selected_variable:
                print("Please choose a city and a variable first.")
                continue
            geojson_data = selected_city.boundary_data

            try:
                start_year = int(input("Enter the initial year of the query: ").strip())
                end_year = int(input("Enter the end year of the query: ").strip())

                if start_year > end_year:
                    print(
                        "Invalid range: Start year must be less than or equal to end year."
                    )
                    continue

                taskids = await perform_us_city_query(
                    int(selected_variable), start_year, end_year, geojson_data
                )
                print(taskids)
                # if result:
                #     print(f"\nQuery result: {result}")
                #     ...
                # else:
                #     print(f"No data found for {selected_variable} in {selected_city}.")

            except ValueError:
                print("Invalid input. Please enter numeric values for years.")

        elif choice == "4":
            filename = input(
                "Enter the filename to export results (e.g., results.csv): "
            ).strip()
            # provider.export_results(filename)

        elif choice == "5":
            print("Exiting the program. Goodbye!")
            break

        else:
            print("Invalid choice. Please enter a number between 1 and 5.")


if __name__ == "__main__":
    asyncio.run(main())
