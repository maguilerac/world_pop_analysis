# world_pop_analysis
World Population Analysis Tool

Simple application allowing an advanced query on the WorldPop online engine.

The application aims to provide the user the following information from a given city in the U.S.:
* Total population
* Age and sex structures

Current status: Application opens query tasks asynchronously and retrieves results for any of the two datasets.
* Request-URI Too Long errors are obtained in some cases due to long geojsons.
* Geojsons with multypolygon feature geometries are also leading to errors.
