import logging

# Create and configure logger
logging.basicConfig(
    level=logging.DEBUG,
    filename="us_cities.log",
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)

logger = logging.getLogger("U.S. City Data Services")
