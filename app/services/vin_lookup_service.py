import requests
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class VinLookupService:
    """
    A service to look up vehicle information using a VIN from the NHTSA vPIC API.
    """
    BASE_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinExtended/"

    def __init__(self):
        pass

    def lookup_vin(self, vin: str) -> Dict[str, Any]:
        """
        Looks up a VIN using the NHTSA vPIC API and returns the decoded information.

        Args:
            vin (str): The Vehicle Identification Number to look up.

        Returns:
            Dict[str, Any]: A dictionary containing the vehicle details from the API.

        Raises:
            ValueError: If the provided VIN is invalid.
            requests.exceptions.RequestException: For network-related errors.
        """
        if not vin or not isinstance(vin, str):
            logging.error("Invalid VIN provided for lookup.")
            raise ValueError("A valid VIN string must be provided.")

        endpoint_url = f"{self.BASE_URL}{vin}?format=json"

        try:
            logging.info(f"Sending VIN lookup request to: {endpoint_url}")
            response = requests.get(endpoint_url, timeout=15)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

            logging.info(f"VIN lookup successful. Status Code: {response.status_code}")
            return response.json()

        except requests.exceptions.Timeout:
            logging.error(f"Request to {endpoint_url} timed out.")
            raise
        except requests.exceptions.ConnectionError:
            logging.error(f"Connection error when sending to {endpoint_url}. Check URL and network.")
            raise
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err} - Response: {response.text}")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"An unexpected request error occurred: {e}")
            raise