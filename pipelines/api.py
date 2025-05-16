from time import sleep
import requests, csv, os
from datetime import datetime
from utils.constants import AWS_BUCKET_NAME, OUTPUT_PATH
from aws_etl import connect_to_s3, create_bucket_if_not_exist, upload_to_s3

file_postfix = datetime.now().strftime("%Y_%m_%d")
output_dir = f"{OUTPUT_PATH}/{file_postfix}"

s3 = connect_to_s3()
AWS_BUCKET_NAME = f"{AWS_BUCKET_NAME}/{file_postfix}"

def upload_s3(s3, file_path):
    create_bucket_if_not_exist(s3, AWS_BUCKET_NAME)
    upload_to_s3(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])

def _seasons(url = "https://api.jolpi.ca/ergast/f1/seasons?limit=100"):
    """
    Fetches F1 seasons data from the Jolpica API (JSON format)

    Args:
        url: The URL of the API endpoint.
    """
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()["MRData"]["SeasonTable"]["Seasons"]
        seasons = [[item.get("season")] for item in data]

        # save data locally to '/data/{file_postfix}/' folder
        os.makedirs(output_dir, exist_ok=True) # Create the output directory if it doesn't exist
        file_path = f"{output_dir}/seasons_{file_postfix}.csv"
        header = ["season"]
        
        with open(file_path, "w", newline ='') as f:
            write = csv.writer(f)
            write.writerow(header)
            write.writerows(seasons)
        
        sleep(2)
        print("Fetched season data")
        upload_s3(s3, file_path)
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def _circuits(url = "https://api.jolpi.ca/ergast/f1/circuits?limit=100"):
    """
    Fetches F1 seasons data from the Jolpica API (JSON format)

    Args:
        url: The URL of the API endpoint.
    """
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()["MRData"]["CircuitTable"]["Circuits"]

        circuits = []
        circuit_id = 1
        for item in data:
            circuit_ref = item.get("circuitId")
            circuit_name = item.get("circuitName")
            circuit_country = item.get("Location", {}).get("country")  # Handle potential missing 'Location'

            circuits.append([circuit_id, circuit_ref, circuit_name, circuit_country])
            circuit_id += 1

        # save data locally to '/data/{file_postfix}/' folder
        os.makedirs(output_dir, exist_ok=True) # Create the output directory if it doesn't exist
        file_path = f"{output_dir}/circuits_{file_postfix}.csv"
        header = ["circuit_id","circuit_ref","circuit_name","circuit_country"]
        
        with open(file_path, "w", newline ='', encoding='utf-8') as f:
            write = csv.writer(f)
            write.writerow(header)
            write.writerows(circuits)
        
        sleep(2)
        print("Fetched circuit data")
        upload_s3(s3, file_path)
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def _constructors():
    """
    Fetches constructors data from the Jolpica API (JSON format) and saves it to a csv file.
    """
    try:
        sleep(60)
        with open(f'{output_dir}/seasons_{file_postfix}.csv', 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            _ = next(reader, None)  # Read and store the header, handle empty file
            seasons = [row[0] for row in reader if row]
            
        existing_constructor_refs = set()
        constructors = []
        constructor_id = 1

        for season in seasons:
            url = f"https://api.jolpi.ca/ergast/f1/{int(season)}/constructors"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()["MRData"]["ConstructorTable"]["Constructors"]

            for constructor in data:
                constructor_ref = constructor.get("constructorId")
                if constructor_ref not in existing_constructor_refs:
                    existing_constructor_refs.add(constructor_ref)
                    constructors.append([constructor_id,
                                        constructor_ref, 
                                        constructor.get("name"), 
                                        constructor.get("nationality")])
                    constructor_id += 1
            sleep(5)
            
        # save data locally to '/data/{file_postfix}/' folder
        os.makedirs(output_dir, exist_ok=True) # Create the output directory if it doesn't exist
        file_path = f"{output_dir}/constructors_{file_postfix}.csv"
        header = ["constructor_id","constructor_ref","constructor_name","constructor_nationality"]
        
        with open(file_path, "w", newline ='', encoding='utf-8') as f:
            write = csv.writer(f)
            write.writerow(header)
            write.writerows(constructors)
        
        sleep(5)
        print("Fetched constructor data")
        upload_s3(s3, file_path)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def _drivers(url = "https://api.jolpi.ca/ergast/f1/drivers?limit=100"):
    """
    Fetches driver data from the Jolpica API (JSON format) and saves it to a csv file.

    Args:
        url: The URL of the API endpoint.
    """
    try:
        sleep(60)
        full_driver_data = []
        offset = 0

        # Keep fetching data samples until the API returns nothing
        while True:
            url_offset = f'{url}&offset={offset}'
            response = requests.get(url_offset)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()["MRData"]["DriverTable"]["Drivers"]

            if not data:
                break

            full_driver_data.extend(data)

            # fetch next batch of 100 data samples from API
            offset += 100
            sleep(10)

        driver_id = 1
        drivers = []

        for driver in full_driver_data:
            drivers.append([driver_id,
                            driver["driverId"],
                            driver["givenName"],
                            driver["familyName"],
                            driver["dateOfBirth"],
                            driver["nationality"]])
            driver_id += 1

        # save data locally to '/data/{file_postfix}/' folder
        os.makedirs(output_dir, exist_ok=True) # Create the output directory if it doesn't exist
        file_path = f"{output_dir}/drivers_{file_postfix}.csv"
        header = ["driver_id","driver_ref","first_name","last_name","dob","nationality"]

        with open(file_path, "w", newline ='', encoding='utf-8') as f:
            write = csv.writer(f)
            write.writerow(header)
            write.writerows(drivers)
        
        sleep(10)
        print("Fetched driver data")
        upload_s3(s3, file_path)
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def _races():
    """
    Fetches race data from the Jolpica API (JSON format) and saves it to a csv file.
    """
    try:
        sleep(60)
        with open(f'{output_dir}/seasons_{file_postfix}.csv', 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            _ = next(reader, None)  # Read and store the header, handle empty file
            seasons = [row[0] for row in reader if row]

        circuits = {}
        with open(f'{output_dir}/circuits_{file_postfix}.csv', 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            _ = next(reader, None)
            for row in reader:
                circuits[f"{row[1]}"] = int(row[0])

        races = []
        race_id = 1

        for season in seasons:
            url = f"https://api.jolpi.ca/ergast/f1/{int(season)}/races"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()["MRData"]["RaceTable"]["Races"]
            for race in data:
                races.append([race_id,
                            race.get("season"),
                            race.get("round"),
                            race.get("raceName"),
                            circuits.get(race.get("Circuit", {}).get("circuitId")),
                            race.get("date")])
                race_id += 1
            sleep(15)
        
        # save data locally to '/data/{file_postfix}/' folder
        os.makedirs(output_dir, exist_ok=True) # Create the output directory if it doesn't exist
        file_path = f"{output_dir}/races_{file_postfix}.csv"
        header = ["race_id","season","round","race_name","circuit_id","date"]

        with open(file_path, "w", newline ='', encoding='utf-8') as f:
            write = csv.writer(f)
            write.writerow(header)
            write.writerows(races)
        
        sleep(10)
        print("Fetched race data")
        upload_s3(s3, file_path)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def _quali():
    """
    Fetches qualifying results data from the Jolpica API (JSON format) and saves it to a csv file.
    """
    try:
        sleep(60)
        with open(f'{output_dir}/seasons_{file_postfix}.csv', 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            _ = next(reader, None)  # Read and store the header, handle empty file
            seasons = [row[0] for row in reader if row]

        races = {}
        with open(f'{output_dir}/races_{file_postfix}.csv', 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            _ = next(reader, None)  # Read and store the header, handle empty file
            for row in reader:
                races[(row[1],row[2])] = int(row[0])
        
        drivers = {}
        with open(f'{output_dir}/drivers_{file_postfix}.csv', 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            _ = next(reader, None)  # Read and store the header, handle empty file
            for row in reader:
                drivers[f"{row[1]}"] = int(row[0])
        
        constructors = {}
        with open(f'{output_dir}/constructors_{file_postfix}.csv', 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            _ = next(reader, None)  # Read and store the header, handle empty file
            for row in reader:
                constructors[f"{row[1]}"] = int(row[0])
        
        quali_seasons = seasons[44:] # start from 1994
        quali_id = 1
        quali = []

        for season in quali_seasons:

            total_season_data = []
            offset = 0

            while True:
                url = f"https://api.jolpi.ca/ergast/f1/{season}/qualifying?limit=100&offset={offset}"
                response = requests.get(url)
                response.raise_for_status()
                season_data = response.json()["MRData"]["RaceTable"]["Races"]

                if not season_data:
                    break

                total_season_data.extend(season_data)
                offset += 100
                sleep(5)

            # For season with no races
            if not total_season_data:
                break

            for round in range(len(total_season_data)):
                round_data = dict(total_season_data[round]) # Need to cycle through each of the rounds of a season
                round_results = round_data["QualifyingResults"]

                for driver in range(len(round_results)):
                    current_driver = round_data["QualifyingResults"][driver]
                    driver_quali_result = [
                        quali_id,
                        races.get((round_data["season"],round_data["round"])),
                        drivers.get(current_driver["Driver"]["driverId"]),
                        constructors.get(current_driver["Constructor"]["constructorId"]),
                        int(current_driver["position"]),
                        current_driver.get("Q1"),
                        current_driver.get("Q2"),
                        current_driver.get("Q3")
                    ]
                    quali.append(driver_quali_result)
                    quali_id += 1

        # save data locally to '/data/{file_postfix}/' folder
        os.makedirs(output_dir, exist_ok=True) # Create the output directory if it doesn't exist
        file_path = f"{output_dir}/quali_{file_postfix}.csv"
        header = ['quali_id','race_id', 'driver_id', 'constructor_id', 'grid_position', 'q1', 'q2', 'q3']              
        
        with open(file_path, "w", newline ='', encoding='utf-8') as f:
            write = csv.writer(f)
            write.writerow(header)
            write.writerows(quali)
        
        sleep(10)
        print("Fetched quali data")
        upload_s3(s3, file_path)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
