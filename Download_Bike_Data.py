import os
import re

import requests
import xmltodict
import pandas as pd

# Function to download a file from a URL and save it locally
def download_file(url, filename):
    if os.path.isfile(filename):
        print("File already downloaded: " + filename)
    else:
        print("Downloading file: " + filename)
        r = requests.get(url, allow_redirects=True)
        open(filename, 'wb').write(r.content)

# Function to read the index file and return a list of files to download
def read_index(index_file):
    with open(index_file) as fd:
        doc = xmltodict.parse(fd.read())

    pattern = re.compile("^usage-stats/.*(2016|2017|2018|2019).csv$")

    entries = [item['Key'] for item in doc['ListBucketResult']['Contents']]
    return [f for f in entries if pattern.match(f)]
    
# Function to read the bike points file and return a dataframe
def import_bike_data(directory):
    """
    Read all CSV files in a directory and return them as a list of Pandas dataframes.
    """
    bike_data = []
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            try:
                bike_data_all = pd.read_csv(filepath)
                bike_data.append(bike_data_all)
                print(f"File {filename} successfully read.")
            except Exception as e:
                print(f"Error reading file {filename}: {e}")
    combined_bike_data = pd.concat(bike_data, ignore_index=True)
    
    # print(combined_bike_data.columns)
    return combined_bike_data

def pre_processing_bike_data(bike_data, year=None):
    
    #Check for duplicates and if there are any, drop them
    if bike_data.duplicated().sum() > 0:
        bike_data.drop_duplicates(inplace=True)
        print("Duplicates dropped.")
    else:
        print("No duplicates.")
        
    #Change column names
    bike_data.columns = bike_data.columns.str.lower().str.replace(' ', '_')
    
    print("Column names changed.", bike_data.columns)
    
    bike_data['start_date'] = bike_data['start_date'].str.slice(0, 16)  
    bike_data['end_date'] = bike_data['start_date'].str.slice(0, 16)  
    
    # Change the data type of the start_date and end_date columns to datetime
    bike_data['start_timestamp'] = pd.to_datetime(bike_data['start_date'],format='%d/%m/%Y %H:%M')
    bike_data['end_timestamp'] = pd.to_datetime(bike_data['end_date'],format='%d/%m/%Y %H:%M')
    
    #get the start_date and end_date columns from the start_timestamp and end_timestamp columns
    bike_data['start_date'] = bike_data['start_timestamp'].dt.date
    bike_data['end_date'] = bike_data['end_timestamp'].dt.date
    
    
    # get the day, month, year, hour, and day of the week from the start_timestamp column
    bike_data['start_day'] = bike_data['start_timestamp'].dt.day
    bike_data['start_month'] = bike_data['start_timestamp'].dt.month
    bike_data['start_year'] = bike_data['start_timestamp'].dt.year
    bike_data['start_hour'] = bike_data['start_timestamp'].dt.hour
    bike_data['start_day_of_week'] = bike_data['start_timestamp'].dt.day_name()
    
    # get the day, month, year, hour, and day of the week from the end_timestamp column
    bike_data['end_day'] = bike_data['end_timestamp'].dt.day
    bike_data['end_month'] = bike_data['end_timestamp'].dt.month
    bike_data['end_year'] = bike_data['end_timestamp'].dt.year
    bike_data['end_hour'] = bike_data['end_timestamp'].dt.hour
    bike_data['end_day_of_week'] = bike_data['end_timestamp'].dt.day_name()
    
    print("Date and time columns created.")
    
    bike_data['start_date'] = pd.to_datetime(bike_data['start_date'])

    # Set Holiday to 1 for specified dates
    holidays = [
        '2016-12-27', '2016-12-26', '2016-12-25', '2016-08-29', '2016-05-30',
        '2016-05-02', '2016-03-28', '2016-03-25', '2016-01-01',
        '2017-12-26', '2017-12-25', '2017-08-28', '2017-05-29',
        '2017-05-01', '2017-04-17', '2017-04-14', '2017-01-02',
        '2018-12-26', '2018-12-25', '2018-08-27', '2018-05-28',
        '2018-05-07', '2018-04-02', '2018-03-30', '2018-01-01',
        '2019-12-26', '2019-12-25', '2019-08-26', '2019-05-27',
        '2019-05-06', '2019-04-22', '2019-04-19', '2019-01-01',
    ]
    
    # set bank_holiday to 1 for all rows where the start_date is in the bank_holidays list
    bike_data['holiday'] = bike_data['start_date'].isin(holidays).astype(int)
    
    print("Holiday column created.", bike_data['holiday'].nunique())

    # set peak_hour to 1 for hours between between 06:30 and 09:30, and between 16:00 and 19:00 from Monday to Friday (not on public holidays)
    bike_data['peak_hour'] = ((bike_data['start_hour'].between(6, 9) | bike_data['end_hour'].between(6, 9) |
                               bike_data['start_hour'].between(16, 19) | bike_data['end_hour'].between(16, 19)) &
                              (bike_data['start_day_of_week'].isin(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']))).astype(int)
    
    print("Peak hour columns created.", bike_data['peak_hour'].nunique())
    
    bike_data['duration_minutes'] = bike_data['duration'] / 60
    
    print("Duration in Minutes column created.")
    
    #check if the year parameter is not None then filter the dataframe by the year
    if year is not None:
        bike_data = bike_data[bike_data['start_year'] == year]
        
    print("Dataframe filtered by year of: ", year)
    
    print( "Data pre-processing complete. With the following columns: \n", bike_data.columns)
    
    
    return bike_data

# Function to save the bike data to a CSV file
def save_bike_data(bike_data):
    """
    Save the bike data to a CSV file.
    """
    bike_data.to_csv('Final Data Files/bike_data_2016_2019.csv', index=False)
    print("Bike data saved to CSV file.")
    
def read_bike_data():
    """
    Read the bike data from a CSV file.
    """
    bike_data = pd.read_csv('Final Data Files/bike_data_2016_2019.csv')
    print("Bike data read from CSV file.")
    return bike_data
    

if __name__ == "__main__":
    BUCKET_URL = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/"
    INDEX_FILE = "Data Files/file-index.xml"
    RAW_TRIP_DIR = "Data Files/raw_trip/"

    if not os.path.exists(RAW_TRIP_DIR):
        os.makedirs(RAW_TRIP_DIR)

    download_file(BUCKET_URL, INDEX_FILE)

    files = read_index(INDEX_FILE)

    for file in files:
        local_file = RAW_TRIP_DIR + file.split('/')[-1].replace(' ', '_')
        url = BUCKET_URL + file

        download_file(url, local_file)
    
    bike_data = import_bike_data('Data Files/raw_trip')
    
    bike_data = pre_processing_bike_data(bike_data)
    
    save_bike_data(bike_data)