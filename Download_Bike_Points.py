import requests
import pandas as pd

def get_bike_points():
    
    # Get the bike points data from the TFL API
    try:
        response = requests.get("https://api.tfl.gov.uk/BikePoint/")
        
        response.raise_for_status()
        
        res = response.json()
        
        data = []
        
        for x in res:
            
            #Change column names
            x['bike_point_id'] = x.pop('id')
            x['bike_station_name'] = x.pop('commonName')
            x['station_longitude'] = x.pop('lon')
            x['station_latitude'] = x.pop('lat')
            
            #Drop columns not needed
            x.pop('placeType')
            x.pop('children')
            x.pop('childrenUrls')
            x.pop('url')
            x.pop('$type')
            
            #Add additional properties to the dataframe
            for additionalProperty in x['additionalProperties']:
                if additionalProperty['key'] == 'NbBikes':
                    x['number_of_bikes'] = additionalProperty['value']
                elif additionalProperty['key'] == 'NbEmptyDocks':
                    x['number_empty_docks'] = additionalProperty['value']
                elif additionalProperty['key'] == 'NbDocks':
                    x['number_of_docks'] = additionalProperty['value']
                elif additionalProperty['key'] == 'NbEBikes':
                    x['number_ebikes'] = additionalProperty['value']
                elif additionalProperty['key'] == 'NbStandardBikes':
                    x['number_standard_bikes'] = additionalProperty['value']
            
            x.pop('additionalProperties')
                    
            data.append(x)
                
        return pd.DataFrame(data)    
        
                
    
    except requests.exceptions.HTTPError as err:
        print(err)
        return None
    
# Function that pre-processes the bike_points.csv file
def pre_process_bike_points(bike_points):
    """
    Pre-processes the bike_points.csv file.
    """
    
    # change the data type of number_of_bikes, number_empty_docks, number_of_docks, number_standard_bikes, and number_ebikes to int
    bike_points['number_of_bikes'] = bike_points['number_of_bikes'].astype(int)
    bike_points['number_empty_docks'] = bike_points['number_empty_docks'].astype(int)
    bike_points['number_of_docks'] = bike_points['number_of_docks'].astype(int)
    bike_points['number_standard_bikes'] = bike_points['number_standard_bikes'].astype(int)
    bike_points['number_ebikes'] = bike_points['number_ebikes'].astype(int)
    bike_points["bike_station_id"] = bike_points["bike_point_id"].str.extract('(\d+)').astype(int)
    
    bike_points.drop(['bike_point_id'], axis=1, inplace=True)
    
    return bike_points

def read_bike_points():
    """
    Read the bike points data from the CSV file.
    """
    return pd.read_csv('./Final Data Files/bike_points.csv')
    
# Function to save the bike points data to Final Data Files
def save_bike_points(bike_points):
    """
    Save the bike points data to a CSV file.
    """
    bike_points.to_csv('./Final Data Files/bike_points.csv', index=False)
    print("Bike points data saved to CSV file.")
    
    

if __name__ == "__main__":
    bike_points = get_bike_points()
    
    bike_points = pre_process_bike_points(bike_points)
    
    save_bike_points(bike_points)