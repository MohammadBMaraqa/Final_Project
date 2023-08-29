import pandas as pd
import numpy as np
import os
import re
from io import StringIO

def load_csv_files(directory):
    """
    Load all CSV files from a folder and merge them into a single DataFrame.
    """
    demographics = []
    common_columns = ['borough_code','borough_name']
    regex_pattern = re.compile(r'Numerator|Denominator|Conf', re.IGNORECASE)

    for filename in os.listdir(directory):
        print(filename)
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            try:
                
                with open(filepath, 'r') as file:
                    lines = file.readlines()

                start_line = next(i for i, line in enumerate(lines) if "local authority" in line)
                end_line = next(i for i, line in enumerate(lines) if "Westminster" in line)

                # Extract the CSV data from lines and remove double quotes
                csv_data = "".join(line.replace('"', '') for line in lines[start_line:end_line+1])

                # Read the CSV data using pandas from the modified CSV string
                df = pd.read_csv(StringIO(csv_data))
                
                # rename local authority column to borough_name
                df.rename(columns={"mnemonic":"borough_code","local authority: district / unitary (as of April 2021)":"borough_name"}, inplace=True)

                # Check if the DataFrame contains the common columns
                if all(column in df.columns for column in common_columns):
                    columns_to_remove = [col for col in df.columns if regex_pattern.search(col)]
                    df = df.drop(columns=columns_to_remove)
                    
                    df.replace(to_replace=["*","!","#","-"],value=np.nan,inplace=True)
                    
                    df = filter_by_borough_code(df)
                    
                    demographics.append(df)
                    print(f"File {filename} successfully read.")
                else:
                    print(df.columns)
                    print(f"File {filename} does not contain the required common columns and will be skipped.")
            except Exception as e:
                print(f"Error reading file {filename}: {e}")

    if demographics:
        # Merge DataFrames using the common columns
        combined_demographics = pd.merge(demographics[0], demographics[1], on=common_columns, how='inner')
        for i in range(2, len(demographics)):
            combined_demographics = pd.merge(combined_demographics, demographics[i], on=common_columns, how='inner')
        return combined_demographics
    else:
        return None
    
def load_excel_file(filepath):
    """
    Load an Excel file with 10 sheets and perform an inner join on the specific columns.
    The specific columns used for joining are: 'Upper Tier Local Authority District code (2019)'
    and 'Upper Tier Local Authority District name (2019)'.
    
    Parameters:
        filepath (str): The path to the Excel file.
        
    Returns:
        pandas.DataFrame: The merged DataFrame.
    """
    common_columns = ['borough_code', 'borough_name']
    merged_data = None

    try:
        excel_data = pd.read_excel(filepath, sheet_name=None)
        for sheet_name, sheet_data in excel_data.items():
            
            sheet_data.rename(columns={
                    'Upper Tier Local Authority District code (2019)': 'borough_code',
                    'Upper Tier Local Authority District name (2019)': 'borough_name'
                }, inplace=True)
            
            if all(column in sheet_data.columns for column in common_columns):
                
                if merged_data is None:
                    merged_data = sheet_data
                else:
                    merged_data = pd.merge(merged_data, sheet_data, on=common_columns, how='inner')
            else:
                print(f"Sheet '{sheet_name}' in file '{filepath}' does not contain the required common columns and will be skipped.")
            
        if merged_data is not None:
            return merged_data
        else:
            print("No sheets with the required common columns were found in the Excel file.")
            return None
    except Exception as e:
        print(f"Error reading the Excel file '{filepath}': {e}")
        return None
    
def read_population_density(file_path):
    """
    Read the required rows of the 'population_estimates_2016_2019.xlsx' file and rename the column.

    Parameters:
        file_path (str): The path to the Excel file.

    Returns:
        pandas.DataFrame: The DataFrame with the required rows and renamed column.
    """
    try:
        # Read the first line (local authority) and including the end line (Westminster)
        data = pd.read_excel(file_path, skiprows=6, skipfooter=1)

        # Rename the column 'local authority: county / unitary (as of April 2021)' to 'borough_name'
        data.rename(columns={'local authority: county / unitary (as of April 2021)': 'borough_name'}, inplace=True)
        
        data = filter_by_borough_name(data)

        return data

    except Exception as e:
        print(f"Error reading the Excel file '{file_path}': {e}")
        return None
    
def read_house_prices(file_path):
    try:
        # Read the first line (local authority) and including the end line (Westminster)
        data= pd.read_excel(file_path, header=1, sheet_name="Average price", usecols="A:AH")
        data.rename(columns={"Unnamed: 0":"date"},inplace=True)
        
        #extract year from date column
        data['year'] = data['date'].dt.year
        
        # keep only rows with year 2019
        data = data[data['year'].isin([2016,2017,2018,2019])]
        
        
        # Define the borough columns to melt
        borough_columns = data.columns[1:]

        houses_by_borough = pd.melt(data, id_vars='date', value_vars=borough_columns, var_name='borough', value_name='value')

        # extract year and month from date column
        houses_by_borough['year'] = pd.DatetimeIndex(houses_by_borough['date']).year
        houses_by_borough['month'] = pd.DatetimeIndex(houses_by_borough['date']).month

        houses_by_borough.rename(columns={"value":"house_price", "borough":"borough_code"},inplace=True)
        
        houses_by_borough = filter_by_borough_code(houses_by_borough)
        
        return houses_by_borough

    except Exception as e:
        print(f"Error reading the Excel file '{file_path}': {e}")
        return None
    
    

def filter_by_borough_code(df):
    """
    Filter DataFrame to keep only rows with borough codes in the specified list.
    """
    borough_codes_to_keep = ['E09000007', 'E09000001', 'E09000020', 'E09000022', 'E09000013', 'E09000028',
                             'E09000025', 'E09000033', 'E09000032', 'E09000012', 'E09000030', 'E09000019']

    filtered_df = df[df['borough_code'].isin(borough_codes_to_keep)]
    return filtered_df


def filter_by_borough_name(df):
    """
    Filter DataFrame to keep only rows with borough codes in the specified list.
    """
    borough_names_to_keep = ['City of London', 'Camden', 'Hackney', 'Hammersmith and Fulham',
       'Islington', 'Kensington and Chelsea', 'Lambeth', 'Newham',
       'Southwark', 'Tower Hamlets', 'Wandsworth', 'Westminster']

    filtered_df = df[df['borough_name'].isin(borough_names_to_keep)]
    return filtered_df

def merge_on_borough_code(df1, df2):
    """
    Merge two DataFrames on the 'borough_code' column.
    """
    merged_df = pd.merge(df1, df2, on='borough_code', how='inner')
    return merged_df

def merge_on_borough_name(df1, df2):
    """
    Merge two DataFrames on the 'borough_code' column.
    """
    merged_df = pd.merge(df1, df2, on='borough_name', how='inner')
    return merged_df

# def preprocess_data(df):
#     """
#     Preprocess the data, handle missing values, and convert data types if needed.
#     """
#     # Preprocessing steps go here
#     # Example: Handle characters in numeric columns
    
#     df.replace(to_replace=["*","!","#","-"],value=np.nan,inplace=True)
    
    

#     return df

def show_insights(df):
    """
    Display some insights about the data.
    """
    # Show summary statistics
    # print("Summary Statistics:")
    # print(df.describe())

    # Show data types and non-null counts
    print("\nData Types and Non-Null Counts:")
    print(df.info())

    # # Show the first few rows of the DataFrame
    # print("\nSample Data:")
    # print(df.head())

def check_null_values(df):
    """
    Check and display null values in the DataFrame.
    """
    print("Null Values:")
    print(df.isnull().sum())
    

if __name__ == "__main__":
    folder_path_2016 = "Data Files/Geographic & Demographics Data/Demographics/2016/"
    folder_path_2017 = "Data Files/Geographic & Demographics Data/Demographics/2017/"  
    folder_path_2018 = "Data Files/Geographic & Demographics Data/Demographics/2018/"
    folder_path_2019 = "Data Files/Geographic & Demographics Data/Demographics/2019/"  
    
    indices_file_path = "Data Files/Geographic & Demographics Data/Demographics/File_11_-_IoD2019_Local_Authority_District_Summaries__upper-tier__.xlsx"

    house_prices_file_path = "Data Files/Geographic & Demographics Data/Demographics/UK House price index.xlsx"
    
    population_file_path = "Data Files/Geographic & Demographics Data/Demographics/population_estimates_2016_2019.xlsx"
    
    # Load all Excel sheets and merge them into a DataFrame
    demographics_2016 = load_csv_files(folder_path_2016)
    demographics_2017 = load_csv_files(folder_path_2017)
    demographics_2018 = load_csv_files(folder_path_2018)
    demographics_2019 = load_csv_files(folder_path_2019)
    
    indices_of_deprivation = load_excel_file(indices_file_path)
    
    indices_of_deprivation = filter_by_borough_code(indices_of_deprivation)
    
    print("demographics: ",demographics_2016.shape, demographics_2017.shape, demographics_2018.shape, demographics_2019.shape)
    print("indices",indices_of_deprivation.shape)
    
    houses = read_house_prices(house_prices_file_path)
    print("houses",houses.shape)
    
    population = read_population_density(population_file_path)
    print("population",population.shape)
    
    demographics_data_2019 = pd.merge(demographics_2019, indices_of_deprivation, on=['borough_code','borough_name'], how='inner')
    demographics_data_2019 = pd.merge(demographics_data_2019, population, on=['borough_name'], how='inner')
    
    demographics_data_2019.drop(columns=['2020','2021'],inplace=True)
    
    demographics_data_2019.rename(columns={"2016":"population_2016","2017":"population_2017","2018":"population_2018","2019":"population_2019"},inplace=True)
    
    print(demographics_data_2019.columns)
    