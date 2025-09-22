""""
Python Extract Transform Load 
"""

# %%
# Import tools to get data from the internet, handle data, and save data to a database
import requests ## for getting data from online sources, in this case, an API
import pandas as pd # for organizing and changing data
from sqlalchemy import create_engine # for connecting to a database

def extract() -> dict: # Declaring function for getting API data
    """ Get spell data from an online Harry Potter source
    https://hp-api.onrender.com/?ref=freepublicapis.com
    """
    API_URL = "https://hp-api.onrender.com/api/spells"
    data = requests.get(API_URL).json() # Download and convert data to a dictionary
    return data # Give back the downloaded data
# %%
def transform(data:dict) -> pd.DataFrame: # Declaring function for cleaning and Feature Engineering
    """ Change the dictionary to a table, keep spells starting with 'A' only """
    df = pd.DataFrame(data)  # Convert the data into a table format
    df = df[df["name"].str.startswith("A", na=False)] # Keep spells that start with 'A'
    df = df.sort_values(by="name", ascending=False) # Order spells by name from Z to A
    df = df.reset_index(drop=True) # Fix row numbering after filtering and sorting
    return df[["name","description"]]  # Only keep the name and description columns 

# %%
MYSQL_CONNECTION_STRING = 'mysql+mysqlconnector://root:######@###.#.#.#:3306/harry_potter_db' # Connection settings to my MySQL database

def load(df: pd.DataFrame) -> None: # Declaring function that takes the clean data and saves it into the database
    """ Open a connection to the database and save the data as a new table """
    engine = create_engine(MYSQL_CONNECTION_STRING)  # Connect to the database
    df.to_sql('spells_a', con=engine, if_exists='replace', index=False) # Save data in table named 'spells_a'

# %%
data = extract() # Step 1: Get the data from the API
df = transform(data) # Step 2: Keep and order the data we want
df.to_csv('a-spells.csv', index=False) # Step 3: Put the processed data into a csv file for displaying data
load(df) # Step 4: Put the processed data into the database
