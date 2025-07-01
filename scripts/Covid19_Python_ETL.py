# -----------------------------
# Required Python Packages
# -----------------------------
import boto3                       # AWS SDK for Python
import pandas as pd                # Data analysis library
from io import StringIO            # For handling CSV string I/O
import time                        # To wait for Athena query completion
from typing import Dict            # For function type hints



###############################################################################################################
# 1. Connect to Athena and query data  - Extraction/Ingestion
###############################################################################################################

# --------------------------------------------------------------------------------------------------------------------
# Configuration & Constants
# --------------------------------------------------------------------------------------------------------------------
# ⚠️ Do NOT hardcode credentials in production!
# Delete them if you share them
AWS_ACCESS_KEY = "AKIAZDSAVGW7IJPPOUNG"           
AWS_SECRET_KEY = "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"           
AWS_REGION = "us-east-1"
SCHEMA_NAME = "covid_19"
S3_STAGING_DIR = "s3://de-covid19-athena-output/output/"
S3_BUCKET_NAME = "de-covid19-athena-output"
S3_OUTPUT_DIRECTORY = "output"

# -----------------------------
# Athena Client Setup
# -----------------------------
# Connect to Athena using boto3 client
athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,              
    aws_secret_access_key=AWS_SECRET_KEY,          
    region_name=AWS_REGION
)

# -----------------------------
# Function: Download Athena Query Result from S3
# -----------------------------
Dict = {}
def download_and_load_query_results(client: boto3.client, query_response: Dict) -> pd.DataFrame:
    """
    Polls Athena until the query completes, then downloads and loads the result CSV from S3.
    Returns: pandas.DataFrame with up to the first 1000 rows.
    """
    # Wait for query to complete
    while True:
        try:
            # This function only loads the first 1000 rows
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(1)
            else:
                raise err

    # Download result from S3
    temp_file_location = "athena_query_results.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    # Write query results to temp_file_location CSV file to local machine
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",  
        temp_file_location,
    )

    # Load the CSV into a DataFrame
    return pd.read_csv(temp_file_location)

# -----------------------------------------------------------------------------------------------------------------
# Execute Athena Query1 for table enigma_jhud
# -----------------------------------------------------------------------------------------------------------------
# Sends a SQL query (SELECT * FROM enigma_jhud) to Athena.
# Sample query to fetch data from 'enigma_jhud' table
response_enigma_jhud = athena_client.start_query_execution(
    QueryString="SELECT * FROM enigma_jhud",   
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={                      
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    }
)
#print(response_enigma_jhud)

# Wait and fetch query results as DataFrame
enigma_jhud_df = download_and_load_query_results(athena_client, response_enigma_jhud)
# print(enigma_jhud_df.head())  # Show preview of result

# -----------------------------------------------------------------------------------------------------------------
# Execute Athena Query2 for table 2 : countrycode
# -----------------------------------------------------------------------------------------------------------------
response_countrycode= athena_client.start_query_execution(
    QueryString="SELECT * FROM countrycode",   
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={                      
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    }
)
#print(response_countrycode)

countrycode_df = download_and_load_query_results(athena_client, response_countrycode)
#print(countrycode_df.head())  # Show preview of result

# -----------------------------------------------------------------------------------------------------------------
# Execute Athena Query3 for table 3: countypopulation
# -----------------------------------------------------------------------------------------------------------------
response_countypopulation= athena_client.start_query_execution(
    QueryString="SELECT * FROM countypopulation",   
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={                      
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    }
)
#print(response_countypopulation)

countypopulation_df = download_and_load_query_results(athena_client, response_countypopulation)
#print(countypopulation_df.head())  # Show preview of result

# -----------------------------------------------------------------------------------------------------------------
# Execute Athena Query4 for table 4: rearc_usa_hospital_beds
# -----------------------------------------------------------------------------------------------------------------
response_hospital_beds= athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_usa_hospital_beds",   
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={                      
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    }
)
#print(response_hospital_beds)

hospital_beds_df = download_and_load_query_results(athena_client, response_hospital_beds)
#print(hospital_beds_df.head())  # Show preview of result

# -----------------------------------------------------------------------------------------------------------------
# Execute Athena Query5 for table 5: state_abv
# -----------------------------------------------------------------------------------------------------------------
response_state_abv= athena_client.start_query_execution(
    QueryString="SELECT * FROM state_abv",   
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={                      
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    }
)
#print(response_state_abv)

state_abv_df = download_and_load_query_results(athena_client, response_state_abv)
#print(state_abv_df.head())  # Show preview of result

# Headers read as first row, convert firat row to header back
# Grab the first row as header
new_header = state_abv_df.iloc[0]
state_abv_df = state_abv_df[1:] # truncuate header
state_abv_df.columns = new_header
# print(state_abv_df.head())



# -----------------------------------------------------------------------------------------------------------------
# Execute Athena Query6 for table 6: states_daily
# -----------------------------------------------------------------------------------------------------------------
response_states_daily= athena_client.start_query_execution(
    QueryString="SELECT * FROM states_daily",   
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={                      
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    }
)
#print(response_states_daily)

states_daily_df = download_and_load_query_results(athena_client, response_states_daily)
#print(states_daily_df.head())  # Show preview of result
# -----------------------------------------------------------------------------------------------------------------
# Execute Athena Query7 for table 7: us_county
# -----------------------------------------------------------------------------------------------------------------
response_us_county= athena_client.start_query_execution(
    QueryString="SELECT * FROM us_county",   
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={                      
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    }
)
#print(response_us_county)

us_county_df = download_and_load_query_results(athena_client, response_us_county)
#print(us_county_df.head())  # Show preview of result

# -----------------------------------------------------------------------------------------------------------------
# Execute Athena Query8 for table 8: us_daily
# -----------------------------------------------------------------------------------------------------------------
response_us_daily= athena_client.start_query_execution(
    QueryString="SELECT * FROM us_daily",   
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={                      
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    }
)
#print(response_us_daily)

us_daily_df = download_and_load_query_results(athena_client, response_us_daily)
#print(us_daily_df.head())  # Show preview of result

# -----------------------------------------------------------------------------------------------------------------
# Execute Athena Query9 for table 9: us_states
# -----------------------------------------------------------------------------------------------------------------
response_us_states= athena_client.start_query_execution(
    QueryString="SELECT * FROM us_states",   
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={                      
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    }
)
#print(response_us_states)


us_states_df = download_and_load_query_results(athena_client, response_us_states)
#print(us_states_df.head())  # Show preview of result
# -----------------------------------------------------------------------------------------------------------------
# Execute Athena Query10 for table 10: us_total_latest
# -----------------------------------------------------------------------------------------------------------------
response_us_total_latest= athena_client.start_query_execution(
    QueryString="SELECT * FROM us_total_latest",   
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={                      
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    }
)
#print(response_us_total_latest)

us_total_latest_df = download_and_load_query_results(athena_client, response_us_total_latest)
#print(us_total_latest_df.head())  # Show preview of result
