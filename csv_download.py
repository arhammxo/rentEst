import requests

url = "https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv"
params = {
    't': '1739990509'  # This appears to be a cache-busting timestamp parameter
}

try:
    response = requests.get(url, params=params)
    response.raise_for_status()  # Check for HTTP errors
    
    with open("zillow_rent_data.csv", "wb") as f:
        f.write(response.content)
    print("File downloaded successfully")
except Exception as e:
    print(f"Error downloading file: {e}")