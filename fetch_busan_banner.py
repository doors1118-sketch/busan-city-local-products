import requests
import pandas as pd
import urllib.parse
import sys

def fetch_and_save_banners():
    url = 'http://apis.data.go.kr/6260000/BusanTblBnrInfoService/getTblBnrInfo'
    serviceKey = '981c3db97758eb447536b0061f60e559bc785662ac531bbf04d057024073baf3'
    
    # URL decode service key
    decoded_key = urllib.parse.unquote(serviceKey)
    
    params = {
        'serviceKey': decoded_key,
        'pageNo': '1',
        'numOfRows': '1000',
        'resultType': 'json'
    }
    
    print("Fetching data from API...")
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if 'response' in data and 'body' in data['response']:
            body = data['response']['body']
            total_count = body.get('totalCount', 0)
            print(f"Total entries found: {total_count}")
            
            items = body.get('items', {}).get('item', [])
            
            if not items:
                print("No items found in the response.")
                return
                
            df = pd.DataFrame(items)
            
            output_file = 'busan_banner_info.xlsx'
            df.to_excel(output_file, index=False)
            print(f"Successfully saved {len(df)} items to {output_file}")
            
        else:
            print("Unexpected API response structure:")
            print(data)
            
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_and_save_banners()
