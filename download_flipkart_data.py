import requests
import pandas as pd 
from concurrent.futures import ThreadPoolExecutor
import threading

lock = threading.Lock()
count = 0

def create_csv(file_name,headers_list):
    df = pd.DataFrame(list(),columns=headers_list)
    df.to_csv(file_name)

data_file = "current_ifsc.csv"
headers = ["ifsc","micr","address","contact","city","district","state","status","version","bank_branch_id","branch_name","bank_id","bank_name","created_at","updated_at"]
create_csv(data_file,headers)
bank_id_exist = 0
bank_id_not_exist = 0
df = pd.read_csv(data_file,index_col=0)

def add_bank(bank_id):
    global bank_id_exist
    global bank_id_not_exist
    bank_id = str(bank_id)
    url = "http://10.24.2.60/bank/" + bank_id + "/branches?include_inactive=false&page_number=1&page_size=1000"
    r = requests.get(url)
    if(r.status_code == 200): 
        bank_id_exist += 1
        result = r.json()
        lock.acquire()
        global count
        count += 1
        print(count)
        for branch in result["bank_branches"]:
            df.loc[len(df)] = branch
        df.to_csv(data_file)
        lock.release()
    elif(r.status_code == 404): bank_id_not_exist += 1

with ThreadPoolExecutor(max_workers=5) as pool:
    for result in pool.map(add_bank,range(0,250)):
        if(result): print(result)
    
print(bank_id_exist , bank_id_not_exist)
    


