from __future__ import print_function
import requests
from bs4 import BeautifulSoup
import os
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import threading
import time



headers = {
    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'}

page = requests.get("https://www.rbi.org.in/Scripts/bs_viewcontent.aspx?Id=2009",headers=headers)

with open('html.txt') as f:
    lines = f.readlines()
page = ""
for line in lines:
    page += line

def create_csv(file_name,headers_list):
    df = pd.DataFrame(list(),columns=headers_list)
    df.to_csv(file_name)

def download_file(link):
    url = link["href"]
    local_filename = dir + url.split('/')[-1]
    # print("Downloading file: " + url + " | File Name: " + local_filename)
    headers = {
    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'}
    try:
        time_lock.acquire()
        time.sleep(1)
        global count
        count += 1
        print(count)
        time_lock.release()
        r = requests.get(url.replace("http:", "https:"),headers=headers,timeout=25)
        lock.acquire()
        try:
            if(r.status_code == 200):
                with open(local_filename, 'wb') as f:
                    f.write(r.content)
            data = pd.read_csv(full_file,index_col=0,low_memory=False)
            df = pd.read_excel(local_filename)  
            df.rename(columns = {"BANK NAME": "BANK","OFFICE":"BRANCH","CITY":"CITY1","DISTRICT":"CITY2"},inplace=True)
            result = pd.concat([data,df],ignore_index=True)
            result.to_csv(full_file)
            
        except Exception as e:
            print(e,url)
            untrked_url.append(url)
        os.remove(local_filename)
        lock.release()
        
        
    except Exception as e:
        print(e,url)
        untrked_url.append(url)


soup = BeautifulSoup(page, 'html.parser')

ifsc_table = soup.find_all('table', class_='tablebg')[0]

ifsc_links = ifsc_table.find_all('a')

untrked_url = []

lock = threading.Lock()

time_lock = threading.Lock()

count = 0

t = time.localtime()
current_time = time.strftime("%H:%M:%S", t)
dir = "./" +current_time + "/"
os.mkdir(dir)
full_file = dir + "all_ifsc.csv"
headers_list = ["BANK","IFSC","BRANCH","ADDRESS","CITY1","CITY2","STATE","STD CODE","PHONE"]
create_csv(full_file,headers_list)
untrked_file = dir + "untracked.csv"
create_csv(untrked_file,["url"])
print(len(ifsc_links))


with ThreadPoolExecutor (max_workers=5) as pool:
    pool.map(download_file,ifsc_links[:])

if(len(untrked_url) > 0):
    df = pd.read_csv(untrked_file,index_col=0)
    df["url"] = untrked_url
    df.to_csv(untrked_file)