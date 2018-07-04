import requests
import os

def download_from_kaggle(file_url, filename, path):
    url = file_url
    saving_loc = path
    file_name = filename

    login_info = {'UserName': "username", 'Password': "password"}
    req = requests.get(url)
    req = requests.post(req.url, data = login_info, stream=True)
    
    file = open(saving_loc + '\\'+file_name , 'w')
    for chunk in req.iter_content(8192):
        if chunk:
            file.write(chunk)
    file.close()
    return 'Success'