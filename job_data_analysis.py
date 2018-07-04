import boto3 
import string 
import pandas as pd
import os 
import kaggle
from nltk.corpus import stopwords
from pattern3.text.en import singularize
from datetime import datetime

def save_to_s3(new_df):
    aws_access_key_id = ''
    aws_secret_access_key = ''
    bucket = 'seektest01'
    filename = 'job_data.csv'
    new_df.to_csv(filename)
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3.Object(bucket, 'seek/'+filename).put(Body=open(filename, 'rb'))
    print('done')


def replace_na_val(extract_job_data, custom='value'):
      extract_job_data['Duration'] = extract_job_data['Duration'].fillna(custom)

def clean_job_resp_col(extract_job_data):
    stop_words = set(stopwords.words('english'))
    remove_punc = str.maketrans('', '', string.punctuation)
    extract_job_data['JobDescription'] = extract_job_data['JobDescription']\
        .apply(lambda x: ' '.join([singularize(word) for word in str(x).translate(remove_punc).split() if word.lower() not in (stop_words)]))
    print('done')

def most_job_ads_month(job_data):
    print(job_data.groupby('Month')['Month'].count().idxmax())

def most_num_job_ads(job_data):
    print(job_data[job_data['Year'] >= (datetime.now().year - 4)].groupby('Company')['Company'].count().idxmax())

def read_csv_file():
    path = os.getcwd()
    file_name = "data job posts.csv"
    url = 'https://www.kaggle.com/madhab/jobposts/downloads/data%20job%20posts.csv'
    k =  kaggle.download_from_kaggle(url, file_name, path)
    df = pd.read_csv(file_name)
    return df

def main():
    job_data = read_csv_file()
    extract_job_data = job_data.loc[:,['Title', 'Duration', 'Location', 'JobDescription', 'JobRequirment' ,'RequiredQual', 'Salary', 'Deadline', 'AboutC']]
    most_num_job_ads(job_data)
    most_job_ads_month(job_data)
    clean_job_resp_col(extract_job_data)
    replace_na_val(extract_job_data)
    #create new data frame 
    new_df = extract_job_data.loc[:,:]
    #save to s3
    save_to_s3(new_df)


if __name__ == '__main__':
    main()