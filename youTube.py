import json
from googleapiclient.discovery import build
import pymongo
import psycopg2


#api connection
Api_id="AIzaSyCZYNWNefpOxIR4yA83zPdBtYhryPjwQeQ"
api_service_name="youtube"
api_version="v3"
youtube=build(api_service_name,api_version,developerKey=Api_id)
channel_id=["UCGlvOc96oNEMd3Aqi4OtCCQ,UCotJ5XeDL1JGbTlSaA0DxBA,UCqW8jxh4tH1Z1sWPbkGWL4g,UCEuOwB9vSL1oPKGNdONB4ig,UCpIkceSRGakqavix5m4ybYA,UCq-Fj5jknLsUf-MWSy4_brA "]
channel_id_str=",".join(channel_id)
#get channel information
def channeldata(channel_id):
    finaldata=[]
    request=youtube.channels().list(
     part="snippet,statistics,status,contentDetails",
     id=channel_id_str
) 
    response=request.execute()
    for i in range(6):
        finalinfo=dict(channel_ID=response['items'][i]['id'],
                       channel_name=response['items'][i]['snippet']['title'],
                       channel_viewCount=response['items'][i]['statistics']['viewCount'],
                       channel_description=response['items'][i]['snippet']['description'],
                       channel_status=response['items'][i]['status'],
                       channel_subscriberCount=response['items'][i]['statistics']['subscriberCount'])
        finaldata.append(finalinfo)               
    return finaldata
channel_information=channeldata(channel_id_str)
print(channel_information)
channel_information_json=json.dumps(channel_information,indent=2,sort_keys=True)
print('------------------------------------')
print(channel_information_json)
#connection to mongodb
client=pymongo.MongoClient("mongodb+srv://sunitasony0:Sony%40123@cluster0.a0mezym.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]
def channel_information(channel_id_str):
  ch_details=channeldata(channel_id_str)
  coll1=db['channel_details1']

  coll1.insert_one({"channel_info":ch_details})
  return "upload completed sucessfully "

insert=channel_information(channel_id)

# connection to sql
mydb=psycopg2.connect(host="localhost",
                      user="User1",
                      password="test1234",
                      database="youtube_data1",
                      port="5432")
cursor=mydb.cursor()

create_query='''create table if not exists channels(channel_ID VARCHAR(255) PRIMARY KEY,
                                                        channel_name VARCHAR(255),
                                                        channel_viewCount INT,
                                                        channel_description TEXT,
                                                        channel_status VARCHAR(255),
                                                        channel_subscriberCount INT)'''

cursor.execute(create_query)
mydb.commit()



