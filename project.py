from googleapiclient.discovery import build
from pprint import pprint
import pandas as pd
from datetime import datetime
from datetime import timezone
import dateutil.parser
import streamlit as st
def Api_connection():
    api_key='AIzaSyAi8H2meOsYFAbkZMWsxbBPc5RcJZLE-JA'
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube
youtube=Api_connection()
def channel_details(channel_id):  
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    for i in response['items']:
        data=dict(
                  channel_name=i['snippet']['title'],
                  channel_id=i['id'],
                  channel_type=i['kind'],
                  channel_views=i['statistics']['viewCount'],
                  channel_des=i['snippet']['description'],
                  channel_subs=i['statistics']['subscriberCount'],
                  channel_videos=i['statistics']['videoCount'])
    return data
def get_channel_videoid(channel_id):
    Video_ids=[]
    request = youtube.channels().list(id=channel_id,part='contentDetails').execute()
    Playlist_Id=request['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    response1=youtube.playlistItems().list(
        part='snippet',
        playlistId=Playlist_Id,
        maxResults=50).execute()
    for i in range(len(response1['items'])):
        Video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    return Video_ids
def video_details(video_ids):
    video_informations=[]
    for video_id in video_ids:
        request1 = youtube.videos().list(part="snippet,contentDetails,statistics",id=video_id).execute()
        for i in request1['items']:
            data=dict(
                video_id= i['id'],
                video_name= i['snippet']['title'],
                vchannel_name=i['snippet']['channelTitle'],
                video_des= i['snippet']['description'],
                video_viewcount= i['statistics']['viewCount'],
                video_comment= i['statistics']['commentCount'],
                video_likes= i['statistics']['likeCount'],
                video_publisheddate= i['snippet']['publishedAt'],
                video_duration= i['contentDetails']['duration'])
        video_informations.append(data)
    return video_informations
def comment_details(Video_ids):
    Comment_info=[]
    try:
        for video_id in Video_ids:
            request2 = youtube.commentThreads().list(part="snippet,replies",videoId=video_id,maxResults=10).execute()
            for i in request2['items']:
                data=dict(
                    comment_id=i['id'],
                    comment_video_id=i['snippet']['videoId'],
                    comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published_date=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_info.append(data)
        return Comment_info
    except:
        pass
import pymongo
connection = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
db = connection['Youtube_Project']
collection1=db['Youtube_Data']
def Youtube_Channel(channel_id):
    channel_info=channel_details(channel_id)
    videosid_info=get_channel_videoid(channel_id)
    videos_info=video_details(videosid_info)
    comment_info=comment_details(videosid_info)
    db.collection1.insert_one({"Channel_information":channel_info,"videos_information":videos_info,"comment_information":comment_info})
    return "Data stored successfully in MongoDB"
import mysql.connector
connection=mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345678',
    database = 'Youtube_Project' #using database d101
)
cursor = connection.cursor()
def Channels_Table():
    connection=mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345678',
    database = 'Youtube_Project')
    cursor = connection.cursor()
    drop_table= '''drop table if exists Channel'''
    cursor.execute(drop_table)
    connection.commit()
    query="""create table Channel(
                                  channel_name varchar(255),
                                  channel_id varchar(255) primary key,
                                  channel_type varchar(255),
                                  channel_views bigint,
                                  channel_des TEXT,
                                  channel_subs bigint,
                                  channel_videos int
                                  )"""
    cursor.execute(query)
    Channel_lists=[]
    for i in db.collection1.find({},{"_id":0,"Channel_information":1}):
         Channel_lists.append(i["Channel_information"])
    df=pd.DataFrame(Channel_lists)
    for x,y in df.iterrows():
        query='''insert into Channel(channel_name,channel_id,channel_type,
                                      channel_views,channel_des,channel_subs,channel_videos)
                                      values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(y['channel_name'],
                y['channel_id'],
                y['channel_type'],
                y['channel_views'],
                y['channel_des'],
                y['channel_subs'],
                y['channel_videos'])
        cursor.execute(query,values)
        connection.commit()
def Videos_Table():
    connection=mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345678',
    database = 'Youtube_Project')
    cursor = connection.cursor()
    drop_table= '''drop table if exists Youtube_Videos'''
    cursor.execute(drop_table)
    connection.commit()
    query="""create table Youtube_Videos(video_id varchar(255),
                                 video_name varchar(255),
                                 vchannel_name varchar(255),
                                 video_des TEXT,
                                 video_viewcount bigint, 
                                 video_comment int,
                                 video_likes bigint,
                                 video_publisheddate DATETIME,
                                 video_duration time)"""
    cursor.execute(query)
    Videos_lists=[]
    for i in db.collection1.find({},{"_id":0,"videos_information":1}):
        for j in range(len(i["videos_information"])):
         Videos_lists.append(i["videos_information"][j])
    df2=pd.DataFrame(Videos_lists)
    for x,y in df2.iterrows():
        query='''insert into Youtube_Videos(video_id,video_name,vchannel_name,video_des,
                                    video_viewcount,video_comment,video_likes,video_publisheddate,video_duration)
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        d = datetime.fromisoformat(y['video_publisheddate']).astimezone(timezone.utc)
        y['video_publisheddate']=d.strftime('%Y-%m-%d %H:%M:%S')
        d1 = dateutil.parser.parse(y['video_duration'][2:])
        y['video_duration']=d1.strftime('%H:%M:%S')
        
        values=(y['video_id'],
                y['video_name'],
                y['vchannel_name'],
                y['video_des'],
                y['video_viewcount'],
                y['video_comment'],
                y['video_likes'],
                y['video_publisheddate'],
                y['video_duration'])
        
        cursor.execute(query,values)
        connection.commit()
def Comments_Table():
    connection=mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345678',
    database = 'Youtube_Project')
    cursor = connection.cursor()
    drop_table= '''drop table if exists Comments'''
    cursor.execute(drop_table)
    connection.commit()
    query="""create table Comments(
                                  comment_id varchar(255) primary key,
                                  comment_video_id varchar(255),
                                  comment_text  Text,
                                  comment_author varchar(255),
                                  comment_published_date DATETIME
                                 )"""
    cursor.execute(query)
    Comments_lists=[]
    for i in db.collection1.find({},{"_id":0,"comment_information":1}):
        for j in range(len(i["comment_information"])):
            Comments_lists.append(i["comment_information"][j])
    df3=pd.DataFrame(Comments_lists)
    for x,y in df3.iterrows():
        query='''insert into Comments(comment_id,comment_video_id,
                                    comment_text,comment_author,comment_published_date)
                                    values(%s,%s,%s,%s,%s)'''
        
        d = datetime.fromisoformat(y['comment_published_date']).astimezone(timezone.utc)
        y['comment_published_date']=d.strftime('%Y-%m-%d %H:%M:%S')
        
        values=(y['comment_id'],
                y['comment_video_id'],
                y['comment_text'],
                y['comment_author'],
                y['comment_published_date'],
              )
        
        cursor.execute(query,values)
        connection.commit()
def tables():
    Channels_Table()
    Videos_Table()
    Comments_Table()
    return "Tables are created successfully"
def Show_Channel_lists():
    Channel_lists=[]
    for i in db.collection1.find({},{"_id":0,"Channel_information":1}):
        Channel_lists.append(i["Channel_information"])
    df=st.dataframe(Channel_lists)
    return df
def Show_Videos_lists():
    Videos_lists=[]
    for i in db.collection1.find({},{"_id":0,"videos_information":1}):
        for j in range(len(i["videos_information"])):
            Videos_lists.append(i["videos_information"][j])
    df2=st.dataframe(Videos_lists)
    return df2
def Show_Comments_lists():
    Comments_lists=[]
    for i in db.collection1.find({},{"_id":0,"comment_information":1}):
        for j in range(len(i["comment_information"])):
            Comments_lists.append(i["comment_information"][j])
    df3=st.dataframe(Comments_lists)
    return df3
#Streamlit
with st.sidebar:
    st.title(":blue[DATA COLLECTION]")
    st.caption("Query")
    st.caption("Data Insertion")
    st.caption("Data Analysis")
st.subheader(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

channel_id=st.text_input("Enter the channel ID")
if st.button("Store Channel Details"):
    channel_ids=[]
    #db = connection['Youtube_Project']
    #collection1=db['Youtube_Data']
    for i in db.collection1.find({},{"_id":0,"Channel_information":1}):
        channel_ids.append(i["Channel_information"]["channel_id"])
    if channel_id in channel_ids:
        st.success("Channel Details are already available for the given ID")
    else:
        insert=Youtube_Channel(channel_id)
        st.success(insert)
if st.button("Create Tables in SQL"):
    Table=tables()
    st.success(Table)
st.text(" ")
st.text('Select Channels/Videos/Comments button to view respective table:')
st.text(" ")
c1, c2, c3 = st.columns(3)
with c1:
   if st.button("Channels"):
    Show_Channel_lists()

with c2:
   if st.button("Videos"):
    Show_Videos_lists()
       
with c3:
   if st.button("Comments"):
    Show_Comments_lists()
       
#Query
connection=mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345678',
    database = 'Youtube_Project')
cursor = connection.cursor()
Questions=st.selectbox("Select the question",
                       ("1.What are the names of all the videos and their corresponding channels?",
                        "2.Which channels have the most number of videos, and how many videos do they have?",
                        "3.What are the top 10 most viewed videos and their respective channels?",
                        "4.How many comments were made on each video, and what are their corresponding video names?",
                        "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                        "6.What is the total number of likes for each video, and what are their corresponding video names?",
                        "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                        "8.What are the names of all the channels that have published videos in the year 2022?",
                        "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                        "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
#SQL Query

if (Questions=="1.What are the names of all the videos and their corresponding channels?"):
    query1= "select video_name,vchannel_name from Youtube_Videos"
    cursor.execute(query1)
    d=cursor.fetchall()
    df1=pd.DataFrame(d,columns=["Video_Title","Channel_name"])
    st.write(df1)
elif(Questions=="2.Which channels have the most number of videos, and how many videos do they have?"):
    query2="select channel_name,channel_videos from Channel order by channel_videos desc"
    cursor.execute(query2)
    d=cursor.fetchall()
    df2=pd.DataFrame(d,columns=["Channel_name","Total_Videos"])
    st.write(df2)
elif(Questions=="3.What are the top 10 most viewed videos and their respective channels?"):
    query3="select video_viewcount,video_name,vchannel_name from Youtube_Videos order by video_viewcount desc limit 10"
    cursor.execute(query3)
    d=cursor.fetchall()
    df3=pd.DataFrame(d,columns=["Video_viewcount","Video_name","Channel_name"])
    st.write(df3)
elif(Questions=="4.How many comments were made on each video, and what are their corresponding video names?"):
    query4="select video_comment,video_name from Youtube_Videos where video_comment is not null"
    cursor.execute(query4)
    d=cursor.fetchall()
    df4=pd.DataFrame(d,columns=["Video_comment","Video_name"])
    st.write(df4)
elif(Questions=="5.Which videos have the highest number of likes, and what are their corresponding channel names?"):
    query5="select video_name,video_likes,vchannel_name from Youtube_Videos where video_likes is not null order by video_likes desc"
    cursor.execute(query5)
    d=cursor.fetchall()
    df5=pd.DataFrame(d,columns=["Video_name","Video_likes","Video_channelname"])
    st.write(df5)
elif(Questions=="6.What is the total number of likes for each video, and what are their corresponding video names?"):
    query6="select video_name,video_likes from Youtube_Videos"
    cursor.execute(query6)
    d=cursor.fetchall()
    df6=pd.DataFrame(d,columns=["Video_name","Video_likes"])
    st.write(df6)
elif(Questions=="7.What is the total number of views for each channel, and what are their corresponding channel names?"):
    query7="select channel_name,channel_views from Channel"
    cursor.execute(query7)
    d=cursor.fetchall()
    df7=pd.DataFrame(d,columns=["Channel_name","Channel_views"])
    st.write(df7)
elif(Questions=="8.What are the names of all the channels that have published videos in the year 2022?"):
    query8='''select video_name,video_publisheddate,vchannel_name from Youtube_Videos where extract(year from video_publisheddate)=2022'''
    cursor.execute(query8)
    d=cursor.fetchall()
    df8=pd.DataFrame(d,columns=["Video_name","Published_date","Channel_name"])
    st.write(df8)
elif(Questions=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?"):
    query9='''select vchannel_name,AVG(video_duration) from Youtube_Videos group by vchannel_name'''
    cursor.execute(query9)
    d=cursor.fetchall()
    df9=pd.DataFrame(d,columns=["Vchannel_name","Average_duration"])
    T9=[]
    for x,y in df9.iterrows():
        channel_name=y["Vchannel_name"]
        average_duration=y["Average_duration"]
        avg_dur=str(average_duration)
        T9.append(dict(Channel_name=channel_name,avgduration=avg_dur))
    df=pd.DataFrame(T9)
    st.write(df)
elif(Questions=="10.Which videos have the highest number of comments, and what are their corresponding channel names?"):
    query10="select video_name,video_comment,vchannel_name from Youtube_videos where video_comment is not null order by video_comment desc"
    cursor.execute(query10)
    d=cursor.fetchall()
    df10=pd.DataFrame(d,columns=["Video_name","Video_comment","Vchannel_name"])
    st.write(df10)





        


