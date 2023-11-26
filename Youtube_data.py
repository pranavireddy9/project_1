from googleapiclient.discovery import build
import pymongo
import mysql.connector
import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import numpy as np
from isodate import parse_duration
from datetime import timedelta
from datetime import datetime

# API key connection
Api_Id = "AIzaSyBMUD52Jov6a1LBkSCvZvyay4Ih0-7GPPk"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey=Api_Id)

#channel_id

#UCmmgDms8duF96DmAdCav5rw
#UC1MxKUMQna22KsD5ugyIYlg
#UCJAgw1niUkaShdmA5aAZdQw
#UCSc9yZ5B6-C5wqztexck5kw
#UClPmNvQqo53VfBl0FKh6d-Astr
#UCNdjZlsRNik6Yo05DpEUkyg
#UCf2O7QPV_YBJyH8SybsUZzg
#UCe_2t1K89jcoojFCdTjLYog
#UCltEVecvxDR_Sn8iOq1JNcQ

#To read configuration details from confi.txt file
with open('confi.txt', 'r') as file:
    data_dict = {}
    for line in file:
        parts = line.strip().split(":")
        if len(parts)==2:
            key, value = parts
            data_dict[key.strip()] = value.strip()


# get channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,Statistics",
        id=channel_id)
    response1 = request.execute()

    data = { 'Channel_Name' : response1["items"][0]["snippet"]["title"],
            'Channel_Id':response1["items"][0]["id"],
            'Subscription_Count':response1["items"][0]["statistics"]["subscriberCount"],
            'Views_Count':response1["items"][0]["statistics"]["viewCount"],
            'Total_Videos':response1["items"][0]["statistics"]["videoCount"],
            'Channel_Description':response1["items"][0]["snippet"]["description"],
            'Playlist_Id':response1["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    }
    return data



# get playlist information
def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = {'PlaylistId': item['id'],
                    'Title': item['snippet']['title'],
                    'ChannelId': item['snippet']['channelId'],
                    'ChannelName': item['snippet']['channelTitle'],
                    'PublishedAt': item['snippet']['publishedAt'],
                    'VideoCount': item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page = False
    return All_data


# get video ids
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# get video information
def get_video_info(video_ids):
    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id)
        response = request.execute()

        for item in response["items"]:
            duration_str = item['contentDetails']['duration']
            duration = parse_duration(duration_str)

            # Convert duration to seconds
            total_seconds = int(duration.total_seconds())

            # Format the duration as HH:MM:SS
            Duration = str(timedelta(seconds=total_seconds))
            data = dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet']['description'],
                        Published_Date=item['snippet']['publishedAt'],
                        Duration=Duration,
                        Views=item['statistics']['viewCount'],
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


# get comment information
def get_comment_info(video_ids):
    Comment_Information = []
    try:
        for video_id in video_ids:

            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response5 = request.execute()

            for item in response5["items"]:
                comment_information = dict(
                    Comment_Id=item["snippet"]["topLevelComment"]["id"],
                    Video_Id=item["snippet"]["videoId"],
                    Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                    Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                Comment_Information.append(comment_information)
    except:
        pass

    return Comment_Information


# MongoDB Connection
client = pymongo.MongoClient("mongodb+srv://Pranave:QUDvL5RiJJT6QEm@cluster0.gktwil5.mongodb.net/?retryWrites=true&w=majority")
db = client["YOUTUBE"]


# upload to MongoDB

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one(
        {"channel_information": ch_details, "playlist_information": pl_details, "video_information": vi_details,
         "comment_information": com_details})

    return "upload completed successfully"


# Table creation for channels,playlists, videos, comments and migrating data to MYSQL

def channels_table():
    config = {'host': data_dict['host'],
        'user': data_dict['user'],
        'password': data_dict['password'],
        'database': 'youtube_data',
        'port':data_dict['port']
    }
    conn = mysql.connector.connect(**config,autocommit=True)
    cursor = conn.cursor()

    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    conn.commit()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                        Channel_Id varchar(80) primary key, 
                        Subscription_Count bigint, 
                        Views bigint,
                        Total_Videos int,
                        Channel_Description text,
                        Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        conn.commit()
    except:
        st.write("Channels Table already created")

    ch_list = []
    db = client["YOUTUBE"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscription_Count,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''

        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscription_Count'],
            row['Views_Count'],
            row['Total_Videos'],
            row['Channel_Description'],
            row['Playlist_Id'])
        try:
            cursor.execute(insert_query, values)
            conn.commit()
        except:
            st.write("Channels values are already inserted")

def playlists_table():
    config = {
        'host': data_dict['host'],
        'user': data_dict['user'],
        'password': data_dict['password'],
        'database': 'youtube_data',
        'port':data_dict['port']
    }
    conn = mysql.connector.connect(**config,autocommit=True)
    cursor = conn.cursor()

    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    conn.commit()

    try:
        create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                        Title varchar(80), 
                        ChannelId varchar(100), 
                        ChannelName varchar(100),
                        PublishedAt timestamp,
                        VideoCount int
                        )'''
        cursor.execute(create_query)
        conn.commit()
    except:
        st.write("Playlists Table already created")

    db = client["YOUTUBE"]
    coll1 = db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df = pd.DataFrame(pl_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT into playlists(PlaylistId,
                                                    Title,
                                                    ChannelId,
                                                    ChannelName,
                                                    PublishedAt,
                                                    VideoCount)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''
        playlist_published = row['PublishedAt']
        playlist_published_dt = datetime.strptime(playlist_published, '%Y-%m-%dT%H:%M:%SZ')
        playlist_published_At = playlist_published_dt.strftime('%Y-%m-%d %H:%M:%S')
        values = (
            row['PlaylistId'],
            row['Title'],
            row['ChannelId'],
            row['ChannelName'],
            playlist_published_At,
            row['VideoCount'])

        try:
            cursor.execute(insert_query, values)
            conn.commit()
        except:
            st.write("Playlists values are already inserted")


def videos_table():
    config = {'host': data_dict['host'],
        'user': data_dict['user'],
        'password': data_dict['password'],
        'database': 'youtube_data',
        'port':data_dict['port']
    }
    conn = mysql.connector.connect(**config,autocommit=True)
    cursor = conn.cursor()

    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    conn.commit()

    try:
        create_query = '''create table if not exists videos(
                        Channel_Name varchar(150),
                        Channel_Id varchar(100),
                        Video_Id varchar(50) primary key, 
                        Title varchar(150), 
                        Tags text,
                        Thumbnail varchar(225),
                        Description text, 
                        Published_Date timestamp,
                        Duration varchar(10), 
                        Views bigint, 
                        Likes bigint,
                        Comments int,
                        Favorite_Count int, 
                        Definition varchar(10), 
                        Caption_Status varchar(50) 
                        )'''

        cursor.execute(create_query)
        conn.commit()
    except:
        st.write("Videos Table already created")

    vi_list = []
    db = client["YOUTUBE"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        if row['Tags'] is not None:
            tags_list = row['Tags']
            tags_as_string = ', '.join(map(str, tags_list))
        else:
        # Decide how to handle None (replace with an empty string in this example)
            tags_as_string = ''
        insert_query = '''
                    INSERT INTO videos (Channel_Name,
                        Channel_Id,
                        Video_Id, 
                        Title, 
                        Tags,
                        Thumbnail,
                        Description, 
                        Published_Date,
                        Duration, 
                        Views, 
                        Likes,
                        Comments,
                        Favorite_Count, 
                        Definition, 
                        Caption_Status 
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                '''
        videos_published= row['Published_Date']
        videos_published_dt = datetime.strptime(videos_published, '%Y-%m-%dT%H:%M:%SZ')
        videos_published_At = videos_published_dt.strftime('%Y-%m-%d %H:%M:%S')


        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Title'],
            tags_as_string,
            row['Thumbnail'],
            row['Description'],
            videos_published_At,
            row['Duration'],
            row['Views'],
            row['Likes'],
            row['Comments'],
            row['Favorite_Count'],
            row['Definition'],
            row['Caption_Status'])

        try:
            cursor.execute(insert_query, values)
            conn.commit()
        except:
            st.write("videos values already inserted in the table")


def comments_table():
    config = {'host': data_dict['host'],
        'user': data_dict['user'],
        'password': data_dict['password'],
        'database': 'youtube_data',
        'port':data_dict['port']
              }
    conn = mysql.connector.connect(**config,autocommit=True)
    cursor = conn.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    conn.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published timestamp)'''
        cursor.execute(create_query)
        conn.commit()

    except:
        st.write("Comments Table already created")

    com_list = []
    db = client["YOUTUBE"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
        insert_query = '''
                INSERT INTO comments (Comment_Id,
                                      Video_Id ,
                                      Comment_Text,
                                      Comment_Author,
                                      Comment_Published)
                VALUES (%s, %s, %s, %s, %s)

            '''
        comment_published = row['Comment_Published']
        comment_published_dt = datetime.strptime(comment_published, '%Y-%m-%dT%H:%M:%SZ')
        comment_published_At = comment_published_dt.strftime('%Y-%m-%d %H:%M:%S')
        values = (
            row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            comment_published_At
        )
        try:
            cursor.execute(insert_query, values)
            conn.commit()
        except:
            st.write("This comments are already exist in comments table")


def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully and migrated data to SQL"


def show_channels_table():
    ch_list = []
    db = client["YOUTUBE"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table


def show_playlists_table():
    db = client["YOUTUBE"]
    coll2 = db["channel_details"]
    pl_list = []
    for pl_data in coll2.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    playlists_table = st.dataframe(pl_list)
    return playlists_table


def show_videos_table():
    vi_list = []
    db = client["YOUTUBE"]
    coll3 = db["channel_details"]
    for vi_data in coll3.find({},{"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table


def show_comments_table():
    com_list = []
    db = client["YOUTUBE"]
    coll4 = db["channel_details"]
    for com_data in coll4.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table


#streamlit

with st.sidebar.header("skills"):
    selected=option_menu(
        menu_title=None,
        options=["Introduction","Storing data in mongodb","Migration of data to sql","data analysis"]
    )
if selected=="Introduction":
    st.title(":red[Youtube Data Harvesting]")
    st.header("Introduction:")
    st.text("In this project we fetch data of few channels to mongodb and transform data to mysql and do analysis on the data")
    st.header("Skills")
    multiline_text = """
        Python\n
        Mongodb\n
        Mysql\n
        streamlit
        """

    st.write(multiline_text)

if selected=="Storing data in mongodb":
    channel_id = st.text_input("Enter the Channel id")
    channels = channel_id.split(',')
    channels = [ch.strip() for ch in channels if ch]
    st.write(channels)
    if st.button("Collect and Store data"):
        for channel in channels:
            ch_ids = []
            db = client["YOUTUBE"]
            coll1 = db["channel_details"]
            for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
                ch_ids.append(ch_data["channel_information"]["Channel_Id"])
            if channel in ch_ids:
                st.success("Channel details of the given channel id: " + channel + " already exists")
            else:
                output = channel_details(channel)
                st.success(output)
    show_table = st.radio("# SELECT THE TABLE FOR VIEW",
                      (":green[channels]", ":orange[playlists]", ":red[videos]", ":blue[comments]"))

    if show_table == ":green[channels]":
        show_channels_table()
    elif show_table == ":orange[playlists]":
        show_playlists_table()
    elif show_table == ":red[videos]":
        show_videos_table()
    elif show_table == ":blue[comments]":
        show_comments_table()

if selected=="Migration of data to sql":
    st.header(":blue[MYSQL]")

    if st.button("Migrate to SQL"):
        display = tables()
        st.success(display)

    

if selected =="data analysis":
    st.header("data analysis")
    config = {'host': data_dict['host'],
        'user': data_dict['user'],
        'password': data_dict['password'],
        'database': 'youtube_data',
        'port':data_dict['port']
    }
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    question = st.selectbox(
        'Please Select Your Question',
        ('1. All the video Names and the Channel Names',
        '2. Channels with most number of videos',
        '3.  Top 10 most viewed videos',
        '4. Comments in each video',
        '5. Videos with highest likes',
        '6. likes of all videos',
        '7. views of each channel',
        '8. videos published in the year 2022',
        '9. average duration of all videos in each channel',
        '10. videos with highest number of comments'))                                             

    if question == '1. All the video Names and the Channel Names':
        query1 = "select Title as Video_Names, Channel_Name as ChannelName from videos;"
        cursor.execute(query1)
        t1 = cursor.fetchall()
        conn.commit()
        st.write(pd.DataFrame(t1, columns=["Video Title", "Channel Name"]))

    elif question == '2. Channels with most number of videos':
        query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
        cursor.execute(query2)
        rows2 = cursor.fetchall()
        conn.commit()
        st.write(pd.DataFrame(rows2, columns=["Channel Name", "No Of Videos"]))

    elif question == '3. Top 10 most viewed videos':
        query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                            where Views is not null order by Views desc limit 10;'''
        cursor.execute(query3)
        t3 = cursor.fetchall()
        conn.commit()
        st.write(pd.DataFrame(t3, columns=["views", "channel Name", "video title"]))

    elif question == '4. Comments in each video':
        query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
        cursor.execute(query4)
        t4 = cursor.fetchall()
        conn.commit()
        st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

    elif question == '5. Videos with highest likes':
        query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                        where Likes is not null order by Likes desc;'''
        cursor.execute(query5)
        t5 = cursor.fetchall()
        conn.commit()
        st.write(pd.DataFrame(t5, columns=["video Title", "channel Name", "like count"]))

    elif question == '6. likes of all videos':
        query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
        cursor.execute(query6)
        t6 = cursor.fetchall()
        conn.commit()
        st.write(pd.DataFrame(t6, columns=["like count", "video title"]))

    elif question == '7. views of each channel':
        query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
        cursor.execute(query7)
        t7 = cursor.fetchall()
        conn.commit()
        st.write(pd.DataFrame(t7, columns=["channel name", "total views"]))

    elif question == '8. videos published in the year 2022':
        query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                    where extract(year from Published_Date) = 2022;'''
        cursor.execute(query8)
        t8 = cursor.fetchall()
        conn.commit()
        st.write(pd.DataFrame(t8, columns=["Name", "Video Publised On", "ChannelName"]))

    elif question == '9. average duration of all videos in each channel':
        query9 = "SELECT Channel_Name as ChannelName,TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(duration))), '%H:%i:%s') as Avg_duration FROM videos group by Channel_Name;"
        cursor.execute(query9)
        t9 = cursor.fetchall()
        conn.commit()
        t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
        T9 = []
        for index, row in t9.iterrows():
            channel_title = row['ChannelTitle']
            average_duration = row['Average Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Title": channel_title, "Average Duration": average_duration_str})
        st.write(pd.DataFrame(T9))

    elif question == '10. videos with highest number of comments':
        query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                        where Comments is not null order by Comments desc;'''
        cursor.execute(query10)
        t10 = cursor.fetchall()
        conn.commit()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
    
