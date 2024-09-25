import mysql.connector
import requests
from googleapiclient.discovery import build
from datetime import datetime

# YouTube API 키를 설정하세요
API_KEY = 'AIzaSyDr6e7TpfoTHCqRFOMO4dneIs2_Mx-iUmo'

# MySQL 연결 설정
db_config = {
    'host': 'www.bjworld21.com',
    'user': 'mokkitlist',        # MariaDB 사용자명
    'password': 'jg@gjd92#g',  # MariaDB 비밀번호
    'database': 'mokkitlist',      # 데이터베이스 이름
    'charset': 'utf8mb4',            # utf8mb4로 설정
    'collation': 'utf8mb4_unicode_ci'
}

# YouTube API 설정
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

# MariaDB 연결 및 데이터 가져오기
def get_sources():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT * FROM sources")
    sources = cursor.fetchall()
    
    cursor.close()
    conn.close()
    return sources

def get_channels(source_id):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT * FROM channels WHERE sourceId = %s", (source_id,))
    channels = cursor.fetchall()
    
    cursor.close()
    conn.close()
    return channels

def save_post(post_data):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO posts (channelId, externalId, title, url, thumbnailUrl, publishedAt, paidPromotion, author, contentType, createdAt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            thumbnailUrl = VALUES(thumbnailUrl),
            publishedAt = VALUES(publishedAt),
            paidPromotion = VALUES(paidPromotion),
            author = VALUES(author),
            contentType = VALUES(contentType)
    """
    
    cursor.execute(insert_query, (
        post_data['channelId'], post_data['externalId'], post_data['title'], 
        post_data['url'], post_data['thumbnailUrl'], post_data['publishedAt'], 
        post_data['paidPromotion'], post_data['author'], post_data['contentType'], 
        datetime.now()
    ))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Saved post: {post_data['title']}")

def get_latest_published_date(channel_id):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    # 특정 채널에서 가장 최근에 수집된 게시물의 publishedAt 가져오기
    query = "SELECT MAX(publishedAt) FROM posts WHERE channelId = %s"
    cursor.execute(query, (channel_id,))
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if result and result[0]:
        return result[0]
    return None

# YouTube에서 채널 ID 가져오기
def get_channel_id(handle):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)
    
    try:
        # search API를 사용하여 채널 검색
        response = youtube.search().list(
            part='snippet',
            q=handle,    # 검색할 채널 핸들
            type='channel',
            maxResults=1
        ).execute()
        
        if 'items' in response and len(response['items']) > 0:
            return response['items'][0]['id']['channelId']
        else:
            print("채널을 찾을 수 없습니다.")
            return None
    except Exception as e:
        print(f"API 요청 중 오류 발생: {e}")
        return None
    

# YouTube 영상 정보 가져오기
def fetch_youtube_videos(channel_url, channel_id, is_playlist):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)      
    
    next_page_token = None
    
    # 특정 채널의 가장 최근에 수집된 게시물의 날짜 가져오기
    latest_published_date = get_latest_published_date(channel_id)
    
    while True:
        if is_playlist == 'Y':
            playlist_id = channel_url.split('list=')[-1]

            # 플레이리스트의 영상 가져오기
            response = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            items = response.get('items', [])
        else:
            # 채널의 모든 영상 가져오기
            response = youtube.search().list(
                part='snippet',
                channelId=channel_id,
                maxResults=50,
                order='date',
                publishedAfter=latest_published_date.strftime("%Y-%m-%dT%H:%M:%SZ") if latest_published_date else None,
                pageToken=next_page_token,
                type='video'
            ).execute()
            items = response.get('items', [])
        
        for item in items:
            snippet = item['snippet']
            video_id = item['id']['videoId'] if not is_playlist else snippet['resourceId']['videoId']
            video_title = snippet['title']
            video_url = f'https://www.youtube.com/watch?v={video_id}'
            thumbnail_url = snippet['thumbnails']['high']['url'] if 'high' in snippet['thumbnails'] else None
            published_at = snippet['publishedAt']
            published_datetime = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
            
            # latest_published_date 이후의 데이터만 수집
            if latest_published_date and published_datetime <= latest_published_date:
                break;
            
            author = snippet['channelTitle']
            paid_promotion = False

            # 영상 유형이 쇼츠인지 아닌지 확인
            video_details = youtube.videos().list(part='contentDetails', id=video_id).execute()
            
            # video_details에 데이터가 있는지 확인
            if 'items' in video_details and len(video_details['items']) > 0 and 'duration' in video_details['items'][0]['contentDetails']: 
                duration = video_details['items'][0]['contentDetails']['duration']
                content_type = 'youtubeShort' if 'PT' in duration and 'M' not in duration and 'S' in duration and int(duration.split('S')[0].replace('PT', '')) <= 60 else 'youtubeVideo'
            else:
                print(video_details)
                print(f"Warning: Unable to fetch details for video ID: {video_id}")
                continue  # 해당 영상의 데이터를 가져오지 못했으므로 다음으로 넘어감
            
            post_data = {
                'channelId': channel_id,
                'externalId': video_id,
                'title': video_title,
                'url': video_url,
                'thumbnailUrl': thumbnail_url,
                'publishedAt': published_datetime,
                'paidPromotion': paid_promotion,
                'author': author,
                'contentType': content_type
            }
            
            save_post(post_data)
        
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break


def main():
    sources = get_sources()        

    for source in sources:
        if source['sourceName'] == 'YouTube':
            channels = get_channels(source['sourceId'])
            for channel in channels:
                print(f"Processing YouTube channel: {channel['channelName']}")
                fetch_youtube_videos(channel['channelUrl'], channel['channelId'], channel['isPlaylist'])
        
        elif source['sourceName'] == 'Instagram':
            print("Instagram data collection is not implemented yet.")
        
        elif source['sourceName'] == 'Blog':
            print("Blog data collection is not implemented yet.")

if __name__ == "__main__":
    main()
