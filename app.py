from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import os
import time
import anthropic
import json

app = Flask(__name__)

# ============ CONFIGURATION ============
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# ALL 43 NBA PODCAST CHANNELS
CHANNELS = [
    'UCrUJ2hRTGLEaozZZ8cp5u0A',  # TheOGsShow
    'UCoA3lm9UTWDWK8Z1kcoWQcA',  # club520podcast
    'UCf5fcEALUCA53oUW3mc8tiQ',  # victoroladipo
    'UCU50K9xm-5_qbzwjMRr9A8w',  # ToTheBaha
    'UC6L_LBqoKZXFa4WxHox5iCw',  # MindTheGamePodcast
    'UCxclESttS8hoRMd6cIYGgeA',  # TheWAEShow
    'UCllPn98H4a079UMrahK7hhw',  # KyrieIrvingLIVE
    'UCuJ6CSQLBNb0nUvOn-s4aSA',  # TheGilbertArenasShow
    'UCIhTfcMzbR5wyNeh57ju0ug',  # DamianLillard
    'UCLqzKYd1tST9lRPbEUoH5Hg',  # knuckleheadspodcastTPT
    'UCIOXmaExi4DjLHGyGvnu3bw',  # TheBigPod
    'UCSes4X8uDrpc4X46hJKjejg',  # RoadTrippin
    'UCIuKoa1AIiLTiXo0v69gTRg',  # RunYourRaceTL
    'UC_a6c3KLo9reMOqn2pbvMqg',  # The_Backyard_Podcast
    'UCFEiKlkFpJBISr1paTLW8Vg',  # AnthonyEdwards
    'UC3ZStupBLNVY4wVVOn7-e3w',  # CuriousMike
    'UCODlrQzQGttxpYglzmoOaEA',  # Dwighthoward.AboveTheRim
    'UCelmZalfinXZQP7dXw0ZauQ',  # JaylenBrown
    'UCfHG9GrqjYaEdPkrRi9LiBA',  # demarderozan6347
    'UCwr3hqUDuO4D4szLkjeVlrA',  # TraeYoung
    'UCbPY1Efha9VPRBYW2x1M16A',  # YOUNGMANANDTHREE
    'UCa9W_cPwwbDlwBwHOd1YWoQ',  # KGCertified
    'UC4uXDQzZG_WNeIyJQEsZcNA',  # Straight2cam
    'UC4p0nlqdUlocfv--48-15Lg',  # Catch12Media
    'UCvy0Lw9TcvSAQnTzj62aOUA',  # roguebogues
    'UCqbroTPGO_SaLJB_TI4zztg',  # OnTheHausPodcast
    'UCuhRRUv5bBdVRU6uExsd24w',  # StraightGame.podcast
    'UC2ozVs4pg2K3uFLw6-0ayCQ',  # AllTheSmokeProductions
    'UC7HDMexhX9XlWqa8Y1n1SJQ',  # whitenoisepodofficial
    'UCB_4C2Gl7Zfg4uBXk_AcWwA',  # TheBoardroom
    'UC_MXNS3qkraCAEebdScjAPA',  # podcastpshow
    'UC_RyUOQqh3W77giXrbGKVGg',  # kylekuzmaofficial
    'UCFpuYxEexZvdaFQQf3Vloqg',  # AndreDrummond
    'UCrlL3lECKmzkcYK8S6cW2Eg',  # DwyaneWade
    'UCUqIXubCV7gcDJ-QZrdYRbw',  # ThanalysisShow
    'UCqwKvfByZqmdC2jaFcUPcvQ',  # OutTheMudTL
    'UCVKTjJgaohHuJTtNjXCKazw',  # DannyGreenxInsidetheGreenRoom
    'UCwZo_mDI4fOIp-g18kinGBw',  # RunItBackFDTV
    'UC5VYF0LHox2_7Ql2pKihqCQ',  # jaredmccain024
    'UC1N15bwJCPFHjEsun1epfmw',  # Roommates_Show
    'UCalFuU3MOWE39CS6SbfOUgA',  # jalenjdubwilliams
    'UCZv6u7QVz-c8UoiJOMb2oyg',  # nigel_hayes
    'UCd6K_nXCeWBk8YDwja0PPZg',  # TylerHerro
]

# ============ GOOGLE SHEETS SETUP ============
def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
    creds_dict = json.loads(os.environ.get('GOOGLE_SHEETS_CREDENTIALS'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

# ============ YOUTUBE DATA FETCHING ============
def get_channel_videos(youtube, channel_id, hours_back=6):
    """Fetch recent videos from a channel"""
    try:
        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        channel_response = youtube.channels().list(
            part='contentDetails',
            id=channel_id
        ).execute()
        
        if not channel_response['items']:
            return []
        
        uploads_playlist = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        videos = []
        next_page_token = None
        
        while True:
            playlist_response = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=uploads_playlist,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            
            for item in playlist_response['items']:
                videos.append(item['contentDetails']['videoId'])
            
            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token or len(videos) >= 10:
                break
        
        return videos[:10]
    
    except Exception as e:
        print(f"Error fetching videos for channel {channel_id}: {e}")
        return []

def get_video_details(youtube, video_ids):
    """Fetch full metadata for videos"""
    if not video_ids:
        return []
    
    try:
        video_response = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=','.join(video_ids)
        ).execute()
        
        videos_data = []
        
        for item in video_response['items']:
            snippet = item['snippet']
            statistics = item.get('statistics', {})
            content_details = item['contentDetails']
            
            duration = parse_duration(content_details.get('duration', ''))
            
            video_data = {
                'video_id': item['id'],
                'title': snippet['title'],
                'channel_name': snippet['channelTitle'],
                'channel_id': snippet['channelId'],
                'published_date': snippet['publishedAt'],
                'thumbnail': snippet['thumbnails']['high']['url'],
                'description': snippet.get('description', '')[:500],
                'view_count': statistics.get('viewCount', '0'),
                'like_count': statistics.get('likeCount', '0'),
                'comment_count': statistics.get('commentCount', '0'),
                'duration': duration
            }
            
            videos_data.append(video_data)
        
        return videos_data
    
    except Exception as e:
        print(f"Error fetching video details: {e}")
        return []

def parse_duration(iso_duration):
    """Convert ISO 8601 duration to readable format"""
    import re
    
    if not iso_duration:
        return ''
    
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', iso_duration)
    if not match:
        return ''
    
    hours, minutes, seconds = match.groups()
    hours = int(hours) if hours else 0
    minutes = int(minutes) if minutes else 0
    seconds = int(seconds) if seconds else 0
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes}:{seconds:02d}"

# ============ AI SUMMARY GENERATION ============
def generate_ai_summary(video_title, video_description):
    """Generate AI summary using Claude API"""
    if not ANTHROPIC_API_KEY:
        return ''
    
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        
        prompt = f"""You are summarizing an NBA podcast video. Provide a brief 2-3 sentence summary of the key topics discussed.

Title: {video_title}
Description: {video_description}

Provide a concise summary focusing on:
- Main topics/players/teams discussed
- Key insights or hot takes
- Any breaking news or analysis

Summary:"""
        
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=150,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return message.content[0].text.strip()
    
    except Exception as e:
        print(f"Error generating AI summary: {e}")
        return ''

# ============ GOOGLE SHEETS OPERATIONS ============
def get_existing_video_ids(sheet):
    """Get list of video IDs already in the sheet"""
    try:
        records = sheet.get_all_records()
        return set(record['Video ID'] for record in records if record.get('Video ID'))
    except:
        return set()

def write_videos_to_sheet(videos_data):
    """Write new videos to the Videos tab"""
    try:
        client = get_sheets_client()
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        
        sheet = spreadsheet.worksheet('Videos')
        
        existing_ids = get_existing_video_ids(sheet)
        
        new_videos = [v for v in videos_data if v['video_id'] not in existing_ids]
        
        if not new_videos:
            print("No new videos to add")
            return 0
        
        rows = []
        for video in new_videos:
            ai_summary = generate_ai_summary(video['title'], video['description'])
            
            row = [
                video['video_id'],
                video['title'],
                video['channel_name'],
                video['channel_id'],
                video['published_date'],
                video['thumbnail'],
                video['description'],
                video['view_count'],
                'N/A',
                'No',
                ai_summary,
                datetime.utcnow().isoformat()
            ]
            
            rows.append(row)
            time.sleep(1)
        
        sheet.append_rows(rows, value_input_option='USER_ENTERED')
        
        print(f"Added {len(rows)} new videos to sheet")
        return len(rows)
    
    except Exception as e:
        print(f"Error writing to sheet: {e}")
        return 0

# ============ MAIN UPDATE FUNCTION ============
def update_videos():
    """Main function to fetch and update videos"""
    print(f"Starting video update at {datetime.utcnow()}")
    
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    
    all_new_videos = []
    
    for channel_id in CHANNELS:
        print(f"Fetching videos from channel: {channel_id}")
        
        video_ids = get_channel_videos(youtube, channel_id, hours_back=6)
        
        if video_ids:
            video_details = get_video_details(youtube, video_ids)
            all_new_videos.extend(video_details)
        
        time.sleep(0.5)
    
    if all_new_videos:
        videos_added = write_videos_to_sheet(all_new_videos)
        print(f"Update complete: {videos_added} videos added")
    else:
        print("No new videos found")

# ============ FLASK ROUTES ============
@app.route('/')
def home():
    return "NBA Podcast Aggregator Running!"

@app.route('/update')
def trigger_update():
    """Manual trigger endpoint"""
    try:
        update_videos()
        return "Update completed successfully!"
    except Exception as e:
        return f"Update failed: {str(e)}", 500

# ============ SCHEDULED UPDATES ============
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()
scheduler.add_job(func=update_videos, trigger="interval", hours=6)
scheduler.start()

if __name__ == '__main__':
    update_videos()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
