import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import os
import time
import anthropic
import json

# ============ CONFIGURATION ============
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# ALL 57 NBA PODCAST CHANNELS (always included)
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
    'UCIOXmaExi4DjLHGyGvnu3bw',  # TheBigPod / The Big Podcast with Shaq
    'UCSes4X8uDrpc4X46hJKjejg',  # RoadTrippin
    'UCIuKoa1AIiLTiXo0v69gTRg',  # RunYourRaceTL
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
    'UCFpuYxEExZvdaFQQf3Vloqg',  # AndreDrummond
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
    'UCIlXFWq4v1CymTFBBgQ589Q',  # Hoopin' N Hollerin'
    'UCBIPGMEJ72kaL9QHw3xUvuw',  # The Dawg Talk Podcast
    'UC-Ovj3qDVxBAOWX3kwzOVRw',  # No Fouls Given
    'UCAvjYgmwadC682OoC4Cc6TQ',  # The Arena
    'UCBNqqomXKPSWvcQDXOkRvRA',  # The Draymond Green Show
    'UCemwSsWl4rg313rUCBcDiJw',  # Point Game Podcast with Isaiah Thomas
    'UCYYTsni8rBjKzBPGu3-Q8LA',  # Stacey King's Gimme The Hot Sauce Podcast
    'UCBcs-QN0f0DkeCU_pDHTYtA',  # Jovan Buha
    'UC-bJU7MdQ4NW7zUbKChPp6Q',  # NBAFrontOffice
    'UCNGuP7AKOZ9yi7x5SDiCOjg',  # AJ Dybantsa
    'UCU1d7JjFkXlvXeFoHez44AQ',  # The Bench Seat
    'UCCGJZrHsGPYXlZx7WeO1P-w',  # Third Apron
    'UCA95kYjGqDgRziNA7sJZBag',  # The Richard Jefferson Show
    'UCT3wF21Qx0d0HmuiCzji8Lg',  # 7PM in Brooklyn with Carmelo Anthony
    'UCS5fdgvuA-fCPZQsk0unKnw',  # The Zach Lowe Show
]

# CONDITIONAL CHANNELS (only include if keywords match)
CONDITIONAL_CHANNELS = {
    'UC_fs6bgkAF9UgWNmJCvl8Pw': {  # Nate Duncan
        'name': 'Nate Duncan',
        'keywords': ['Hollinger'],
        'match_all': False  # Any keyword matches
    },
    'UCxcTeAKWJca6XyJ37_ZoKIQ': {  # Pat McAfee Show
        'name': 'The Pat McAfee Show',
        'keywords': ['Shams'],
        'match_all': False
    },
    'UCVSSpcmZD2PwPBqb8yKQKBA': {  # NBA on ESPN
        'name': 'NBA on ESPN',
        'keywords': ['Collective'],
        'match_all': False
    },
    'UCT83YP07yVuaH9J19YABhlw': {  # The Ringer NBA
        'name': 'The Ringer NBA',
        'keywords': [],  # Empty = always include
        'match_all': False
    },
    'UCP032AGFh2KzzIUGylB9BjA': {  # Bill Simmons
        'name': 'Bill Simmons',
        'keywords': ['NBA'],
        'match_all': False
    },
    'UCi9ACGG8NptsQhaMyezITEw': {  # Yahoo Sports
        'name': 'Yahoo Sports',
        'keywords': ['Kevin O\'Connor', 'Iko'],
        'match_all': False
    },
    'UCPAt6z5uX_c5Eo_cSNROzYw': {  # Sports Illustrated
        'name': 'Sports Illustrated',
        'keywords': ['Mannix', 'Nichols'],
        'match_all': True  # BOTH keywords must appear
    }
}

def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
    creds_dict = json.loads(os.environ.get('GOOGLE_SHEETS_CREDENTIALS'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def get_channel_videos(youtube, channel_id, hours_back=6):
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
        playlist_response = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=uploads_playlist,
            maxResults=10
        ).execute()
        
        for item in playlist_response['items']:
            videos.append(item['contentDetails']['videoId'])
        
        return videos
    
    except Exception as e:
        print(f"Error fetching videos for channel {channel_id}: {e}")
        return []

def get_video_details(youtube, video_ids):
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

def matches_keywords(video, keywords, match_all=False):
    """
    Check if video title or description contains the required keywords.
    
    Args:
        video: Video data dict
        keywords: List of keywords to search for
        match_all: If True, ALL keywords must be present. If False, ANY keyword matches.
    
    Returns:
        True if keywords match, False otherwise
    """
    if not keywords:  # Empty keywords list = always include
        return True
    
    text = f"{video['title']} {video['description']}".lower()
    
    if match_all:
        # ALL keywords must be present
        return all(keyword.lower() in text for keyword in keywords)
    else:
        # ANY keyword matches
        return any(keyword.lower() in text for keyword in keywords)

def generate_ai_summary(video_title, video_description):
    # AI summaries disabled
    return ''

def get_existing_video_ids(sheet):
    try:
        records = sheet.get_all_records()
        return set(record['Video ID'] for record in records if record.get('Video ID'))
    except:
        return set()

def write_videos_to_sheet(videos_data):
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
            print(f"Processing: {video['title']}")
            ai_summary = generate_ai_summary(video['title'], video['description'])
            
            row = [
                video['video_id'],              # A: Video ID
                video['title'],                 # B: Title
                video['channel_name'],          # C: Channel Name
                video['channel_id'],            # D: Channel ID
                video['published_date'],        # E: Published Date
                video['thumbnail'],             # F: Thumbnail URL
                video['description'],           # G: Description
                video['view_count'],            # H: View Count
                'N/A',                          # I: Subscriber Count
                'No',                           # J: Transcript Available
                ai_summary,                     # K: AI Summary
                datetime.utcnow().isoformat(),  # L: Last Updated
                video['duration'],              # M: Duration
                video['like_count'],            # N: Like Count
                video['comment_count']          # O: Comment Count
            ]
            
            rows.append(row)
            time.sleep(1)
        
        sheet.append_rows(rows, value_input_option='USER_ENTERED')
        
        print(f"✅ Added {len(rows)} new videos to sheet")
        return len(rows)
    
    except Exception as e:
        print(f"❌ Error writing to sheet: {e}")
        return 0

def main():
    print(f"🚀 Starting video update at {datetime.utcnow()}")
    
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    
    all_new_videos = []
    
    # Process regular channels (always include all videos)
    print("\n📺 Processing regular channels...")
    for channel_id in CHANNELS:
        print(f"  Fetching from: {channel_id}")
        
        video_ids = get_channel_videos(youtube, channel_id, hours_back=6)
        
        if video_ids:
            video_details = get_video_details(youtube, video_ids)
            all_new_videos.extend(video_details)
        
        time.sleep(0.5)
    
    # Process conditional channels (filter by keywords)
    print("\n🔍 Processing conditional channels...")
    for channel_id, config in CONDITIONAL_CHANNELS.items():
        print(f"  Fetching from: {config['name']} (keywords: {config['keywords']})")
        
        video_ids = get_channel_videos(youtube, channel_id, hours_back=6)
        
        if video_ids:
            video_details = get_video_details(youtube, video_ids)
            
            # Filter videos by keywords
            for video in video_details:
                if matches_keywords(video, config['keywords'], config['match_all']):
                    print(f"    ✓ MATCH: {video['title']}")
                    all_new_videos.append(video)
                else:
                    print(f"    ✗ SKIP: {video['title']}")
        
        time.sleep(0.5)
    
    print(f"\n📊 Found {len(all_new_videos)} total videos")
    
    if all_new_videos:
        videos_added = write_videos_to_sheet(all_new_videos)
        print(f"✅ Update complete: {videos_added} new videos added")
    else:
        print("ℹ️ No new videos found")

if __name__ == '__main__':
    main()
