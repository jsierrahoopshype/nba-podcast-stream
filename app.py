import os
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import json
import logging
import traceback

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Setup credentials
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_INFO = json.loads(os.environ['GOOGLE_SHEETS_CREDENTIALS'])
creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)

# Setup Google Sheets
gc = gspread.authorize(creds)
SPREADSHEET_ID = os.environ['SPREADSHEET_ID']
spreadsheet = gc.open('NBA Podcast Stream')
worksheet = spreadsheet.sheet1

# Setup YouTube API
YOUTUBE_API_KEY = os.environ['YOUTUBE_API_KEY']
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

def get_channel_videos(channel_id, hours=48):
    """Fetch recent videos from a YouTube channel."""
    logger.info(f"DEBUG: Fetching channel info for {channel_id}")
    try:
        # Get channel info to find uploads playlist
        channel_response = youtube.channels().list(
            part='contentDetails,snippet',
            id=channel_id
        ).execute()
        
        logger.info(f"DEBUG: Channel response received, items count: {len(channel_response.get('items', []))}")
        
        if not channel_response.get('items'):
            logger.warning(f"DEBUG: No channel found for ID {channel_id}")
            return []
        
        channel_title = channel_response['items'][0]['snippet']['title']
        uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        logger.info(f"DEBUG: Channel '{channel_title}' found, uploads playlist: {uploads_playlist_id}")
        
        # Get videos from uploads playlist
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        playlist_response = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=uploads_playlist_id,
            maxResults=10
        ).execute()
        
        video_ids = [item['contentDetails']['videoId'] for item in playlist_response.get('items', [])]
        logger.info(f"DEBUG: Found video IDs: {video_ids}")
        
        if not video_ids:
            return []
        
        # Get video details
        videos_response = youtube.videos().list(
            part='snippet,contentDetails',
            id=','.join(video_ids)
        ).execute()
        
        recent_videos = []
        for video in videos_response.get('items', []):
            published_at = datetime.strptime(video['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            
            if published_at >= cutoff_time:
                duration = video['contentDetails']['duration']
                recent_videos.append({
                    'title': video['snippet']['title'],
                    'video_id': video['id'],
                    'channel_title': channel_title,
                    'published_at': published_at,
                    'duration': duration
                })
        
        return recent_videos
        
    except Exception as e:
        logger.error(f"DEBUG: ERROR fetching videos for channel {channel_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return []

def update_sheet():
    """Main function to update the sheet with new videos."""
    logger.info("Starting NBA Podcast Stream update...")
    
    try:
        # Get all channel IDs from Column B (skip header)
        channel_ids = worksheet.col_values(2)[1:]  # Column B, skip header row
        
        logger.info(f"Current headers: {worksheet.row_values(1)}")
        
        # Find the "Duration" column
        headers = worksheet.row_values(1)
        try:
            duration_col_index = headers.index('Duration') + 1
            logger.info(f"Duration column found at index {duration_col_index}")
        except ValueError:
            logger.error("Duration column not found in headers")
            return
        
        # Get existing video IDs to avoid duplicates
        all_values = worksheet.get_all_values()
        existing_video_ids = set()
        for row in all_values[1:]:  # Skip header
            if len(row) > duration_col_index:
                video_id = row[duration_col_index]
                if video_id:
                    existing_video_ids.add(video_id)
        
        # Collect all new videos
        all_new_videos = []
        
        for channel_id in channel_ids:
            if not channel_id or not channel_id.startswith('UC'):
                continue
                
            videos = get_channel_videos(channel_id)
            logger.info(f"Fetched {len(videos)} videos from channel {channel_id}")
            
            for video in videos:
                if video['video_id'] not in existing_video_ids:
                    all_new_videos.append(video)
        
        # Sort by publish date (newest first)
        all_new_videos.sort(key=lambda x: x['published_at'], reverse=True)
        
        # Add new videos to sheet
        if all_new_videos:
            # Get current last row
            last_row = len(worksheet.get_all_values())
            
            for video in all_new_videos:
                last_row += 1
                video_url = f"https://www.youtube.com/watch?v={video['video_id']}"
                
                # Add row with: Channel URL (empty), Duration, Video URL, Channel Title, Video Title
                new_row = ['', video['duration'], video_url, video['channel_title'], video['title']]
                worksheet.insert_row(new_row, last_row)
                logger.info(f"Added: {video['title']} from {video['channel_title']}")
        
        logger.info(f"Completed! Processed {len(all_new_videos)} videos")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def handler(event=None, context=None):
    """Handler for serverless deployment"""
    update_sheet()
    return {'statusCode': 200, 'body': 'Success'}

if __name__ == '__main__':
    handler()
