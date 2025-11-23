import os
import logging
from datetime import datetime, timedelta
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from anthropic import Anthropic

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
GOOGLE_SHEETS_CREDENTIALS = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
SHEET_NAME = 'NBA Podcast Stream'

# YouTube channels to monitor
CHANNELS = [
    'UCxgXtBJHYRhix8TSEa8j6iQ',  # Road Trippin' Show
    'UC9Pd-5TAwCg5Y_P8BDGPjQg',  # All The Smoke
    'UC3qn0bAOiQRPqsVSv8vXhyw',  # The Draymond Green Show
    'UCVKW7R2vPwRiKFOL-QJzTrw',  # Above the Rim with DH12
    'UCxCsJhHPZLpqv1dHCxKx2ew',  # Point Forward with Andre Iguodala
    'UCYlUCQRNhvCLLs8p02U-Adg',  # Run It Back on FanDuel TV
    'UCIM6xWLAm-Vwz10oBd-XyVw',  # Gil's Arena
    'UC3sqm5gLcfPV-ECEy2RJkxA',  # The OGs Show
    'UCiPGk-oBTeWSXTaXzNT4VmQ',  # 7PM in Brooklyn with Carmelo Anthony & The Kid Mero
    'UCWhqYkZKOqGQhtx7DsNKGTA',  # Big Fish Energy with David Fizdale and Kelenna Azubuike
    'UCh5_5D78_L-DKvYNEVTuIxA',  # Bully Ball with Rachel Nichols and DeMarcus Cousins
    'UCxLr54__LqSF22pRvUYUqQQ',  # Unapologetically Angel with Angel Reese
    'UCq1rWFHH2yDdaRPBBfT6OBA',  # Come And Talk 2 Me with Mark Jackson and Stephen Jackson
    'UC_qM9Qp02aQjPljYPz6HzzA',  # The Dunk Bait
    'UCPlhJvX4PyhZEzz_LlbYFMQ',  # Club Shay Shay
    'UCqBuVlZvAf0w51g2wrz33gQ',  # Podcast P with Paul George
    'UC_i_KrVnRStqzAOKFRJuIkg',  # The Old Man and The Three with JJ Redick and Tommy Alter
    'UCDLwhSKpbTFqGxHQJG5f0jw',  # Up & Adams with Kay Adams
    'UCVrtm2o0gyFXE_8L-l_GDwg',  # 7 Rings with Grayson Boucher
    'UC8R4YxIxzASUAiOUUgaVBJw',  # The Draymond Green Show with Baron Davis
    'UCDGPm9cVUYbvp4Dm8ygIjVQ',  # Knuckleheads with Quentin Richardson & Darius Miles
    'UCpyqVYKiNKUV5xM-zKS1psg',  # The WHite Noise Podcast with Derrick White
    'UCxLDCBjCc2d4QNiPJ4_Hl0A',  # VC 3&Out
    'UCXjXmON_qxCJc7vy7j0fJiw',  # The Why's with CJ McCollum
    'UCjVIXYqNsD7UoYQTR3A2OyQ',  # The K$K Show
    'UCLaGjWYEsPeqqM9h-T0lWaA',  # Ryen Russillo Podcast
    'UCEPLNzwtTMBxYONjZZGYJWg',  # Serge Ibaka | How Hungry Are You?
    'UC_6zk_GdMuJJmG_k1Q1gq5w',  # Forgotten Seasons
    'UCKZJ7vWu4S8NPvmn-GdCmaw',  # TIDAL League
    'UCjpJq6d7A2O1z6zL6Z6SuRA',  # Keepin' It Real with Brandon Jennings
    'UCBRj6s3mh-Y_pJLSDQYSTqA',  # Out Of Bounds with Gilbert Arenas
    'UCnQ__7tXOD63tFn_31yFk_w',  # BUCKETS with Nic Batum
    'UCsCWmjFOdIFdGfQ_g8E1GDw',  # Shots with Shane & Company
    'UC5sC3eH5tFMFqtOpYfQMk3Q',  # The Backyard Podcast
    'UCjGlq72Db_gTVqaT8iuA0Pw',  # Mind the Game Podcast with LeBron James and JJ Redick
    'UCPEyJKdWPLxJUzVDTNJlsAw',  # STRAIGHT GAME PODCAST
    'UCDgfQF14xRh5aaXv2EYaGNQ',  # Court Vision with BJ Armstrong
    'UCOkb4YYIW9pUCFf9KdFMdfA',  # The Pivot Podcast
    'UCJfWJ-QoGVBfvPxYlqwXm6g',  # The Draymond Green Show with Evan Turner  
    'UCLKBHqVwP2OC1DdmJYXhL0w',  # Bald Man & Ballin with Gary Trent Jr.
    'UCVPZIbqIRkzpBxDXCJqaOAQ',  # These Guys Must Be On Something
    'UC3oHr1bQKmMJRdJXWQsGsYA',  # Last Night's Game
    'UCaQE0oTwEVBlhLXPQAFxRnA'   # Zeke & The Freak
]

BATCH_SIZE = 15
MAX_RESULTS_PER_CHANNEL = 3

class TranscriptFetcher:
    @staticmethod
    def get_transcript(video_id):
        """Fetch transcript for a video"""
        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            
            # Try to get English transcript
            try:
                transcript = transcript_list.find_transcript(['en'])
                segments = transcript.fetch()
                return ' '.join([segment['text'] for segment in segments])
            except:
                # Try auto-generated
                try:
                    transcript = transcript_list.find_generated_transcript(['en'])
                    segments = transcript.fetch()
                    return ' '.join([segment['text'] for segment in segments])
                except:
                    return None
                    
        except Exception as e:
            logger.debug(f"No transcript available for {video_id}: {str(e)}")
            return None

class SummaryGenerator:
    def __init__(self, api_key):
        self.client = Anthropic(api_key=api_key)
    
    def generate_summary(self, title, transcript):
        """Generate AI summary from transcript"""
        if not transcript or len(transcript.strip()) < 50:
            return ""
        
        # Truncate transcript to first 8000 characters
        truncated = transcript[:8000]
        
        prompt = f"""Based on this NBA podcast episode transcript, write a concise 3-4 sentence summary:

Title: {title}

Transcript excerpt:
{truncated}

Summary (3-4 sentences only):"""

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text.strip()
        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            return ""

class YouTubeFetcher:
    def __init__(self, api_key):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
    
    def format_duration(self, duration_iso):
        """Convert ISO 8601 duration to readable format (e.g., PT1H23M45S -> 1:23:45)"""
        import re
        
        if not duration_iso:
            return ""
        
        # Parse ISO 8601 duration (PT1H23M45S)
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_iso)
        if not match:
            return ""
        
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0
        
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes}:{seconds:02d}"
    
    def get_channel_videos(self, channel_id):
        """Fetch recent videos from a channel"""
        try:
            # Get uploads playlist
            channel_response = self.youtube.channels().list(
                part='contentDetails,snippet,statistics',
                id=channel_id
            ).execute()
            
            if not channel_response.get('items'):
                return []
            
            channel_info = channel_response['items'][0]
            uploads_playlist = channel_info['contentDetails']['relatedPlaylists']['uploads']
            channel_name = channel_info['snippet']['title']
            subscriber_count = channel_info['statistics'].get('subscriberCount', 'Hidden')
            
            # Get videos from uploads playlist
            playlist_response = self.youtube.playlistItems().list(
                part='contentDetails',
                playlistId=uploads_playlist,
                maxResults=MAX_RESULTS_PER_CHANNEL
            ).execute()
            
            video_ids = [item['contentDetails']['videoId'] for item in playlist_response.get('items', [])]
            
            if not video_ids:
                return []
            
            # Get detailed video info INCLUDING contentDetails for duration
            videos_response = self.youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(video_ids)
            ).execute()
            
            videos = []
            for item in videos_response.get('items', []):
                # Get duration from contentDetails
                duration_iso = item['contentDetails'].get('duration', '')
                duration_formatted = self.format_duration(duration_iso)
                
                video = {
                    'id': item['id'],
                    'title': item['snippet']['title'],
                    'channel_name': channel_name,
                    'channel_id': channel_id,
                    'published_at': item['snippet']['publishedAt'],
                    'thumbnail': item['snippet']['thumbnails']['high']['url'],
                    'description': item['snippet'].get('description', ''),
                    'view_count': item['statistics'].get('viewCount', '0'),
                    'subscriber_count': subscriber_count,
                    'duration': duration_formatted,  # NEW: Add duration
                    'like_count': item['statistics'].get('likeCount', '0'),
                    'comment_count': item['statistics'].get('commentCount', '0')
                }
                videos.append(video)
            
            return videos
            
        except Exception as e:
            logger.error(f"Error fetching videos for channel {channel_id}: {str(e)}")
            return []

class GoogleSheetsManager:
    def __init__(self, credentials_json):
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds_dict = json.loads(credentials_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        self.client = gspread.authorize(creds)
        self.sheet = None
    
    def get_or_create_sheet(self):
        """Get existing sheet or create new one"""
        try:
            self.sheet = self.client.open(SHEET_NAME).sheet1
            logger.info(f"Opened existing sheet: {SHEET_NAME}")
        except gspread.SpreadsheetNotFound:
            spreadsheet = self.client.create(SHEET_NAME)
            self.sheet = spreadsheet.sheet1
            logger.info(f"Created new sheet: {SHEET_NAME}")
            
            # Set headers with Duration column
            headers = [
                'Video ID', 'Title', 'Channel Name', 'Channel ID', 
                'Published Date', 'Thumbnail URL', 'Description', 
                'View Count', 'Subscriber Count', 'Duration',  # Duration is column J
                'Like Count', 'Comment Count', 
                'Transcript Available', 'AI Summary', 'Last Updated'
            ]
            self.sheet.append_row(headers)
            
            # Share with anyone with link
            spreadsheet.share('', perm_type='anyone', role='reader')
    
    def get_existing_video_ids(self):
        """Get set of video IDs already in sheet"""
        try:
            video_ids = self.sheet.col_values(1)[1:]  # Skip header
            return set(video_ids)
        except:
            return set()
    
    def add_video(self, video, has_transcript=False, summary=""):
        """Add new video to sheet"""
        row = [
            video['id'],
            video['title'],
            video['channel_name'],
            video['channel_id'],
            video['published_at'],
            video['thumbnail'],
            video['description'][:500],  # Limit description length
            video['view_count'],
            video['subscriber_count'],
            video['duration'],  # NEW: Duration column
            video['like_count'],
            video['comment_count'],
            'Yes' if has_transcript else 'No',
            summary,
            datetime.utcnow().isoformat()
        ]
        self.sheet.append_row(row)
    
    def update_video(self, row_index, video, has_transcript=False, summary=""):
        """Update existing video row"""
        updates = {
            'View Count': video['view_count'],
            'Subscriber Count': video['subscriber_count'],
            'Duration': video['duration'],  # Update duration too
            'Like Count': video['like_count'],
            'Comment Count': video['comment_count'],
            'Transcript Available': 'Yes' if has_transcript else 'No',
            'AI Summary': summary,
            'Last Updated': datetime.utcnow().isoformat()
        }
        
        # Update specific cells
        headers = self.sheet.row_values(1)
        for field, value in updates.items():
            try:
                col_index = headers.index(field) + 1
                self.sheet.update_cell(row_index, col_index, value)
            except ValueError:
                continue

class PodcastStreamManager:
    def __init__(self):
        self.youtube = YouTubeFetcher(YOUTUBE_API_KEY)
        self.sheets = GoogleSheetsManager(GOOGLE_SHEETS_CREDENTIALS)
        self.transcript_fetcher = TranscriptFetcher()
        self.summary_generator = SummaryGenerator(ANTHROPIC_API_KEY)
    
    def run(self):
        """Main execution flow"""
        logger.info("Starting NBA Podcast Stream update...")
        
        # Initialize sheet
        self.sheets.get_or_create_sheet()
        existing_ids = self.sheets.get_existing_video_ids()
        
        # Collect videos from all channels
        all_videos = []
        for channel_id in CHANNELS:
            videos = self.youtube.get_channel_videos(channel_id)
            all_videos.extend(videos)
            logger.info(f"Fetched {len(videos)} videos from channel {channel_id}")
        
        # Sort by published date (newest first)
        all_videos.sort(key=lambda x: x['published_at'], reverse=True)
        
        # Process videos in batches
        processed = 0
        for video in all_videos:
            if processed >= BATCH_SIZE:
                logger.info(f"Reached batch limit of {BATCH_SIZE}")
                break
            
            # Get transcript
            transcript = self.transcript_fetcher.get_transcript(video['id'])
            has_transcript = transcript is not None and len(transcript) > 50
            
            # Generate summary if transcript available
            summary = ""
            if has_transcript:
                summary = self.summary_generator.generate_summary(
                    video['title'],
                    transcript
                )
            
            # Add or update video
            if video['id'] not in existing_ids:
                self.sheets.add_video(video, has_transcript, summary)
                logger.info(f"Added new video: {video['title'][:50]}... (Duration: {video['duration']})")
                processed += 1
            else:
                # Update existing (stats may have changed)
                all_data = self.sheets.sheet.get_all_values()
                for idx, row in enumerate(all_data[1:], start=2):
                    if row[0] == video['id']:
                        self.sheets.update_video(idx, video, has_transcript, summary)
                        logger.info(f"Updated video: {video['title'][:50]}...")
                        processed += 1
                        break
        
        logger.info(f"Completed! Processed {processed} videos")

def handler(event=None, context=None):
    """Handler for serverless deployment"""
    manager = PodcastStreamManager()
    manager.run()
    return {'statusCode': 200, 'body': 'Success'}

if __name__ == '__main__':
    handler()
