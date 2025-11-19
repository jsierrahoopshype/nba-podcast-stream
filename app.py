"""
NBA Podcast Stream - Enhanced Version with Statistics # Updated with enhanced stats - Nov 19
Fetches video views, channel subscribers, and more metadata
"""

import os
import json
import time
from datetime import datetime, timedelta
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
from googleapiclient.discovery import build
from google.oauth2 import service_account
import anthropic
import requests

# ============================================
# CONFIGURATION
# ============================================

class Config:
    # Get from environment variables
    GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
    YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
    ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
    SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
    
    # Settings
    CHANNELS_SHEET = 'Sheet1'
    VIDEOS_SHEET = 'Videos'
    CHANNEL_CACHE_SHEET = 'ChannelCache'
    MAX_VIDEOS_PER_CHANNEL = 3
    VIDEO_CACHE_DAYS = 7
    BATCH_SIZE = 15
    CHANNEL_GROUPS = 2

# ============================================
# GOOGLE SHEETS CLIENT
# ============================================

class SheetsClient:
    def __init__(self):
        # Parse credentials from environment
        creds_dict = json.loads(Config.GOOGLE_CREDENTIALS_JSON)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        self.service = build('sheets', 'v4', credentials=creds)
        self.spreadsheet_id = Config.SPREADSHEET_ID
    
    def read_range(self, range_name):
        """Read values from a range"""
        result = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=range_name
        ).execute()
        return result.get('values', [])
    
    def append_rows(self, range_name, values):
        """Append rows to a sheet"""
        body = {'values': values}
        self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
    
    def create_sheet_if_not_exists(self, sheet_name, headers):
        """Create a sheet with headers if it doesn't exist"""
        try:
            # Check if sheet exists
            sheet_metadata = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            sheets = sheet_metadata.get('sheets', [])
            exists = any(s['properties']['title'] == sheet_name for s in sheets)
            
            if not exists:
                # Create sheet
                request = {
                    'addSheet': {
                        'properties': {'title': sheet_name}
                    }
                }
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={'requests': [request]}
                ).execute()
                
                # Add headers
                self.append_rows(f'{sheet_name}!A1', [headers])
                print(f"Created sheet: {sheet_name}")
        except Exception as e:
            print(f"Error creating sheet {sheet_name}: {e}")

# ============================================
# YOUTUBE CLIENT
# ============================================

class YouTubeClient:
    def __init__(self):
        self.api_key = Config.YOUTUBE_API_KEY
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
    
    def search_channel(self, handle):
        """Search for a channel by handle"""
        try:
            request = self.youtube.search().list(
                part='snippet',
                type='channel',
                q=handle,
                maxResults=1
            )
            response = request.execute()
            
            if 'items' in response and len(response['items']) > 0:
                return response['items'][0]['snippet']['channelId']
        except Exception as e:
            print(f"Error searching channel: {e}")
        return None
    
    def get_channel_info(self, channel_id):
        """Get channel statistics"""
        try:
            request = self.youtube.channels().list(
                part='statistics,snippet',
                id=channel_id
            )
            response = request.execute()
            
            if 'items' in response and len(response['items']) > 0:
                item = response['items'][0]
                return {
                    'subscriber_count': item['statistics'].get('subscriberCount', 'Hidden'),
                    'total_views': item['statistics'].get('viewCount', '0'),
                    'video_count': item['statistics'].get('videoCount', '0')
                }
        except Exception as e:
            print(f"Error getting channel info: {e}")
        return {'subscriber_count': 'N/A', 'total_views': '0', 'video_count': '0'}
    
    def get_channel_videos(self, channel_id, max_results=3):
        """Get latest videos from a channel with statistics"""
        try:
            # Get video IDs
            search_request = self.youtube.search().list(
                part='snippet',
                channelId=channel_id,
                order='date',
                type='video',
                maxResults=max_results
            )
            search_response = search_request.execute()
            
            if 'items' not in search_response:
                return []
            
            # Get video IDs
            video_ids = [item['id']['videoId'] for item in search_response['items']]
            
            # Get video statistics
            stats_request = self.youtube.videos().list(
                part='statistics,snippet',
                id=','.join(video_ids)
            )
            stats_response = stats_request.execute()
            
            videos = []
            for item in stats_response['items']:
                videos.append({
                    'id': item['id'],
                    'title': item['snippet']['title'],
                    'channel_name': item['snippet']['channelTitle'],
                    'channel_id': item['snippet']['channelId'],
                    'published_at': item['snippet']['publishedAt'],
                    'thumbnail': item['snippet']['thumbnails']['high']['url'],
                    'description': item['snippet']['description'],
                    'view_count': item['statistics'].get('viewCount', '0'),
                    'like_count': item['statistics'].get('likeCount', '0'),
                    'comment_count': item['statistics'].get('commentCount', '0')
                })
            
            return videos
            
        except Exception as e:
            print(f"Error getting channel videos: {e}")
            return []

# ============================================
# TRANSCRIPT FETCHER
# ============================================

class TranscriptFetcher:
    @staticmethod
    def get_transcript(video_id):
        """Fetch transcript for a YouTube video - try multiple methods"""
        
        # Method 1: Try to get manually created English transcript
        try:
            transcript_list = YouTubeTranscriptApi.get_transcript(
                video_id,
                languages=['en', 'en-US', 'en-GB']
            )
            
            full_text = ' '.join([entry['text'] for entry in transcript_list])
            full_text = full_text.replace('\n', ' ').replace('  ', ' ').strip()
            
            words = full_text.split()
            limited_text = ' '.join(words[:4000])
            
            print(f"✓ Extracted manual transcript: {len(words)} words")
            return limited_text
            
        except (TranscriptsDisabled, NoTranscriptFound):
            pass
        except Exception as e:
            print(f"Manual transcript attempt failed: {str(e)[:50]}")
        
        # Method 2: Try to get auto-generated transcript
        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            
            try:
                transcript = transcript_list.find_generated_transcript(['en', 'en-US', 'en-GB'])
                entries = transcript.fetch()
                
                full_text = ' '.join([entry['text'] for entry in entries])
                full_text = full_text.replace('\n', ' ').replace('  ', ' ').strip()
                
                words = full_text.split()
                limited_text = ' '.join(words[:4000])
                
                print(f"✓ Extracted auto-generated transcript: {len(words)} words")
                return limited_text
                
            except:
                pass
            
        except Exception as e:
            print(f"Auto-generated transcript attempt failed: {str(e)[:50]}")
        
        # Method 3: Try any available transcript
        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            
            for transcript in transcript_list:
                try:
                    entries = transcript.fetch()
                    full_text = ' '.join([entry['text'] for entry in entries])
                    full_text = full_text.replace('\n', ' ').replace('  ', ' ').strip()
                    
                    words = full_text.split()
                    limited_text = ' '.join(words[:4000])
                    
                    print(f"✓ Extracted transcript ({transcript.language_code}): {len(words)} words")
                    return limited_text
                except:
                    continue
                    
        except Exception as e:
            print(f"All transcript methods failed: {str(e)[:50]}")
        
        print(f"✗ No transcript available")
        return None

# ============================================
# AI SUMMARY GENERATOR
# ============================================

class SummaryGenerator:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=Config.ANTHROPIC_API_KEY)
    
    def generate_summary(self, title, transcript):
        """Generate conversational summary using Claude - ONLY if transcript exists"""
        
        if not transcript or len(transcript) < 100:
            return ""
        
        prompt = f"""You're writing a summary for HoopsHype readers - basketball fans who want real insights, not fluff. Based on the transcript below, write a 3-4 sentence summary that captures what was actually discussed.

Write in a conversational but informative tone - think sports blog, not academic paper. Use natural language:
- "Gil breaks down..." not "Arenas provides an analysis of..."
- "They get into..." not "The discussion encompasses..."
- "He talks about..." not "He elucidates upon..."

Focus on:
- The actual basketball topics, debates, or stories discussed
- Any interesting takes, stats, or insights mentioned
- What makes this episode worth watching
- Keep it readable and engaging

Episode: {title}

Transcript excerpt:
{transcript[:3500]}

Write a natural, conversational summary that sounds like a person wrote it, not a robot."""
        
        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=300,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            return message.content[0].text
            
        except Exception as e:
            print(f"AI summary error: {e}")
            return ""

# ============================================
# MAIN APPLICATION
# ============================================

class PodcastStreamApp:
    def __init__(self):
        self.sheets = SheetsClient()
        self.youtube = YouTubeClient()
        self.transcript_fetcher = TranscriptFetcher()
        self.summary_generator = SummaryGenerator()
        
        # Initialize sheets with enhanced headers
        self.sheets.create_sheet_if_not_exists(
            Config.VIDEOS_SHEET,
            ['Video ID', 'Title', 'Channel Name', 'Channel ID', 'Published Date', 
             'Thumbnail URL', 'Description', 'View Count', 'Subscriber Count',
             'Transcript Available', 'AI Summary', 'Last Updated']
        )
        self.sheets.create_sheet_if_not_exists(
            Config.CHANNEL_CACHE_SHEET,
            ['Channel Handle', 'Channel ID', 'Subscriber Count']
        )
    
    def get_channel_urls(self):
        """Get channel URLs from spreadsheet"""
        try:
            values = self.sheets.read_range(f'{Config.CHANNELS_SHEET}!A2:A')
            return [row[0] for row in values if row]
        except:
            return []
    
    def get_cached_channels(self):
        """Get cached channel IDs"""
        try:
            values = self.sheets.read_range(f'{Config.CHANNEL_CACHE_SHEET}!A2:C')
            cache = {}
            for row in values:
                if len(row) >= 2:
                    cache[row[0]] = {'id': row[1], 'subscribers': row[2] if len(row) > 2 else 'N/A'}
            return cache
        except:
            return {}
    
    def cache_channel(self, handle, channel_id, subscriber_count):
        """Cache a channel ID with subscriber count"""
        self.sheets.append_rows(
            f'{Config.CHANNEL_CACHE_SHEET}!A:C',
            [[handle, channel_id, subscriber_count]]
        )
    
    def get_existing_video_ids(self):
        """Get IDs of videos already in the sheet"""
        try:
            values = self.sheets.read_range(f'{Config.VIDEOS_SHEET}!A2:A')
            return set(row[0] for row in values if row)
        except:
            return set()
    
    def extract_channel_handle(self, url):
        """Extract channel handle from URL"""
        import re
        
        match = re.search(r'@([^/]+)', url)
        if match:
            return match.group(1)
        
        match = re.search(r'channel/([^/]+)', url)
        if match:
            return match.group(1)
        
        return None
    
    def resolve_channel_ids(self, channel_urls):
        """Resolve channel URLs to IDs"""
        cached = self.get_cached_channels()
        channel_data = []
        
        for url in channel_urls:
            handle = self.extract_channel_handle(url)
            if not handle:
                continue
            
            if handle in cached:
                channel_data.append(cached[handle])
                print(f"Using cached ID for {handle}")
            else:
                if handle.startswith('UC'):
                    channel_id = handle
                else:
                    channel_id = self.youtube.search_channel(handle)
                
                if channel_id:
                    # Get channel stats
                    channel_info = self.youtube.get_channel_info(channel_id)
                    subscriber_count = channel_info['subscriber_count']
                    
                    channel_data.append({'id': channel_id, 'subscribers': subscriber_count})
                    self.cache_channel(handle, channel_id, subscriber_count)
                    print(f"Resolved {handle} -> {channel_id} ({subscriber_count} subs)")
                    time.sleep(0.2)
        
        return channel_data
    
    def run(self):
        """Main update function"""
        print(f"\n{'='*60}")
        print(f"NBA Podcast Stream Update - {datetime.now()}")
        print(f"{'='*60}\n")
        
        all_channel_urls = self.get_channel_urls()
        print(f"Total channels: {len(all_channel_urls)}")
        
        current_hour = datetime.now().hour
        group_index = (current_hour // 6) % Config.CHANNEL_GROUPS
        
        channels_per_group = len(all_channel_urls) // Config.CHANNEL_GROUPS
        start_idx = group_index * channels_per_group
        end_idx = start_idx + channels_per_group if group_index < Config.CHANNEL_GROUPS - 1 else len(all_channel_urls)
        channel_urls = all_channel_urls[start_idx:end_idx]
        
        print(f"Processing group {group_index + 1}/{Config.CHANNEL_GROUPS}: channels {start_idx + 1}-{end_idx}")
        
        channel_data = self.resolve_channel_ids(channel_urls)
        print(f"Resolved {len(channel_data)} channel IDs\n")
        
        all_videos = []
        cutoff_date = datetime.now() - timedelta(days=Config.VIDEO_CACHE_DAYS)
        
        for i, data in enumerate(channel_data):
            print(f"Fetching videos from channel {i + 1}/{len(channel_data)}")
            try:
                videos = self.youtube.get_channel_videos(
                    data['id'],
                    Config.MAX_VIDEOS_PER_CHANNEL
                )
                
                for video in videos:
                    pub_date = datetime.fromisoformat(video['published_at'].replace('Z', '+00:00'))
                    if pub_date.replace(tzinfo=None) >= cutoff_date:
                        video['subscriber_count'] = data['subscribers']
                        all_videos.append(video)
                
                time.sleep(0.3)
            except Exception as e:
                print(f"Error fetching channel: {e}")
        
        print(f"\nFetched {len(all_videos)} total videos")
        
        all_videos.sort(key=lambda v: v['published_at'], reverse=True)
        
        existing_ids = self.get_existing_video_ids()
        new_videos = [v for v in all_videos if v['id'] not in existing_ids]
        
        print(f"Found {len(new_videos)} new videos to process\n")
        
        processed = 0
        batch_count = min(Config.BATCH_SIZE, len(new_videos))
        
        for i in range(batch_count):
            video = new_videos[i]
            
            try:
                print(f"Processing {i + 1}/{batch_count}: {video['title'][:60]}...")
                
                transcript = self.transcript_fetcher.get_transcript(video['id'])
                has_transcript = transcript is not None and len(transcript) > 50
                
                if has_transcript:
                    summary = self.summary_generator.generate_summary(
                        video['title'],
                        transcript
                    )
                else:
                    summary = ""
                
                self.sheets.append_rows(
                    f'{Config.VIDEOS_SHEET}!A:L',
                    [[
                        video['id'],
                        video['title'],
                        video['channel_name'],
                        video['channel_id'],
                        video['published_at'],
                        video['thumbnail'],
                        video['description'],
                        video['view_count'],
                        video['subscriber_count'],
                        'Yes' if has_transcript else 'No',
                        summary,
                        datetime.now().isoformat()
                    ]]
                )
                
                processed += 1
                time.sleep(2)
                
            except Exception as e:
                print(f"Error processing video: {e}")
        
        print(f"\n✓ Successfully processed {processed} new videos")
        print(f"{'='*60}\n")
        
        return {
            'success': True,
            'group_processed': group_index + 1,
            'channels_in_group': len(channel_data),
            'videos_found': len(all_videos),
            'new_videos_processed': processed
        }

# ============================================
# ENTRY POINT
# ============================================

if __name__ == '__main__':
    app = PodcastStreamApp()
    result = app.run()
    print(f"Result: {result}")
