import os
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import json
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Setup credentials
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_INFO = json.loads(os.environ['GOOGLE_SHEETS_CREDENTIALS'])
creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)

# Setup Google Sheets
gc = gspread.authorize(creds)
spreadsheet = gc.open('NBA Podcast Stream')
worksheet = spreadsheet.sheet1

# Setup YouTube API
YOUTUBE_API_KEY = os.environ['YOUTUBE_API_KEY']
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

logger.info("Starting channel ID update process...")
logger.info("=" * 60)

# Get all handles from Column A (skip header)
all_values = worksheet.get_all_values()
handles = [row[0] for row in all_values[1:] if row[0]]  # Skip row 1 (header)

logger.info(f"Found {len(handles)} channels to update\n")

updated_count = 0
failed_count = 0
failed_handles = []

# Process each handle
for i, handle in enumerate(handles, start=2):  # Start at row 2 (skip header)
    logger.info(f"Processing row {i}: {handle}")
    
    # Clean the handle (remove @ if present)
    clean_handle = handle.strip()
    if clean_handle.startswith('@'):
        clean_handle = clean_handle[1:]
    
    try:
        # Try YouTube search for the channel
        request = youtube.search().list(
            part='snippet',
            q=f"@{clean_handle}",
            type='channel',
            maxResults=1
        )
        response = request.execute()
        
        if response['pageInfo']['totalResults'] > 0:
            channel_id = response['items'][0]['snippet']['channelId']
            channel_title = response['items'][0]['snippet']['channelTitle']
            
            # Update the sheet with the new channel ID
            worksheet.update_cell(i, 2, channel_id)  # Column B (2)
            logger.info(f"  ✅ Updated: {channel_title} → {channel_id}")
            updated_count += 1
        else:
            logger.warning(f"  ❌ NOT FOUND: {handle}")
            failed_count += 1
            failed_handles.append(handle)
        
        # Sleep to avoid rate limits
        time.sleep(0.5)
        
    except Exception as e:
        logger.error(f"  ❌ ERROR: {handle} - {str(e)}")
        failed_count += 1
        failed_handles.append(handle)

logger.info("\n" + "=" * 60)
logger.info("SUMMARY:")
logger.info(f"✅ Successfully updated: {updated_count}")
logger.info(f"❌ Failed: {failed_count}")

if failed_handles:
    logger.info("\nFailed handles:")
    for h in failed_handles:
        logger.info(f"  - {h}")

logger.info("\nDone! Channel IDs have been updated in the spreadsheet.")
