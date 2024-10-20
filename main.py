import os
from oauth2client.tools import argparser
from googleapiclient.errors import HttpError

from src.youtube_upload import VALID_PRIVACY_STATUSES, CLIENT_SECRETS_FILE, YoutubeOptions
from src.youtube_upload.upload_video import youtubeUploader

argparser.add_argument("--file", required=True, help="Video file to upload", default='./test_video.mp4')
argparser.add_argument("--title", help="Video title", default="Test Title")
argparser.add_argument("--description", help="Video description", default="Test Description")
argparser.add_argument("--category", default="22",
help="Numeric video category. " +
    "See https://developers.google.com/youtube/v3/docs/videoCategories/list")
argparser.add_argument("--keywords", help="Video keywords, comma separated",
default="")
argparser.add_argument("--privacyStatus", choices=VALID_PRIVACY_STATUSES,
default=VALID_PRIVACY_STATUSES[0], help="Video privacy status.")
args = argparser.parse_args()

if not os.path.exists(args.file):
    exit("Please specify a valid file using the --file= parameter.")

options = YoutubeOptions(
    file=args.file,
    title=args.title,
    description=args.description,
    category=args.category,
    keywords=args.keywords,
    privacyStatus=args.privacyStatus,
)

uploader = youtubeUploader(client_secrets_file=CLIENT_SECRETS_FILE)
service = uploader.get_authenticated_service()
try:
    uploader.initialize_upload(youtube=service, options=options)
except HttpError as e:
    print("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))