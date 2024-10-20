#!/usr/bin/python
import httplib2
import os
import random
import sys
import time

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow
from src.youtube_upload import *

class youtubeUploader():
  def __init__(self, client_secrets_file: str):
    self.client_secrets = client_secrets_file
    self.authenticated_service = self.get_authenticated_service()

  def get_authenticated_service(self):
    flow = flow_from_clientsecrets(self.client_secrets,
      scope=YOUTUBE_UPLOAD_SCOPE,
      message=MISSING_CLIENT_SECRETS_MESSAGE)

    storage = Storage("%s-oauth2.json" % sys.argv[0])
    credentials = storage.get()

    if credentials is None or credentials.invalid:
      credentials = run_flow(flow, storage)

    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
      http=credentials.authorize(httplib2.Http()))

  def initialize_upload(self, youtube, options: YoutubeOptions):
    tags = None
    if options['keywords']:
      tags = options['keywords'].split(",")

    body=dict(
      snippet=dict(
        title=options['title'],
        description=options['description'],
        tags=tags,
        categoryId=options['category']
      ),
      status=dict(
        privacyStatus=options['privacyStatus']
      )
    )

    # Call the API's videos.insert method to create and upload the video.
    insert_request = youtube.videos().insert(
      part=",".join(body.keys()),
      body=body,
      # The chunksize parameter specifies the size of each chunk of data, in
      # bytes, that will be uploaded at a time. Set a higher value for
      # reliable connections as fewer chunks lead to faster uploads. Set a lower
      # value for better recovery on less reliable connections.
      #
      # Setting "chunksize" equal to -1 in the code below means that the entire
      # file will be uploaded in a single HTTP request. (If the upload fails,
      # it will still be retried where it left off.) This is usually a best
      # practice, but if you're using Python older than 2.6 or if you're
      # running on App Engine, you should set the chunksize to something like
      # 1024 * 1024 (1 megabyte).
      media_body=MediaFileUpload(options['file'], chunksize=-1, resumable=True)
    )

    self.resumable_upload(insert_request)

  # This method implements an exponential backoff strategy to resume a
  # failed upload.
  def resumable_upload(self, insert_request):
    response = None
    error = None
    retry = 0
    while response is None:
      try:
        print("Uploading file...")
        status, response = insert_request.next_chunk()
        if response is not None:
          if 'id' in response:
            print("Video id '%s' was successfully uploaded." % response['id'])
          else:
            exit("The upload failed with an unexpected response: %s" % response)
      except HttpError as e:
        if e.resp.status in RETRIABLE_STATUS_CODES:
          error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status,
                                                              e.content)
        else:
          raise
      except RETRIABLE_EXCEPTIONS as e:
        error = "A retriable error occurred: %s" % e

      if error is not None:
        print(error)
        retry += 1
        if retry > MAX_RETRIES:
          exit("No longer attempting to retry.")

        max_sleep = 2 ** retry
        sleep_seconds = random.random() * max_sleep
        print("Sleeping %f seconds and then retrying..." % sleep_seconds)
        time.sleep(sleep_seconds)

if __name__ == '__main__':
  argparser.add_argument("--file", required=True, help="Video file to upload")
  argparser.add_argument("--title", help="Video title", default="Test Title")
  argparser.add_argument("--description", help="Video description",
    default="Test Description")
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
  options = {
    key: args.__dict__[key] for key in YoutubeOptions['keys']()
  }

  uploader = youtubeUploader(client_secrets_file=CLIENT_SECRETS_FILE)
  service = uploader.get_authenticated_service()
  try:
    uploader.initialize_upload(youtube=service, options=options)
  except HttpError as e:
    print("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))