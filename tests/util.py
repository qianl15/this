######################################################
# -*- coding: utf-8 -*-
# File Name: util.py
# Author: Qian Li
# Created Date: 2017-10-20
# Description: This util is based on 
# scanner/examples/util.py
######################################################

import os.path

try:
  import youtube_dl
except ImportError:
  print('You need to install youtube-dl to run this. Try running:\npip install youtube-dl')
  exit()

# VID_PATH = "/tmp/example2.mp4"

def download_video(num = 1):
  VID_PATH = ""
  if num == 1:
    VID_PATH = "/tmp/example.mp4"
  elif num == 2:
    VID_PATH = "/tmp/example2.mp4"
  else:
    print('invalid option for video: choose 1 or 2')
    exit()

  if not os.path.isfile(VID_PATH):
    ydl_opts = {
      'format': '134',
      'outtmpl': u'/tmp/example.%(ext)s'
    }
    if num == 2:
      ydl_opts['outtmpl'] = u'/tmp/example2.%(ext)s'
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
      if num == 1:
        ydl.download(["https://www.youtube.com/watch?v=79DijItQXMM"])
      elif num == 2:
        ydl.download(["https://www.youtube.com/watch?v=cPAbx5kgCJo"])
  return VID_PATH
