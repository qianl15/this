######################################################
# -*- coding: utf-8 -*-
# File Name: util.py
# Author: Qian Li
# Created Date: 2017-10-20
# Description: This util is based on 
# scanner/examples/util.py
######################################################

import os.path
from subprocess import check_call as run
import urllib
import hashlib
try:
  import youtube_dl
except ImportError:
  print('You need to install youtube-dl to run this. Try running:\npip install youtube-dl')
  exit()

def get_md5(filePath):
  hash_md5 = hashlib.md5()
  with open(filePath, 'rb') as f:
    for chunk in iter(lambda: f.read(4096), b''):
      hash_md5.update(chunk)
  return hash_md5.hexdigest()

# default: download the lowest quality of the first video
def download_video1(num = 1, fm_num = 1):
   # format 134 = 360p, 135 = 480p, 136 = 720p, 137 = 1080p, 138 = 4k
  if fm_num == 1:
    format = '134'
  elif fm_num == 2:
    format = '135'
  elif fm_num == 3:
    format = '136'
  elif fm_num == 4:
    format = '137'
  elif fm_num == 5:
    format = '138'
  else:
    print('Please select the format between 1~5')
    exit()

  VID_PATH = "/tmp/example%d_%s.mp4"%(num, format)

  if not os.path.isfile(VID_PATH):
    ydl_opts = {
      'format': format,
      'outtmpl': u'/tmp/example%d_%s'%(num, format)+'.%(ext)s'
    }
    # if num == 2:
    #   ydl_opts['outtmpl'] = u'/tmp/example2.%(ext)s'
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
      if num == 1:
        ydl.download(["https://www.youtube.com/watch?v=79DijItQXMM"])
      elif num == 2:
        ydl.download(["https://www.youtube.com/watch?v=cPAbx5kgCJo"])
      elif num == 3:
        ydl.download(["https://www.youtube.com/watch?v=xDMP3i36naA"])
      elif num == 4:
        ydl.download(["https://www.youtube.com/watch?v=5DP5I8Gd6wY"])
      else:
        print('invalid option for video: choose 1 / 2 / 3')
        exit()
  return VID_PATH

def download_video2(videoUrl = ''):
  VID_PATH = "/tmp/example.mkv"

  if not os.path.isfile(VID_PATH):
    print 'Downloading file: %s' % videoUrl

    urllib.urlretrieve(videoUrl, VID_PATH)
    print 'Download complete'
    if not os.path.exists(VID_PATH):
      raise Exception('%s does not exist' % VID_PATH)
    else:
      inputSize = os.path.getsize(VID_PATH)
      os.chmod(VID_PATH, 0o0755)
      print ' [%dKB] %s' % (inputSize >> 10, VID_PATH)
      print ' [md5] %s' % get_md5(VID_PATH)
  return VID_PATH

def have_gpu():
  try:
    run(['nvidia-smi'])
    return True
  except OSError:
    return False
