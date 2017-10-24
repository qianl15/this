######################################################
# -*- coding: utf-8 -*-
# File Name: lambda_client.py
# Author: James Hong
# Created Date: 2017-10-24
# Description: lambda functiion
######################################################
import socket
import sys
import time

download_size = 1024 * 1024 * 100 # 100 MB
num_trials = 25
server_url = "0.0.0.0" # change to your own server's url!

def receive_bytes(n):
    start_time = time.time()
    server_addr = (server_url, 8000)
    sock = socket.create_connection(server_addr)
    
    num_left = n
    while num_left > 0:
        data = sock.recv(num_left if num_left < 1024 else 1024)
        num_left -= len(data)
    sock.close()
    end_time = time.time()
    
    elapsed_time = (end_time - start_time) * 1000
    print "Elapsed: %dms" % elapsed_time
    return elapsed_time

def mean(arr):
    agg = 0.0
    for x in arr:
        agg += x
    return agg / len(arr)
    
def var(arr):
    arr_mean = mean(arr)
    agg = 0.0
    for x in arr:
        agg += (arr_mean - x) ** 2
    return agg / len(arr)

def handler(event, context):
    times = []
    for _ in xrange(num_trials):
        elapsed_time = receive_bytes(download_size)
        times.append(elapsed_time)
    avg_time = mean(times)
    var_time = var(times)
    return {
        "message": 
            "Took on avg %dms (var=%fms, n=%d) to download %d bytes" % (
                avg_time, var_time, num_trials, download_size)
    }
