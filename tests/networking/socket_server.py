######################################################
# -*- coding: utf-8 -*-
# File Name: socket_server.py
# Author: James Hong
# Created Date: 2017-10-24
# Description: script for the server side
######################################################import socket
import sys

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_addr = ('', 8000)
print "listening on ", server_addr
sock.bind(server_addr)
sock.listen(1)

download_size = 1024 * 1024 * 100

onekb = "A" * 1024

while True:
    print "waiting to connect"
    conn, cli_addr = sock.accept()
    print "connected client", cli_addr
    try:
        for _ in xrange(download_size / 1024):
            conn.sendall(onekb)
    finally:
        conn.close()
