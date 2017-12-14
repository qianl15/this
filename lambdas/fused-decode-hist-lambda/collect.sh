#!/bin/sh

#zip decode-scanner.zip \
#    lambda.py DecoderAutomataCmd-static

rm *.zip

zip -9r fused_decode_hist.zip lambda.py 
