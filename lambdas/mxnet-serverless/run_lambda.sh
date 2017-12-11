#!/bin/bash

#time aws lambda invoke --invocation-type RequestResponse --cli-read-timeout 240 \
#--function-name mxnet-test-dev-hellov2 \ --region us-west-2 --log-type Tail --payload \
#'{"inputBucket": "vass-video-samples2","inputKey":"jpeg-test/0901+100.jpg","batchSize": 10}' outputfile

#time aws lambda invoke --invocation-type Event --function-name mxnet-test-dev-hellov2 \
#--region us-west-2 --log-type Tail --payload \
#'{"inputBucket": "vass-video-samples2","inputKey":"jpeg-test/1001+100.jpg","batchSize": 10}' outputfile

time aws lambda invoke --invocation-type Event --function-name mxnet-test-dev-mxnets3 \
--region us-west-2 --payload file://./s3event.txt \
outputfile
