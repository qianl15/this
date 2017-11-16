#!/bin/bash

time aws lambda invoke --invocation-type RequestResponse --function-name mxnet-test-dev-hellov2 \
--region us-west-2 --log-type Tail --payload \
'{"inputBucket": "vass-video-samples2","inputKey":"batch-test/1901+100.jpg","batchSize": 1}' outputfile
