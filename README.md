# Thousand Island Scanner (THIS): Scaling Video Analysis on AWS Lambda

Video analysis is computationally and monetarily expensive. 
We present a scalable video analysis framework based on 
[Scanner](https://github.com/scanner-research/scanner) that uses 
[AWS Lambda](https://aws.amazon.com/lambda/) 
to efficiently meet users’ computational needs while minimizing
unused resources by quickly scaling up and down.

* [Install](https://github.com/qinglan233/this#install)
* [Running THIS](https://github.com/qinglan233/this#running-this)


## Install
First, clone the repo:
```bash
git clone https://github.com/qinglan233/this.git && cd this 

git submodule update --init
```


### Build Scanner
Build and install Scanner in `/opt` directory, please refer to the installation guide
of the Scanner [installation guide](https://github.com/scanner-research/scanner#install). 
We also found the [docker file](https://github.com/scanner-research/scanner-docker) is a good guidance for installation.
Note that you must use our [modified version](https://github.com/qinglan233/scanner/tree/273289965f1e173142def6e95b2c771a4d7b3cf7). Because in our end-to-end code, we will use Scanner to ingest the video and prepare arguments for the decoder Lambdas, then our modified Scanner will *terminate* instead of proceeding to the normal path!

Since we will use AWS Lambda that currently does not support GPU, 
please install the *CPU version* of Scanner. 

### Build Decoder
We share our static build [decoder](https://s3-us-west-2.amazonaws.com/mxnet-params/DecoderAutomataCmd-static) and [fused-histogram decoder](https://s3-us-west-2.amazonaws.com/mxnet-params/FusedDecodeHist-static) on a public S3 bucket, however, be careful to use them or compile your own static executable files! Use the following instructions if you want to build your own decode binary file from scratch.

We implemented two models: the fuse model and the split model. 

If you wanted to run the split model, then you can build the standalone decoder:
```bash
cd src/decode-scanner/ && mkdir build
cd build/
cmake ..
make
```
Then use [Ermine](http://www.magicermine.com/) to create a static build binary file.
```bash
ErmineProTrial.x86_64 DecoderAutomataCmd --output=DecoderAutomataCmd-static
```
This static binary file will be used by decoder Lambdas.

If you want to test the fuse model, similarly, you should build the standalone fused decoder and the kernel (e.g., histogram).
```bash
cd src/fused-decode-hist/ && mkdir build
cd build/
cmake ..
make
```
And again use Ermine to create the static binary file called 
`FusedDecodeHist-static`.

You need to upload the decoder binary to S3 because they are too large to fit into a Lambda function. You'll also have to change the decoder download path in Lambda functions. 

### Deploy Lambda Functions

You need to create an AWS account to use AWS Lambda. The services will be used are: [Lambda](https://aws.amazon.com/lambda/) and [S3](https://aws.amazon.com/s3/). 
We also use [Serverless](https://serverless.com/) to automatically create the deployable artifact so that you don’t need to worry about dependency issues.

You need to create two buckets on S3: one for decoder arguments and intermediate data, another for storing the output results.


## Running THIS
As we described, we implement both the fuse and split models. Here are two examples to use them respectively.

### Fuse Model
We use the fused histogram as an example. As described before, you can upload your own  
`FusedDecodeHist-static` file to S3 and change the dowload path in the Lambda function, or you can use our default shared public static build file (no change needed). Then deploy the Lambda function by:
```bash
cd lambdas/fused-decode-hist-lambda/
./collect.sh
```

Create a new Lambda function called `fused-decode-hist`, upload the generated `.zip` file to your Lambda console and create a new Lambda function.

Or you can use [AWS CLI](https://aws.amazon.com/cli/) to deploy Lambda functions:
```bash
aws lambda create-function --function-name <function name> \
  --zip-file fileb://<local zip file> --runtime python2.7 \
  --region <us-west-2 or others> --role <role arn> \
  --handler <lambda file.function name> --memory-size 3008 --timeout 300
```

An example here:
```bash
aws lambda create-function --function-name fused-decode-hist \
  --zip-file fileb://fused_decode_hist.zip --runtime python2.7 \
  --region us-west-2 --role arn:aws:iam:xxxxxxx<your own num>:role/<rolename> \
  --handler lambda.handler --memory-size 3008 --timeout 300
```


You may need to modify the end-to-end [file](end_to_end/end2end_fused_hist.py) to configure `UPLOAD_BUCKET` and `DOWNLOAD_BUCKET` to your own buckets (the two buckets you created in the previous step).

Then run:
```bash
cd end_to_end/
python2 end2end_fused_hist.py 3 1 ./ 50 1
```

You should be able to see the progress bars in your console!


### Split Model
We will deploy two Lambda functions: decoder Lambda and MXNet Lambda. Then use S3 event to link these two Lambda functions. That is, upon uploading decoded frames from the decoder Lambdas, the S3 event will 

the MXNet Lambdas.
So you need to add the decoder uploading path as a resource of trigger to your MXNet Lambda. For more information, please refer to Lambda [document](http://docs.aws.amazon.com/lambda/latest/dg/with-s3.html).

So first, deploy the decoder Lambda:
```bash
mv src/decode-scanner/build/DecoderAutomataCmd-static lambdas/decode-scanner-lambda
cd lambdas/decode-scanner-lambda
./collect.sh
```
Upload the `.zip` file to S3 and create a new Lambda function `decoder-scanner`, import the zip file from S3 since it is too large to upload locally.

Then, deploy the MXNet Lambda:
```bash
cd lambdas/mxnet-serverless/
serverless deploy
```
And remember to configure the **S3 event trigger source**.

Finally, you can run the end-to-end script. Again, you need to modify this 
end-to-end [file](end_to_end/end2end_mxnet.py) to your own S3 buckets. Run:
```bash
cd end_to_end/
python2 end2end_mxnet.py 3 1 ./ 50 1
```
And wait for the progress bars finished.

## Authors

* [Qian Li](https://github.com/qinglan233)
* [James Hong](https://github.com/jhong93)
* [David Durst](https://github.com/David-Durst)

See also the list of [contributors](https://github.com/qinglan233/this/contributors) who participated in this project.


## License

This project is licensed under the Apache License 2.0 - see the 
[LICENSE](LICENSE) file for details

## Acknowledgments

We would like to thank everyone who has helped and supported this project.
