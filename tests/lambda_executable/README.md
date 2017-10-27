# Lambda Executable

We use the `executable.py` from [LambdaExecutable](https://github.com/umeat/LambdaExecutable) to demonstrate how to call arbitrary
binary files from Lambda functions.

Put all related executable binary files in `executables/` directory. Remember
that those executable files must be compiled in the latest 
[Amazon Linux AMI](https://aws.amazon.com/amazon-linux-ami/).

Feel free to use [sam](https://github.com/awslabs/aws-sam-local) to test
Lambda function locally. Example usage:
```
sam local invoke -e event.json HelloWorld
```
Our `run_sam.sh` is an example script to call `sam`
and be careful about `template.yaml` file. 
