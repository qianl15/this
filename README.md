# THIS: Thousand Island Scanner

[Scanner](https://github.com/scanner-research/scanner) proposes a high
performance framework to perform visual data analysis on large datasets. We
would like to extend this work to use serverless platforms such as AWS lambda to
enable more elastic scaling of visual data processing tasks. Specfically, can we build a system that automatically scales Scanner computations across thousands or tens of thousands of threads to meet very tight job completion deadlines (on the order of 3-5 minutes)? We are also interested in the cost ($) analysis of running large video analysis jobs on-demand in a serverless setting.

## Getting Started
First, clone the repo:
```
git clone https://github.com/qinglan233/vass.git && cd vass 

git submodule update --init
```
Then follow Scanner instruction to install Scanner on your machine

## Running the tests
All tests are in the `tests` folder
```
cd tests/origin/ 

./run_hist.sh
```
