######################################################
# -*- coding: utf-8 -*-
# File Name: mxnet_lambda.py
# Author: Qian Li
# Created Date: 2017-11-04
# Description: This file show how to integrate Lambda
# with Scanner. Adapted from Scanner example6
######################################################

from scannerpy import Database, Job, DeviceType, BulkJob
from scannerpy.stdlib import parsers
import os.path


with Database() as db:

    if not os.path.isfile('lambda_op/build/libmxnetlambda_op.so'):
        print('You need to build the custom op first: \n'
              '$ cd lambda_op; mkdir build && cd build; cmake ..; make')
        exit()

    # To load a custom op into the Scanner runtime, we use db.load_op to open the
    # shared library we compiled. If the op takes arguments, it also optionally
    # takes a path to the generated python file for the arg protobuf.
    db.load_op('lambda_op/build/libmxnetlambda_op.so', 'lambda_op/build/mxnetlambda_pb2.py')

    frame = db.ops.FrameInput()
    # Then we use our op just like in the other examples.
    classes = db.ops.MxnetLambda(
        frame = frame,
        batch = 100,
        server = "aaa", path = "/bbb")
    output_op = db.ops.Output(columns=[classes])
    job = Job(
        op_args={
            frame: db.table('example').column('frame'),
            output_op: 'mxnet_lambda',
        }
    )
    bulk_job = BulkJob(output=output_op, jobs=[job])
    output_tables = db.run(bulk_job, force=True)

    video_classes = output_tables[0].load(['class'], parsers.classes)

    # Loop over the column's rows. Each row is a tuple of the frame number and
    # value for that row.
    num_rows = 0
    for (frame_index, frame_classes) in video_classes:
        assert len(frame_classes) == 1
        assert frame_classes[0].shape[0] == 1
        print(frame_classes[0])
        num_rows += 1
    assert num_rows == db.table('example').num_rows()

    print(db.summarize())
