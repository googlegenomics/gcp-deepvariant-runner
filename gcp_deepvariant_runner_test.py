# Copyright 2017 Google LLC.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from this
#    software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
"""Tests for gcp_deepvariant_runner.

To run the tests, first activate virtualenv and install required packages:
$ virtualenv venv
$ . venv/bin/activate
$ pip install -r requirements.txt
$ pip install mock

Then run:
$ python gcp_deepvariant_runner_test.py
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import multiprocessing
import os
import tempfile
import unittest

import gcp_deepvariant_runner
import gke_cluster

import mock
from google.cloud import storage


# TODO(b/119814955): expand and check command's subargs.
class _HasAllOf(object):
  """Helper class used in mock.call to check that all arguments are in a set."""

  def __init__(self, *values):
    self._values = set(values)

  def _expand(self, items):
    expanded = []
    explode = False
    for item in items:
      if explode:
        explode = False
        expanded.extend(item.split(','))
      else:
        expanded.append(item)

      if item == '--inputs' or item == '--outputs':
        explode = True

    return expanded

  def __eq__(self, other):
    return self._values.issubset(set(self._expand(other)))

  def __ne__(self, other):
    return not self.__eq__(other)

  def __repr__(self):
    return '<_HasAllOf({})>'.format(', '.join(repr(v) for v in self._values))


class AnyStringWith(str):
  """Helper class used in mocking to check string arguments."""

  def __eq__(self, other):
    return self in other


class DeepvariantRunnerTest(unittest.TestCase):

  def setUp(self):
    super(DeepvariantRunnerTest, self).setUp()
    self._argv = [
        '--project',
        'project',
        '--docker_image',
        'gcr.io/dockerimage',
        '--zones',
        'zone-a',
        'zone-b',
        '--outfile',
        'gs://bucket/output.vcf',
        '--staging',
        'gs://bucket/staging',
        '--model',
        'gs://bucket/model',
        '--bam',
        'gs://bucket/bam',
        '--ref',
        'gs://bucket/ref',
    ]

  @mock.patch('gcp_deepvariant_runner._run_job')
  @mock.patch.object(multiprocessing, 'Pool')
  @mock.patch('gcp_deepvariant_runner._gcs_object_exist')
  @mock.patch('gcp_deepvariant_runner._can_write_to_bucket')
  def testRunPipeline(self, mock_can_write_to_bucket, mock_obj_exist, mock_pool,
                      mock_run_job):
    mock_apply_async = mock_pool.return_value.apply_async
    mock_apply_async.return_value = None
    mock_obj_exist.return_value = True
    mock_can_write_to_bucket.return_value = True
    self._argv.extend(
        ['--make_examples_workers', '1', '--call_variants_workers', '1'])
    gcp_deepvariant_runner.run(self._argv)

    mock_apply_async.assert_has_calls([
        mock.call(mock_run_job, [
            _HasAllOf('make_examples', 'gcr.io/dockerimage',
                      'INPUT_BAM=gs://bucket/bam',
                      'INPUT_BAI=gs://bucket/bam.bai',
                      'INPUT_REF=gs://bucket/ref',
                      'INPUT_REF_FAI=gs://bucket/ref.fai',
                      'EXAMPLES=gs://bucket/staging/examples/0/*',
                      '--output-interval', '60s'),
            'gs://bucket/staging/logs/make_examples/0'
        ]),
        mock.call(mock_run_job, [
            _HasAllOf('call_variants', 'gcr.io/dockerimage',
                      'MODEL=gs://bucket/model',
                      'EXAMPLES=gs://bucket/staging/examples/0/*',
                      'CALLED_VARIANTS=gs://bucket/staging/called_variants/*',
                      '--output-interval', '60s'),
            'gs://bucket/staging/logs/call_variants/0'
        ]),
    ])
    self.assertEqual(mock_apply_async.call_count, 2)

    mock_run_job.assert_called_once_with(
        _HasAllOf('postprocess_variants', 'gcr.io/dockerimage',
                  'CALLED_VARIANTS=gs://bucket/staging/called_variants/*',
                  'OUTFILE=gs://bucket/output.vcf', '--output-interval', '60s'),
        'gs://bucket/staging/logs/postprocess_variants')

  @mock.patch('gcp_deepvariant_runner._run_job')
  @mock.patch.object(multiprocessing, 'Pool')
  @mock.patch('gcp_deepvariant_runner._gcs_object_exist')
  @mock.patch('gcp_deepvariant_runner._can_write_to_bucket')
  def testRunPipeline_WithGVCFOutFile(self, mock_can_write_to_bucket,
                                      mock_obj_exist, mock_pool, mock_run_job):
    mock_apply_async = mock_pool.return_value.apply_async
    mock_apply_async.return_value = None
    mock_obj_exist.return_value = True
    mock_can_write_to_bucket.return_value = True
    self._argv.extend([
        '--make_examples_workers',
        '1',
        '--call_variants_workers',
        '1',
        '--gvcf_outfile',
        'gs://bucket/gvcf_output.vcf',
        '--gvcf_gq_binsize',
        '5',
    ])
    gcp_deepvariant_runner.run(self._argv)

    mock_apply_async.assert_has_calls([
        mock.call(mock_run_job, [
            _HasAllOf('make_examples', 'gcr.io/dockerimage',
                      'INPUT_BAM=gs://bucket/bam',
                      'INPUT_BAI=gs://bucket/bam.bai',
                      'INPUT_REF=gs://bucket/ref',
                      'INPUT_REF_FAI=gs://bucket/ref.fai',
                      'EXAMPLES=gs://bucket/staging/examples/0/*',
                      'GVCF=gs://bucket/staging/gvcf/*'),
            'gs://bucket/staging/logs/make_examples/0'
        ]),
        mock.call(mock_run_job, [
            _HasAllOf('call_variants', 'gcr.io/dockerimage',
                      'MODEL=gs://bucket/model',
                      'EXAMPLES=gs://bucket/staging/examples/0/*',
                      'CALLED_VARIANTS=gs://bucket/staging/called_variants/*'),
            'gs://bucket/staging/logs/call_variants/0'
        ]),
    ])
    self.assertEqual(mock_apply_async.call_count, 2)

    mock_run_job.assert_called_once_with(
        _HasAllOf('postprocess_variants', 'gcr.io/dockerimage',
                  'CALLED_VARIANTS=gs://bucket/staging/called_variants/*',
                  'OUTFILE=gs://bucket/output.vcf',
                  'GVCF=gs://bucket/staging/gvcf/*',
                  'GVCF_OUTFILE=gs://bucket/gvcf_output.vcf'),
        'gs://bucket/staging/logs/postprocess_variants')

  @mock.patch.object(multiprocessing, 'Pool')
  @mock.patch('gcp_deepvariant_runner._gcs_object_exist')
  @mock.patch('gcp_deepvariant_runner._can_write_to_bucket')
  def testRunMakeExamples(self, mock_can_write_to_bucket, mock_obj_exist,
                          mock_pool):
    mock_apply_async = mock_pool.return_value.apply_async
    mock_apply_async.return_value = None
    mock_obj_exist.return_value = True
    mock_can_write_to_bucket.return_value = True
    self._argv.extend([
        '--jobs_to_run',
        'make_examples',
        '--make_examples_workers',
        '3',
        '--shards',
        '15',
        '--regions',
        'gs://bucket/region-1.bed',
        'chr1:10,000-10,010',
        'chr2:10000-10010',
        'gs://bucket/region-2.bed',
        'chr3:1,000,000-2,000,000',
        '--gpu',  # GPU should not have any effect.
        '--docker_image_gpu',
        'image_gpu',
        '--job_name_prefix',
        'prefix_',
    ])
    temp_dir = tempfile.gettempdir()
    before_temp_files = os.listdir(temp_dir)
    gcp_deepvariant_runner.run(self._argv)
    after_temp_files = os.listdir(tempfile.gettempdir())
    new_json_files = [os.path.join(temp_dir, item) for item in
                      after_temp_files if item not in before_temp_files]
    new_json_files.sort()
    self.assertEqual(len(new_json_files), 3)  # One json file per worker
    # Verifying Pipeline's API commands
    mock_apply_async.assert_has_calls([
        mock.call(mock.ANY, [
            _HasAllOf('prefix_make_examples', 'gcr.io/dockerimage',
                      'EXAMPLES=gs://bucket/staging/examples/0/*',
                      'INPUT_BAM=gs://bucket/bam', 'INPUT_REF=gs://bucket/ref',
                      'INPUT_BAI=gs://bucket/bam.bai',
                      'INPUT_REGIONS_0=gs://bucket/region-1.bed',
                      'INPUT_REGIONS_1=gs://bucket/region-2.bed',
                      new_json_files[0]),
            'gs://bucket/staging/logs/make_examples/0'
        ]),
        mock.call(mock.ANY, [
            _HasAllOf('prefix_make_examples', 'gcr.io/dockerimage',
                      'EXAMPLES=gs://bucket/staging/examples/0/*',
                      'INPUT_BAM=gs://bucket/bam', 'INPUT_REF=gs://bucket/ref',
                      'INPUT_BAI=gs://bucket/bam.bai',
                      'INPUT_REGIONS_0=gs://bucket/region-1.bed',
                      'INPUT_REGIONS_1=gs://bucket/region-2.bed',
                      new_json_files[1]),
            'gs://bucket/staging/logs/make_examples/1'
        ]),
        mock.call(mock.ANY, [
            _HasAllOf('prefix_make_examples', 'gcr.io/dockerimage',
                      'EXAMPLES=gs://bucket/staging/examples/0/*',
                      'INPUT_BAM=gs://bucket/bam', 'INPUT_REF=gs://bucket/ref',
                      'INPUT_BAI=gs://bucket/bam.bai',
                      'INPUT_REGIONS_0=gs://bucket/region-1.bed',
                      'INPUT_REGIONS_1=gs://bucket/region-2.bed',
                      new_json_files[2]),
            'gs://bucket/staging/logs/make_examples/2'
        ]),
    ])
    self.assertEqual(mock_apply_async.call_count, 3)
    # Verify json files contain correct actions_list.
    command_template = gcp_deepvariant_runner._MAKE_EXAMPLES_COMMAND.format(
        NUM_SHARDS='15', EXTRA_ARGS=
        '--regions \\\'chr1:10,000-10,010 chr2:10000-10010 '
        'chr3:1,000,000-2,000,000 "$INPUT_REGIONS_0" "$INPUT_REGIONS_1"\\\'')

    shards_per_worker = int(15 / 3)
    for worker_index in range(3):
      with open(new_json_files[worker_index]) as json_file:
        recieved_actions_list = json.load(json_file)

      expected_command = command_template.format(
          SHARD_START_INDEX=worker_index * shards_per_worker,
          SHARD_END_INDEX=(worker_index + 1) * shards_per_worker - 1,
          TASK_INDEX='{}', INPUT_BAM='$INPUT_BAM')
      expected_actions_list = [
          {'commands':
           ['-c', expected_command],
           'entrypoint': 'bash',
           'mounts': [{'disk': 'google', 'path': '/mnt/google'}],
           'imageUri': 'gcr.io/dockerimage'}]
      self.assertEqual(len(expected_actions_list), len(recieved_actions_list))
      for i in range(len(expected_actions_list)):
        self.assertEqual(sorted(expected_actions_list[i].items()),
                         sorted(recieved_actions_list[i].items()))

  @mock.patch.object(multiprocessing, 'Pool')
  @mock.patch('gcp_deepvariant_runner._gcs_object_exist')
  @mock.patch('gcp_deepvariant_runner._can_write_to_bucket')
  def testRunMakeExamples_WithGcsfuse(self, mock_can_write_to_bucket,
                                      mock_obj_exist, mock_pool):
    mock_apply_async = mock_pool.return_value.apply_async
    mock_apply_async.return_value = None
    mock_obj_exist.return_value = True
    mock_can_write_to_bucket.return_value = True
    self._argv.extend([
        '--jobs_to_run',
        'make_examples',
        '--make_examples_workers',
        '4',
        '--shards',
        '32',
        '--call_variants_workers',
        '2',
        '--gpu',  # GPU should not have any effect.
        '--docker_image_gpu',
        'image_gpu',
        '--job_name_prefix',
        'prefix_',
        '--gcsfuse',
    ])
    temp_dir = tempfile.gettempdir()
    before_temp_files = os.listdir(temp_dir)
    gcp_deepvariant_runner.run(self._argv)
    after_temp_files = os.listdir(tempfile.gettempdir())
    new_json_files = [os.path.join(temp_dir, item) for item in
                      after_temp_files if item not in before_temp_files]
    new_json_files.sort()
    self.assertEqual(len(new_json_files), 4)  # One json file per worker
    # Verifying Pipeline's API commands
    mock_apply_async.assert_has_calls([
        mock.call(mock.ANY, [
            _HasAllOf('prefix_make_examples', 'gcr.io/dockerimage',
                      'EXAMPLES=gs://bucket/staging/examples/0/*',
                      'INPUT_REF=gs://bucket/ref',
                      'INPUT_BAI=gs://bucket/bam.bai',
                      new_json_files[0]),
            'gs://bucket/staging/logs/make_examples/0'
        ]),
        mock.call(mock.ANY, [
            _HasAllOf('prefix_make_examples', 'gcr.io/dockerimage',
                      'EXAMPLES=gs://bucket/staging/examples/0/*',
                      'INPUT_REF=gs://bucket/ref',
                      'INPUT_BAI=gs://bucket/bam.bai',
                      new_json_files[1]),
            'gs://bucket/staging/logs/make_examples/1'
        ]),
        mock.call(mock.ANY, [
            _HasAllOf('prefix_make_examples', 'gcr.io/dockerimage',
                      'EXAMPLES=gs://bucket/staging/examples/1/*',
                      'INPUT_REF=gs://bucket/ref',
                      'INPUT_BAI=gs://bucket/bam.bai',
                      new_json_files[2]),
            'gs://bucket/staging/logs/make_examples/2'
        ]),
        mock.call(mock.ANY, [
            _HasAllOf('prefix_make_examples', 'gcr.io/dockerimage',
                      'EXAMPLES=gs://bucket/staging/examples/1/*',
                      'INPUT_REF=gs://bucket/ref',
                      'INPUT_BAI=gs://bucket/bam.bai',
                      new_json_files[3]),
            'gs://bucket/staging/logs/make_examples/3'
        ]),
    ])
    self.assertEqual(mock_apply_async.call_count, 4)
    # Verify json files contain correct actions_list.
    command_template = gcp_deepvariant_runner._MAKE_EXAMPLES_COMMAND.format(
        NUM_SHARDS='32', EXTRA_ARGS='')
    shards_per_worker = int(32 / 4)
    for worker_index in range(4):
      with open(new_json_files[worker_index]) as json_file:
        recieved_actions_list = json.load(json_file)

      shard_start_index = worker_index * shards_per_worker
      shard_end_index = (worker_index + 1) * shards_per_worker - 1
      expected_command = command_template.format(
          SHARD_START_INDEX=shard_start_index,
          SHARD_END_INDEX=shard_end_index,
          TASK_INDEX='{}',
          INPUT_BAM='/mnt/google/input-gcsfused-{}/bam')
      expected_actions_list = []
      for shard_index in range(shard_start_index, shard_end_index + 1):
        gcsfuse_create_command = gcp_deepvariant_runner._GCSFUSE_CREATE_COMMAND_TEMPLATE.format(
            BUCKET='bucket',
            LOCAL_DIR=gcp_deepvariant_runner._GCSFUSE_LOCAL_DIR_TEMPLATE.format(
                SHARD_INDEX=shard_index))
        expected_actions_list.append(
            {'commands':
             ['-c', gcsfuse_create_command],
             'entrypoint': '/bin/sh',
             'flags': ['RUN_IN_BACKGROUND', 'ENABLE_FUSE'],
             'mounts': [{'disk': 'google', 'path': '/mnt/google'}],
             'imageUri': 'gcr.io/cloud-genomics-pipelines/gcsfuse'})
        gcsfuse_verify_command = gcp_deepvariant_runner._GCSFUSE_VERIFY_COMMAND_TEMPLATE.format(
            LOCAL_DIR=gcp_deepvariant_runner._GCSFUSE_LOCAL_DIR_TEMPLATE.format(
                SHARD_INDEX=shard_index))
        expected_actions_list.append(
            {'commands':
             ['-c', gcsfuse_verify_command],
             'entrypoint': '/bin/sh',
             'mounts': [{'disk': 'google', 'path': '/mnt/google'}],
             'imageUri': 'gcr.io/cloud-genomics-pipelines/gcsfuse'})
      expected_actions_list.append(
          {'commands':
           ['-c', expected_command],
           'entrypoint': 'bash',
           'mounts': [{'disk': 'google', 'path': '/mnt/google'}],
           'imageUri': 'gcr.io/dockerimage'})
      self.assertEqual(len(expected_actions_list), len(recieved_actions_list))
      for i in range(len(expected_actions_list)):
        self.assertEqual(sorted(expected_actions_list[i].items()),
                         sorted(recieved_actions_list[i].items()))

  @mock.patch.object(multiprocessing, 'Pool')
  @mock.patch('gcp_deepvariant_runner._gcs_object_exist')
  @mock.patch('gcp_deepvariant_runner._can_write_to_bucket')
  def testRunCallVariants(self, mock_can_write_to_bucket, mock_obj_exist,
                          mock_pool):
    mock_apply_async = mock_pool.return_value.apply_async
    mock_apply_async.return_value = None
    mock_obj_exist.return_value = True
    mock_can_write_to_bucket.return_value = True
    self._argv.extend([
        '--make_examples_workers',
        '3',
        '--jobs_to_run',
        'call_variants',
        '--call_variants_workers',
        '3',
        '--call_variants_cores_per_worker',
        '5',
        '--shards',
        '15',
    ])
    gcp_deepvariant_runner.run(self._argv)

    mock_apply_async.assert_has_calls([
        mock.call(mock.ANY, [
            _HasAllOf('call_variants', 'gcr.io/dockerimage',
                      'CALL_VARIANTS_SHARD_INDEX=0'),
            'gs://bucket/staging/logs/call_variants/0'
        ]),
        mock.call(mock.ANY, [
            _HasAllOf('call_variants', 'gcr.io/dockerimage',
                      'CALL_VARIANTS_SHARD_INDEX=1'),
            'gs://bucket/staging/logs/call_variants/1'
        ]),
        mock.call(mock.ANY, [
            _HasAllOf('call_variants', 'gcr.io/dockerimage',
                      'CALL_VARIANTS_SHARD_INDEX=2'),
            'gs://bucket/staging/logs/call_variants/2'
        ]),
    ])
    self.assertEqual(mock_apply_async.call_count, 3)

  @mock.patch.object(multiprocessing, 'Pool')
  @mock.patch('gcp_deepvariant_runner._gcs_object_exist')
  @mock.patch('gcp_deepvariant_runner._can_write_to_bucket')
  def testRunCallVariants_GPU(self, mock_can_write_to_bucket, mock_obj_exist,
                              mock_pool):
    mock_apply_async = mock_pool.return_value.apply_async
    mock_apply_async.return_value = None
    mock_obj_exist.return_value = True
    mock_can_write_to_bucket.return_value = True
    self._argv.extend([
        '--make_examples_workers',
        '3',
        '--jobs_to_run',
        'call_variants',
        '--call_variants_workers',
        '3',
        '--call_variants_cores_per_worker',
        '5',
        '--shards',
        '15',
        '--gpu',
        '--docker_image_gpu',
        'gcr.io/dockerimage_gpu',
    ])
    gcp_deepvariant_runner.run(self._argv)

    mock_apply_async.assert_has_calls([
        mock.call(mock.ANY, [
            _HasAllOf('call_variants', 'gcr.io/dockerimage_gpu',
                      'nvidia-tesla-k80', 'CALL_VARIANTS_SHARD_INDEX=0'),
            'gs://bucket/staging/logs/call_variants/0'
        ]),
        mock.call(mock.ANY, [
            _HasAllOf('call_variants', 'gcr.io/dockerimage_gpu',
                      'nvidia-tesla-k80', 'CALL_VARIANTS_SHARD_INDEX=1'),
            'gs://bucket/staging/logs/call_variants/1'
        ]),
        mock.call(mock.ANY, [
            _HasAllOf('call_variants', 'gcr.io/dockerimage_gpu',
                      'nvidia-tesla-k80', 'CALL_VARIANTS_SHARD_INDEX=2'),
            'gs://bucket/staging/logs/call_variants/2'
        ]),
    ])
    self.assertEqual(mock_apply_async.call_count, 3)

  @mock.patch.object(gke_cluster.GkeCluster, '__init__', return_value=None)
  @mock.patch.object(gke_cluster.GkeCluster, 'deploy_pod')
  @mock.patch.object(gke_cluster.GkeCluster, '_cluster_exists')
  @mock.patch('gcp_deepvariant_runner._gcs_object_exist')
  @mock.patch('gcp_deepvariant_runner._can_write_to_bucket')
  def testRunCallVariants_TPU(self, mock_can_write_to_bucket, mock_obj_exist,
                              mock_cluster_exists, mock_deploy_pod, mock_init):
    mock_obj_exist.return_value = True
    mock_can_write_to_bucket.return_value = True
    mock_cluster_exists.return_value = True
    self._argv.extend([
        '--jobs_to_run',
        'call_variants',
        '--call_variants_workers',
        '1',
        '--shards',
        '15',
        '--tpu',
        '--gke_cluster_name',
        'foo-cluster',
        '--gke_cluster_zone',
        'us-central1-c',
        '--docker_image',
        'gcr.io/dockerimage',
    ])
    gcp_deepvariant_runner.run(self._argv)
    mock_init.assert_has_calls([
        mock.call(
            'foo-cluster', None, 'us-central1-c', create_if_not_exist=False),
        mock.call('foo-cluster', None, 'us-central1-c')
    ])
    self.assertEqual(mock_init.call_count, 2)

    mock_deploy_pod.assert_called_once_with(
        pod_config=mock.ANY,
        pod_name=AnyStringWith('deepvariant-'),
        retries=1,
        wait=True)

  def testRunFailCallVariants_TPU(self):
    self._argv.extend([
        '--jobs_to_run',
        'call_variants',
        '--call_variants_workers',
        '1',
        '--shards',
        '15',
        '--tpu',
        '--gke_cluster_name',
        'foo-cluster',
        '--gke_cluster_zone',
        'us-central1-c',
        '--docker_image',
        'gcr.io/dockerimage',
    ])
    with self.assertRaises(ValueError):
      gcp_deepvariant_runner.run(self._argv)

  @mock.patch('gcp_deepvariant_runner._run_job')
  @mock.patch('gcp_deepvariant_runner._gcs_object_exist')
  @mock.patch('gcp_deepvariant_runner._can_write_to_bucket')
  def testRunPostProcessVariants(self, mock_can_write_to_bucket, mock_obj_exist,
                                 mock_run_job):
    mock_obj_exist.return_value = True
    mock_can_write_to_bucket.return_value = True
    self._argv.extend([
        '--jobs_to_run',
        'postprocess_variants',
        '--shards',
        '15',
        '--gpu',  # GPU should not have any effect.
        '--gvcf_outfile',
        'gvcf-folder-path',
        '--docker_image_gpu',
        'gcr.io/dockerimage_gpu',
    ])
    gcp_deepvariant_runner.run(self._argv)
    mock_run_job.assert_called_once_with(
        _HasAllOf('postprocess_variants', 'gcr.io/dockerimage',
                  'CALLED_VARIANTS=gs://bucket/staging/called_variants/*',
                  'INPUT_REF=gs://bucket/ref', 'SHARDS=15',
                  'CALL_VARIANTS_SHARDS=1', 'INPUT_REF_FAI=gs://bucket/ref.fai',
                  'OUTFILE=gs://bucket/output.vcf'),
        'gs://bucket/staging/logs/postprocess_variants')

  @mock.patch.object(storage.bucket.Bucket, 'test_iam_permissions')
  def testRunFailsMissingInput(self, mock_bucket_iam):
    mock_bucket_iam.return_value = (
        gcp_deepvariant_runner._ROLE_STORAGE_OBJ_CREATOR)
    self._argv.extend([
        '--jobs_to_run',
        'postprocess_variants',
        '--shards',
        '15',
        '--gpu',  # GPU should not have any effect.
        '--gvcf_outfile',
        'gvcf-folder-path',
        '--docker_image_gpu',
        'gcr.io/dockerimage_gpu',
    ])
    with self.assertRaises(ValueError):
      gcp_deepvariant_runner.run(self._argv)

  @mock.patch.object(storage.blob.Blob, 'exists')
  def testRunFailsCannotWriteOutputBucket(self, mock_blob_exists):
    mock_blob_exists.return_value = True
    self._argv.extend([
        '--jobs_to_run',
        'postprocess_variants',
        '--shards',
        '15',
        '--gpu',  # GPU should not have any effect.
        '--gvcf_outfile',
        'gvcf-folder-path',
        '--docker_image_gpu',
        'gcr.io/dockerimage_gpu',
    ])
    with self.assertRaises(ValueError):
      gcp_deepvariant_runner.run(self._argv)


class UtilsTest(unittest.TestCase):

  def testIsValidGcsPath(self):
    invalid_paths = ['gs://', '://bucket', 'gs//bucket', 'gs:/bucket']
    for path in invalid_paths:
      self.assertEqual(gcp_deepvariant_runner._is_valid_gcs_path(path), False)

    valid_paths = ['gs://bucket/', 'gs://bucket/obj', 'gs://bucket/dir/obj']
    for path in valid_paths:
      self.assertEqual(gcp_deepvariant_runner._is_valid_gcs_path(path), True)

  def testGetGcsBucket(self):
    invalid_paths = ['gs://', '://bucket', 'gs//bucket', 'gs:/bucket']
    for path in invalid_paths:
      with self.assertRaisesRegex(
          ValueError, 'Invalid GCS path provided: %s' % path):
        gcp_deepvariant_runner._get_gcs_bucket(path)

    valid_paths = ['gs://bucket/', 'gs://bucket/obj', 'gs://bucket/dir/obj']
    for path in valid_paths:
      self.assertEqual(gcp_deepvariant_runner._get_gcs_bucket(path), 'bucket')

  def testGetGcsRelativePath(self):
    invalid_paths = ['gs://', '://bucket', 'gs//bucket', 'gs:/bucket']
    for path in invalid_paths:
      with self.assertRaisesRegex(
          ValueError, 'Invalid GCS path provided: %s' % path):
        gcp_deepvariant_runner._get_gcs_relative_path(path)

    paths_objects = {'gs://bucket': '',
                     'gs://bucket/': '',
                     'gs://bucket/obj': 'obj',
                     'gs://bucket/obj/': 'obj',
                     'gs://bucket/dir/obj': 'dir/obj',
                     'gs://bucket/dir/obj/': 'dir/obj'}
    for path, obj in paths_objects.items():
      self.assertEqual(gcp_deepvariant_runner._get_gcs_relative_path(path), obj)

  def test_meets_gcp_label_restrictions(self):
    valid_labels = ['label', 'label1', 'label1_2', 'label1_2-3', 'l']
    invalid_labels = ['Label', '1label', 'label$', '',
                      'very_very_very_very_long_prefix_for_a_label_']
    for label in valid_labels:
      self.assertEqual(
          gcp_deepvariant_runner._meets_gcp_label_restrictions(label), True)

    for label in invalid_labels:
      self.assertEqual(
          gcp_deepvariant_runner._meets_gcp_label_restrictions(label), False)

  def testGenerateActionsForMakeExample(self):
    command_template = gcp_deepvariant_runner._MAKE_EXAMPLES_COMMAND.format(
        NUM_SHARDS='6', EXTRA_ARGS=' --extra-args')
    expected_command = command_template.format(SHARD_START_INDEX=1,
                                               SHARD_END_INDEX=4,
                                               TASK_INDEX='{}',
                                               INPUT_BAM='$INPUT_BAM')
    expected_actions_list = [
        {'commands':
         ['-c', expected_command],
         'entrypoint': 'bash',
         'mounts': [{'disk': 'google', 'path': '/mnt/google'}],
         'imageUri': 'gcr.io/temp/image'}]

    actions_list = gcp_deepvariant_runner._generate_actions_for_make_example(
        1, 4, 'gs://temp-bucket/path/input.bam', False, 'gcr.io/temp/image',
        command_template)
    self.assertListEqual(actions_list, expected_actions_list)
    gcp_deepvariant_runner._write_actions_to_temp_file(actions_list)

  def testGenerateActionsForMakeExampleGcsfuse(self):
    command_template = gcp_deepvariant_runner._MAKE_EXAMPLES_COMMAND.format(
        NUM_SHARDS='6', EXTRA_ARGS=' --extra-args')
    expected_command = command_template.format(
        SHARD_START_INDEX=2, SHARD_END_INDEX=4, TASK_INDEX='{}',
        INPUT_BAM='/mnt/google/input-gcsfused-{}/path/input.bam')
    expected_actions_list = []
    for shard_index in range(2, 4 + 1):
      gcsfuse_create_command = gcp_deepvariant_runner._GCSFUSE_CREATE_COMMAND_TEMPLATE.format(
          BUCKET='temp-bucket',
          LOCAL_DIR=gcp_deepvariant_runner._GCSFUSE_LOCAL_DIR_TEMPLATE.format(
              SHARD_INDEX=shard_index))
      expected_actions_list.append(
          {'commands':
           ['-c', gcsfuse_create_command],
           'entrypoint': '/bin/sh',
           'flags': ['RUN_IN_BACKGROUND', 'ENABLE_FUSE'],
           'mounts': [{'disk': 'google', 'path': '/mnt/google'}],
           'imageUri': 'gcr.io/cloud-genomics-pipelines/gcsfuse'})
      gcsfuse_verify_command = gcp_deepvariant_runner._GCSFUSE_VERIFY_COMMAND_TEMPLATE.format(
          LOCAL_DIR=gcp_deepvariant_runner._GCSFUSE_LOCAL_DIR_TEMPLATE.format(
              SHARD_INDEX=shard_index))
      expected_actions_list.append(
          {'commands':
           ['-c', gcsfuse_verify_command],
           'entrypoint': '/bin/sh',
           'mounts': [{'disk': 'google', 'path': '/mnt/google'}],
           'imageUri': 'gcr.io/cloud-genomics-pipelines/gcsfuse'})
    expected_actions_list.append(
        {'commands':
         ['-c', expected_command],
         'entrypoint': 'bash',
         'mounts': [{'disk': 'google', 'path': '/mnt/google'}],
         'imageUri': 'gcr.io/temp/image'})

    actions_list = gcp_deepvariant_runner._generate_actions_for_make_example(
        2, 4, 'gs://temp-bucket/path/input.bam', True, 'gcr.io/temp/image',
        gcp_deepvariant_runner._MAKE_EXAMPLES_COMMAND.format(
            NUM_SHARDS='6', EXTRA_ARGS=' --extra-args'))
    self.assertListEqual(actions_list, expected_actions_list)
    gcp_deepvariant_runner._write_actions_to_temp_file(actions_list)

if __name__ == '__main__':
  unittest.main()
