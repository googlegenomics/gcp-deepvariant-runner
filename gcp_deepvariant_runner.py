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
r"""Runs the DeepVariant pipeline using the Google Genomics Pipelines API.

To run this script, you also need the pipelines tool in your $PATH.  You can
install it using:

$ go get github.com/googlegenomics/pipelines-tools/...

Sample run command (please run 'python gcp_deepvariant_runner.py --help' for
details on all available options):
$ python gcp_deepvariant_runner.py \
   --project alphanumeric_project_id \
   --zones 'us-*' \
   --docker_image gcr.io/path_to_deepvariant_cpu_docker_image \
   --outfile gs://bucket/output.vcf \
   --staging gs://bucket/staging \
   --model gs://path_to_deepvariant_model_folder \
   --bam gs://path_to_bam_file.bam \
   --ref gs://path_to_fasta_file.fasta
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import datetime
import json
import logging
import multiprocessing
import os
import re
import subprocess
import tempfile
import time
import urllib
import uuid

import gke_cluster
from google.api_core import exceptions as google_exceptions
from google.cloud import storage


_BAI_FILE_SUFFIX = '.bai'
_BAM_FILE_SUFFIX = '.bam'
_CRAM_FILE_SUFFIX = '.cram'
_FAI_FILE_SUFFIX = '.fai'
_GZ_FILE_SUFFIX = '.gz'
_GZI_FILE_SUFFIX = '.gzi'

_MAKE_EXAMPLES_JOB_NAME = 'make_examples'
_CALL_VARIANTS_JOB_NAME = 'call_variants'
_POSTPROCESS_VARIANTS_JOB_NAME = 'postprocess_variants'
_DEFAULT_BOOT_DISK_SIZE_GB = '50'
_ROLE_STORAGE_OBJ_CREATOR = ['storage.objects.create']

_GCSFUSE_IMAGE = 'gcr.io/cloud-genomics-pipelines/gcsfuse'
_GCSFUSE_LOCAL_DIR_TEMPLATE = '/mnt/google/input-gcsfused-{SHARD_INDEX}/'

_GCSFUSE_CREATE_COMMAND_TEMPLATE = r"""
mkdir -p {LOCAL_DIR}
/usr/local/bin/entrypoint.sh --implicit-dirs --foreground {BUCKET} {LOCAL_DIR}
"""

_GCSFUSE_VERIFY_COMMAND_TEMPLATE = r"""
/usr/local/bin/entrypoint.sh wait {LOCAL_DIR}
"""

_MAKE_EXAMPLES_COMMAND = r"""
seq {{SHARD_START_INDEX}} {{SHARD_END_INDEX}} | parallel --halt 2 \
  /opt/deepvariant/bin/make_examples \
    --mode calling \
    --examples "$EXAMPLES"/examples_output.tfrecord@{NUM_SHARDS}.gz \
    --reads "{{INPUT_BAM}}" \
    --ref "$INPUT_REF" \
    --task {{TASK_INDEX}} \
    {EXTRA_ARGS}
"""

_CALL_VARIANTS_COMMAND = r"""
/opt/deepvariant/bin/call_variants
  --examples "${{EXAMPLES}}"/examples_output.tfrecord@"${{SHARDS}}".gz
  --outfile "${{CALLED_VARIANTS}}"/call_variants_output.tfrecord-"$(printf "%05d" "${{CALL_VARIANTS_SHARD_INDEX}}")"-of-"$(printf "%05d" "${{CALL_VARIANTS_SHARDS}}")".gz
  --checkpoint "${{MODEL}}"/model.ckpt
  {EXTRA_ARGS}
"""

_POSTPROCESS_VARIANTS_COMMAND = r"""
/opt/deepvariant/bin/postprocess_variants
    --ref "${{INPUT_REF}}"
    --infile "${{CALLED_VARIANTS}}"/call_variants_output.tfrecord@"${{CALL_VARIANTS_SHARDS}}".gz
    --outfile "${{OUTFILE}}"
    {EXTRA_ARGS}
"""

_NOW_STR = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# This is used by the cancel script and must not be changed unless it is updated
# there as well.
_DEEPVARIANT_LABEL_KEY = 'deepvariant-operation-label'

_POD_CONFIG_TEMPLATE = r"""
{{
    "kind": "Pod",
    "apiVersion": "v1",
    "metadata": {{
        "name": "{POD_NAME}",
        "annotations": {{
            "tf-version.cloud-tpus.google.com": "1.12"
        }}
    }},
    "spec": {{
        "containers": [
            {{
                "name": "deepvaraint",
                "image": "{DOCKER_IMAGE}",
                "command": [
                    "/opt/deepvariant/bin/call_variants",
                    "--use_tpu",
                    "--outfile={OUTFILE}",
                    "--examples={EXAMPLES}",
                    "--checkpoint={MODEL_CHECKPOINT}",
                    "--batch_size={BATCH_SIZE}"
                ],
                "resources": {{
                    "limits": {{
                        "{TPU_RESOURCE}": "8"
                    }}
                }}
            }}
        ],
        "restartPolicy": "Never"
    }}
}}
"""


def _get_staging_examples_folder_to_write(pipeline_args,
                                          make_example_worker_index):
  """Returns the folder to store examples from make_examples job."""
  # call_variants_workers is less than or equal to make_examples_workers.
  folder_index = int(
      make_example_worker_index * pipeline_args.call_variants_workers /
      pipeline_args.make_examples_workers)
  return os.path.join(*[pipeline_args.staging, 'examples', str(folder_index)])


def _get_staging_examples_folder_to_read(pipeline_args,
                                         call_variants_worker_index):
  """Returns the folder to read examples from make_examples job."""
  return os.path.join(
      *[pipeline_args.staging, 'examples',
        str(call_variants_worker_index)])


def _get_staging_gvcf_folder(pipeline_args):
  """Returns the folder to store gVCF TF records from make_examples job."""
  return os.path.join(pipeline_args.staging, 'gvcf')


def _get_staging_called_variants_folder(pipeline_args):
  """Returns the folder to store called variants from call_variants job."""
  return os.path.join(pipeline_args.staging, 'called_variants')


def _get_base_job_args(pipeline_args):
  """Base arguments that are common among all jobs."""
  pvm_attempts = 0
  if pipeline_args.preemptible:
    pvm_attempts = pipeline_args.max_preemptible_tries

  job_args = [
      'pipelines', '--project', pipeline_args.project, 'run', '--attempts',
      str(pipeline_args.max_non_preemptible_tries), '--pvm-attempts',
      str(pvm_attempts), '--boot-disk-size', _DEFAULT_BOOT_DISK_SIZE_GB,
      '--output-interval',
      str(pipeline_args.logging_interval_sec) + 's', '--zones'
  ] + pipeline_args.zones
  if pipeline_args.network:
    job_args.extend(['--network', pipeline_args.network])
  if pipeline_args.subnetwork:
    job_args.extend(['--subnetwork', pipeline_args.subnetwork])
  if pipeline_args.operation_label:
    job_args.extend([
        '--labels', _DEEPVARIANT_LABEL_KEY + '=' + pipeline_args.operation_label
    ])

  return job_args


def _generate_actions_for_make_example(
    shard_start_index, shard_end_index, input_bam_file, is_gcsfuse_activated,
    deep_variant_image, make_example_command_template):
  """Returns a dictionary of actions for execution of make_examples stage.

  Args:
    shard_start_index: Index of first assigned shard to this worker (inclusive).
    shard_end_index: Index of last assigned shard to this worker (inclusive).
    input_bam_file: full path of bam file on gcs (gs://bucket/path/file.bam).
    is_gcsfuse_activated: whether or not read input bam file using gcsfuse.
    deep_variant_image: DeepVariant image given using --docker_image flag.
    make_example_command_template: template command used in actions list.
  """
  gcs_bucket = _get_gcs_bucket(input_bam_file)
  bam_file_relative_path = _get_gcs_relative_path(input_bam_file)

  actions = []
  if is_gcsfuse_activated:
    for shard_index in range(shard_start_index, shard_end_index + 1):
      local_dir = _GCSFUSE_LOCAL_DIR_TEMPLATE.format(SHARD_INDEX=shard_index)
      gcsfuse_create_command = _GCSFUSE_CREATE_COMMAND_TEMPLATE.format(
          BUCKET=gcs_bucket, LOCAL_DIR=local_dir)
      actions.append({'imageUri': _GCSFUSE_IMAGE,
                      'commands': ['-c', gcsfuse_create_command],
                      'entrypoint': '/bin/sh',
                      'flags': ['RUN_IN_BACKGROUND', 'ENABLE_FUSE'],
                      'mounts': [{'disk': 'google', 'path': '/mnt/google'}]})
      gcsfuse_verify_command = _GCSFUSE_VERIFY_COMMAND_TEMPLATE.format(
          LOCAL_DIR=local_dir)
      actions.append({'imageUri': _GCSFUSE_IMAGE,
                      'commands': ['-c', gcsfuse_verify_command],
                      'entrypoint': '/bin/sh',
                      'mounts': [{'disk': 'google', 'path': '/mnt/google'}]})

    local_bam_template = (_GCSFUSE_LOCAL_DIR_TEMPLATE.format(SHARD_INDEX='{}') +
                          bam_file_relative_path)
  else:
    local_bam_template = '$INPUT_BAM'

  make_example_command = make_example_command_template.format(
      SHARD_START_INDEX=shard_start_index, SHARD_END_INDEX=shard_end_index,
      TASK_INDEX='{}', INPUT_BAM=local_bam_template)
  actions.append(
      {'imageUri': deep_variant_image,
       'commands': ['-c', make_example_command],
       'entrypoint': 'bash',
       'mounts': [{'disk': 'google', 'path': '/mnt/google'}]})
  return actions


def _write_actions_to_temp_file(actions):
  micro_second = int(round(time.time() * 1000000))
  with tempfile.NamedTemporaryFile(mode='w', prefix=str(micro_second),
                                   suffix='.json', delete=False) as temp_file:
    json.dump(actions, temp_file)
  return temp_file.name


def _run_job(run_args, log_path):
  """Runs a job using the pipelines CLI tool.

  Args:
    run_args: A list of arguments (type string) to pass to the pipelines tool.
    log_path: Path to which pipelines API worker writes its log into.
  Raises:
    RuntimeError: if there was an error running the pipeline.
  """

  process = subprocess.Popen(
      run_args,
      stdin=subprocess.PIPE,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
      env={'PATH': os.environ['PATH']})

  try:
    stdout, stderr = process.communicate()
    if process.returncode == 0:
      return
  except KeyboardInterrupt:
    raise RuntimeError('Job cancelled by user')

  logging.error('Job failed with error %s \n %s. Job args: %s', stdout, stderr,
                run_args)
  logging.error('For more information, consult the worker log at %s', log_path)
  raise RuntimeError('Job failed with error %s' % stderr)


def _is_valid_gcs_path(gcs_path):
  """Returns true if the given path is a valid GCS path.

  Args:
    gcs_path: (str) a path to directory or an obj on GCS.
  """
  return (urllib.parse.urlparse(gcs_path).scheme == 'gs' and
          urllib.parse.urlparse(gcs_path).netloc != '')


def _gcs_object_exist(gcs_obj_path):
  """Returns true if the given path is a valid object on GCS.

  Args:
    gcs_obj_path: (str) a path to an obj on GCS.
  """
  try:
    storage_client = storage.Client()
    bucket_name = _get_gcs_bucket(gcs_obj_path)
    obj_name = _get_gcs_relative_path(gcs_obj_path)
    bucket = storage_client.bucket(bucket_name)
    obj = bucket.blob(obj_name)
    return obj.exists()
  except google_exceptions.Forbidden as e:
    logging.error('Missing GCS object: %s', str(e))
    return False


def _can_write_to_bucket(bucket_name):
  """Returns True if caller is authorized to write into the bucket.

  Args:
    bucket_name: (str) name of the bucket
  """
  if not bucket_name:
    return False
  try:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    return (bucket.test_iam_permissions(_ROLE_STORAGE_OBJ_CREATOR) ==
            _ROLE_STORAGE_OBJ_CREATOR)
  except google_exceptions.Forbidden as e:
    logging.error('Write access denied: %s', str(e))
    return False


def _get_gcs_bucket(gcs_path):
  """Returns bucket name from gcs_path.

    E.g.: gs://bucket/path0/path1/file' --> bucket

  Args:
    gcs_path: (str) a Google cloud storage path.
  """
  if not _is_valid_gcs_path(gcs_path):
    raise ValueError('Invalid GCS path provided: %s' % gcs_path)
  return urllib.parse.urlparse(gcs_path).netloc


def _get_gcs_relative_path(gcs_path):
  """Returns anything after bucket name.

    E.g.: gs://bucket/path0/path1/file --> path0/path1/file

  Args:
    gcs_path: (str) a valid Google cloud storage path.
  """
  if not _is_valid_gcs_path(gcs_path):
    raise ValueError('Invalid GCS path provided: %s' % gcs_path)
  return urllib.parse.urlparse(gcs_path).path.strip('/')


def _meets_gcp_label_restrictions(label):
  """Does given string meet GCP label restrictions?"""
  max_label_len = 63
  max_suffix_len = max(len(_MAKE_EXAMPLES_JOB_NAME),
                       len(_CALL_VARIANTS_JOB_NAME),
                       len(_POSTPROCESS_VARIANTS_JOB_NAME))
  max_repetition = max_label_len - max_suffix_len - 1
  return re.match(re.compile(r'^[a-z][a-z0-9_-]{,%d}$' % max_repetition),
                  label) is not None


def _run_make_examples(pipeline_args):
  """Runs the make_examples job."""

  def get_region_paths(regions):
    return [
        region for region in regions or [] if _is_valid_gcs_path(region)
    ]

  def get_region_literals(regions):
    return [
        region for region in regions or [] if not _is_valid_gcs_path(region)
    ]

  def get_extra_args():
    """Optional arguments that are specific to make_examples binary."""
    extra_args = []
    if pipeline_args.gvcf_outfile:
      extra_args.extend(
          ['--gvcf', '"$GVCF"/gvcf_output.tfrecord@{NUM_SHARDS}.gz'.format(
              NUM_SHARDS=pipeline_args.shards)])
    if pipeline_args.gvcf_gq_binsize:
      extra_args.extend(
          ['--gvcf_gq_binsize',
           str(pipeline_args.gvcf_gq_binsize)])
    if pipeline_args.regions:
      num_localized_region_paths = len(get_region_paths(pipeline_args.regions))
      localized_region_paths = list(map('"$INPUT_REGIONS_{0}"'.format,
                                        range(num_localized_region_paths)))
      region_literals = get_region_literals(pipeline_args.regions)
      extra_args.extend([
          '--regions',
          r'\'%s\'' % ' '.join(region_literals + localized_region_paths)
      ])
    if pipeline_args.sample_name:
      extra_args.extend(['--sample_name', pipeline_args.sample_name])
    if pipeline_args.hts_block_size:
      extra_args.extend(['--hts_block_size', str(pipeline_args.hts_block_size)])
    return extra_args

  command = _MAKE_EXAMPLES_COMMAND.format(
      NUM_SHARDS=pipeline_args.shards,
      EXTRA_ARGS=' '.join(get_extra_args()))

  machine_type = 'custom-{0}-{1}'.format(
      pipeline_args.make_examples_cores_per_worker,
      pipeline_args.make_examples_ram_per_worker_gb * 1024)

  num_workers = min(pipeline_args.make_examples_workers, pipeline_args.shards)
  shards_per_worker = pipeline_args.shards / num_workers
  threads = multiprocessing.Pool(num_workers)
  results = []
  for i in range(num_workers):
    outputs = [
        'EXAMPLES=' + _get_staging_examples_folder_to_write(pipeline_args, i) +
        '/*'
    ]
    if pipeline_args.gvcf_outfile:
      outputs.extend(['GVCF=' + _get_staging_gvcf_folder(pipeline_args) + '/*'])
    inputs = [
        'INPUT_BAI=' + pipeline_args.bai,
        'INPUT_REF=' + pipeline_args.ref,
        'INPUT_REF_FAI=' + pipeline_args.ref_fai,
    ] + [
        'INPUT_REGIONS_%s=%s' % (k, region_path)
        for k, region_path in enumerate(
            get_region_paths(pipeline_args.regions))
    ]
    if not pipeline_args.gcsfuse:
      # Without gcsfuse, BAM file must be copied as one of the input files.
      inputs.extend(['INPUT_BAM=' + pipeline_args.bam])

    if pipeline_args.ref_gzi:
      inputs.extend([pipeline_args.ref_gzi])
    shard_start_index = int(i * shards_per_worker)
    shard_end_index = int((i + 1) * shards_per_worker - 1)

    job_name = pipeline_args.job_name_prefix + _MAKE_EXAMPLES_JOB_NAME
    output_path = os.path.join(pipeline_args.logging, _MAKE_EXAMPLES_JOB_NAME,
                               str(i))

    actions_array = _generate_actions_for_make_example(
        shard_start_index, shard_end_index, pipeline_args.bam,
        pipeline_args.gcsfuse, pipeline_args.docker_image, command)
    actions_filename = _write_actions_to_temp_file(actions_array)

    run_args = _get_base_job_args(pipeline_args) + [
        '--name', job_name, '--vm-labels', 'dv-job-name=' + job_name, '--image',
        pipeline_args.docker_image, '--output', output_path, '--inputs',
        ','.join(inputs), '--outputs', ','.join(outputs), '--machine-type',
        machine_type, '--disk-size',
        str(pipeline_args.make_examples_disk_per_worker_gb), actions_filename]
    results.append(threads.apply_async(_run_job, [run_args, output_path]))

  _wait_for_results(threads, results)


def _wait_for_results(threads, results):
  threads.close()
  try:
    threads.join()
  except KeyboardInterrupt:
    raise RuntimeError('Cancelled')

  for result in results:
    if result:
      result.get()


def _deploy_call_variants_pod(pod_name, cluster, pipeline_args):
  """Deploys a pod into Kubernetes cluster, and waits on completion."""
  # TODO(b/112042350): Add support for custom network and subnetwork.
  infile = os.path.join(
      _get_staging_examples_folder_to_read(pipeline_args, 0),
      'examples_output.tfrecord@{}.gz'.format(str(pipeline_args.shards)))
  outfile = os.path.join(
      _get_staging_called_variants_folder(pipeline_args),
      'call_variants_output.tfrecord-00000-of-00001.gz')
  pod_config = _POD_CONFIG_TEMPLATE.format(
      POD_NAME=pod_name,
      DOCKER_IMAGE=pipeline_args.docker_image,
      EXAMPLES=infile,
      OUTFILE=outfile,
      MODEL_CHECKPOINT=pipeline_args.model + '/model.ckpt',
      TPU_RESOURCE=('cloud-tpus.google.com/preemptible-v2' if
                    pipeline_args.preemptible else 'cloud-tpus.google.com/v2'),
      BATCH_SIZE=pipeline_args.call_variants_batch_size)

  if pipeline_args.preemptible:
    num_tries = pipeline_args.max_preemptible_tries
  else:
    num_tries = pipeline_args.max_non_preemptible_tries

  cluster.deploy_pod(
      pod_config=pod_config,
      pod_name=pod_name,
      retries=num_tries - 1,
      wait=True)


def _run_call_variants_with_kubernetes(pipeline_args):
  """Runs call_variants step with kubernetes."""
  # Setup Kubernetes cluster.
  if pipeline_args.gke_cluster_name:
    # Reuse provided GKE cluster.
    new_cluster_created = False
    cluster = gke_cluster.GkeCluster(pipeline_args.gke_cluster_name,
                                     pipeline_args.gke_cluster_region,
                                     pipeline_args.gke_cluster_zone)
  else:
    # Create a new GKE cluster.
    job_name_label = pipeline_args.job_name_prefix + _CALL_VARIANTS_JOB_NAME
    extra_args = [
        '--num-nodes=1', '--enable-kubernetes-alpha', '--enable-ip-alias',
        '--create-subnetwork=', '--node-labels=job_name=' + job_name_label,
        '--scopes=cloud-platform', '--enable-tpu', '--no-enable-autorepair',
        '--project', pipeline_args.project, '--quiet'
    ]
    cluster_name = 'deepvariant-' + _NOW_STR + uuid.uuid4().hex[:5]
    cluster = gke_cluster.GkeCluster(
        cluster_name,
        pipeline_args.gke_cluster_region,
        pipeline_args.gke_cluster_zone,
        alpha_cluster=False,
        extra_create_args=extra_args)
    new_cluster_created = True

  # Deploy call_variants pod.
  pod_name = 'deepvariant-' + _NOW_STR + '-' + uuid.uuid4().hex[:5]
  try:
    _deploy_call_variants_pod(pod_name, cluster, pipeline_args)
  except KeyboardInterrupt:
    cluster.delete_pod(pod_name)
    raise RuntimeError('Job cancelled by user.')
  finally:
    if new_cluster_created:
      cluster.delete_cluster(wait=False)


def _run_call_variants_with_pipelines_api(pipeline_args):
  """Runs call_variants step with pipelines API."""

  def get_extra_args():
    """Optional arguments that are specific to call_variants binary."""
    return ['--batch_size', str(pipeline_args.call_variants_batch_size)]

  command = _CALL_VARIANTS_COMMAND.format(EXTRA_ARGS=' '.join(get_extra_args()))

  machine_type = 'custom-{0}-{1}'.format(
      pipeline_args.call_variants_cores_per_worker,
      pipeline_args.call_variants_ram_per_worker_gb * 1024)

  num_workers = min(pipeline_args.call_variants_workers, pipeline_args.shards)
  threads = multiprocessing.Pool(processes=num_workers)
  results = []
  for i in range(num_workers):
    inputs = [
        'EXAMPLES=' + _get_staging_examples_folder_to_read(pipeline_args, i) +
        '/*'
    ]
    outputs = [
        'CALLED_VARIANTS=' + _get_staging_called_variants_folder(pipeline_args)
        + '/*'
    ]

    job_name = pipeline_args.job_name_prefix + _CALL_VARIANTS_JOB_NAME
    output_path = os.path.join(pipeline_args.logging, _CALL_VARIANTS_JOB_NAME,
                               str(i))
    run_args = _get_base_job_args(pipeline_args) + [
        '--name', job_name, '--vm-labels', 'dv-job-name=' + job_name,
        '--output', output_path, '--image',
        (pipeline_args.docker_image_gpu if pipeline_args.gpu else
         pipeline_args.docker_image), '--inputs', ','.join(inputs), '--outputs',
        ','.join(outputs), '--machine-type', machine_type, '--disk-size',
        str(pipeline_args.call_variants_disk_per_worker_gb), '--set', 'MODEL=' +
        pipeline_args.model, '--set', 'SHARDS=' + str(pipeline_args.shards),
        '--set', 'CALL_VARIANTS_SHARD_INDEX=' + str(i), '--set',
        'CALL_VARIANTS_SHARDS=' + str(num_workers), '--command', command
    ]
    if pipeline_args.gpu:
      run_args.extend(
          ['--gpu-type', pipeline_args.accelerator_type, '--gpus', '1'])
    results.append(threads.apply_async(_run_job, [run_args, output_path]))

  _wait_for_results(threads, results)


def _run_call_variants(pipeline_args):
  """Runs the call_variants job."""
  if pipeline_args.tpu:
    _run_call_variants_with_kubernetes(pipeline_args)
  else:
    _run_call_variants_with_pipelines_api(pipeline_args)


def _run_postprocess_variants(pipeline_args):
  """Runs the postprocess_variants job."""

  def get_extra_args():
    """Optional arguments that are specific to postprocess_variants binary."""
    extra_args = []
    if pipeline_args.gvcf_outfile:
      extra_args.extend([
          '--nonvariant_site_tfrecord_path',
          '"${GVCF}"/gvcf_output.tfrecord@"${SHARDS}".gz',
          '--gvcf_outfile',
          '"${GVCF_OUTFILE}"',
      ])
    return extra_args

  machine_type = 'custom-{0}-{1}'.format(
      pipeline_args.postprocess_variants_cores,
      pipeline_args.postprocess_variants_ram_gb * 1024)

  inputs = [
      'CALLED_VARIANTS=' + _get_staging_called_variants_folder(pipeline_args) +
      '/*',
      'INPUT_REF=' + pipeline_args.ref,
      'INPUT_REF_FAI=' + pipeline_args.ref_fai,
  ]
  outputs = ['OUTFILE=' + pipeline_args.outfile]

  if pipeline_args.ref_gzi:
    inputs.extend([pipeline_args.ref_gzi])

  if pipeline_args.gvcf_outfile:
    inputs.extend(['GVCF=' + _get_staging_gvcf_folder(pipeline_args) + '/*'])
    outputs.extend(['GVCF_OUTFILE=' + pipeline_args.gvcf_outfile])

  call_variants_shards = 1 if pipeline_args.tpu else min(
      pipeline_args.call_variants_workers, pipeline_args.shards)
  job_name = pipeline_args.job_name_prefix + _POSTPROCESS_VARIANTS_JOB_NAME
  output_path = os.path.join(pipeline_args.logging,
                             _POSTPROCESS_VARIANTS_JOB_NAME)
  run_args = _get_base_job_args(pipeline_args) + [
      '--name', job_name, '--vm-labels', 'dv-job-name=' + job_name, '--output',
      output_path, '--image', pipeline_args.docker_image, '--inputs',
      ','.join(inputs), '--outputs', ','.join(outputs), '--machine-type',
      machine_type, '--disk-size',
      str(pipeline_args.postprocess_variants_disk_gb), '--set',
      'SHARDS=' + str(pipeline_args.shards), '--set',
      'CALL_VARIANTS_SHARDS=' + str(call_variants_shards), '--command',
      _POSTPROCESS_VARIANTS_COMMAND.format(
          EXTRA_ARGS=' '.join(get_extra_args()))
  ]
  _run_job(run_args, output_path)


def _validate_and_complete_args(pipeline_args):
  """Validates pipeline arguments and fills some missing args (if any)."""
  # Basic validation logic. More detailed validation is done by pipelines API.
  if (pipeline_args.job_name_prefix and
      not _meets_gcp_label_restrictions(pipeline_args.job_name_prefix)):
    raise ValueError(
        '--job_name_prefix must meet GCP label restrictions: {}'.format(
            pipeline_args.job_name_prefix))
  if pipeline_args.preemptible and pipeline_args.max_preemptible_tries <= 0:
    raise ValueError('--max_preemptible_tries must be greater than zero.')
  if pipeline_args.max_non_preemptible_tries <= 0:
    raise ValueError('--max_non_preemptible_tries must be greater than zero.')
  if pipeline_args.make_examples_workers <= 0:
    raise ValueError('--make_examples_workers must be greater than zero.')
  if pipeline_args.call_variants_workers <= 0:
    raise ValueError('--call_variants_workers must be greater than zero.')
  if pipeline_args.shards <= 0:
    raise ValueError('--shards must be greater than zero.')
  if pipeline_args.shards % pipeline_args.make_examples_workers != 0:
    raise ValueError('--shards must be divisible by --make_examples_workers')
  if pipeline_args.shards % pipeline_args.call_variants_workers != 0:
    raise ValueError('--shards must be divisible by --call_variants_workers')
  if pipeline_args.call_variants_workers > pipeline_args.make_examples_workers:
    logging.warning(
        '--call_variants_workers cannot be greather than '
        '--make_examples_workers. Setting call_variants_workers to  %d',
        pipeline_args.make_examples_workers)
    pipeline_args.call_variants_workers = pipeline_args.make_examples_workers

  if pipeline_args.gpu and not pipeline_args.docker_image_gpu:
    raise ValueError('--docker_image_gpu must be provided with --gpu')
  if (pipeline_args.gvcf_gq_binsize is not None and
      not pipeline_args.gvcf_outfile):
    raise ValueError('--gvcf_outfile must be provided with --gvcf_gq_binsize')
  if (pipeline_args.gvcf_gq_binsize is not None and
      pipeline_args.gvcf_gq_binsize < 1):
    raise ValueError('--gvcf_gq_binsize must be greater or equal to 1')
  if pipeline_args.gpu and pipeline_args.tpu:
    raise ValueError('Both --gpu and --tpu cannot be set.')
  # TODO(nmousavi): Support multiple TPUs for call_variants if there is an
  # interest.
  if pipeline_args.tpu and pipeline_args.call_variants_workers != 1:
    raise ValueError(
        '--call_variants_workers must be equal to one when --tpu is set.')
  if pipeline_args.tpu and bool(pipeline_args.gke_cluster_region) == bool(
      pipeline_args.gke_cluster_zone):
    raise ValueError('Exactly one of --gke_cluster_region or '
                     '--gke_cluster_zone must be specified if --tpu is set.')

  # Verify the existing gke cluster is up and running.
  if pipeline_args.gke_cluster_name:
    try:
      _ = gke_cluster.GkeCluster(
          pipeline_args.gke_cluster_name,
          pipeline_args.gke_cluster_region,
          pipeline_args.gke_cluster_zone,
          create_if_not_exist=False)
    except ValueError:
      raise ValueError('Given --gke_cluster_name does not exist: %s' %
                       pipeline_args.gke_cluster_name)

  # Automatically generate default values for missing args (if any).
  if not pipeline_args.logging:
    pipeline_args.logging = os.path.join(pipeline_args.staging, 'logs')
  if not pipeline_args.ref_fai:
    pipeline_args.ref_fai = pipeline_args.ref + _FAI_FILE_SUFFIX
  if not pipeline_args.ref_gzi and pipeline_args.ref.endswith(_GZ_FILE_SUFFIX):
    pipeline_args.ref_gzi = pipeline_args.ref + _GZI_FILE_SUFFIX
  if not pipeline_args.bai:
    pipeline_args.bai = pipeline_args.bam + _BAI_FILE_SUFFIX
    if not _gcs_object_exist(pipeline_args.bai):
      pipeline_args.bai = pipeline_args.bam.replace(_BAM_FILE_SUFFIX,
                                                    _BAI_FILE_SUFFIX)

  # Ensuring all input files exist...
  if not _gcs_object_exist(pipeline_args.ref):
    raise ValueError('Given reference file via --ref does not exist')
  if not _gcs_object_exist(pipeline_args.ref_fai):
    raise ValueError('Given FAI index file via --ref_fai does not exist')
  if (pipeline_args.ref_gzi and not _gcs_object_exist(pipeline_args.ref_gzi)):
    raise ValueError('Given GZI index file via --ref_gzi does not exist')
  if not _gcs_object_exist(pipeline_args.bam):
    raise ValueError('Given BAM file via --bam does not exist')
  if not _gcs_object_exist(pipeline_args.bai):
    raise ValueError('Given BAM index file via --bai does not exist')
  # ...and we can write to output buckets.
  if not _can_write_to_bucket(_get_gcs_bucket(pipeline_args.staging)):
    raise ValueError('Cannot write to staging bucket, change --staging value')
  if not _can_write_to_bucket(_get_gcs_bucket(pipeline_args.outfile)):
    raise ValueError('Cannot write to output bucket, change --outfile value')


def run(argv=None):
  """Runs the DeepVariant pipeline."""
  parser = argparse.ArgumentParser()

  # Required args.
  parser.add_argument(
      '--project',
      required=True,
      help='Cloud project ID in which to run the pipeline.')
  parser.add_argument(
      '--docker_image', required=True, help='DeepVariant docker image.')
  parser.add_argument(
      '--zones',
      required=True,
      nargs='+',
      help=('List of Google Compute Engine zones. Wildcard suffixes are '
            'supported, such as "us-central1-*" or "us-*".'))
  parser.add_argument(
      '--outfile',
      required=True,
      help=('Destination path in Google Cloud Storage where the resulting '
            'VCF file will be stored.'))
  parser.add_argument(
      '--staging',
      required=True,
      help=('A folder in Google Cloud Storage to use for storing intermediate '
            'files from the pipeline.'))
  parser.add_argument(
      '--model',
      required=True,
      help=('A folder in Google Cloud Storage that stores the TensorFlow '
            'model to use to evaluate candidate variant calls. It expects '
            'the files to be prefixed with "model.ckpt".'))
  parser.add_argument(
      '--bam',
      required=True,
      help='Path in Google Cloud Storage that stores the BAM file.')
  parser.add_argument(
      '--ref',
      required=True,
      help='Path in Google Cloud Storage that stores the reference file.')

  # Additional input args. These are required for the pipeline run.
  # Reasonable defaults would be chosen if unspecified (the generated paths
  # must map to valid files).
  parser.add_argument(
      '--bai',
      help=('BAM index file. Defaults to --bam + "%s" suffix.' %
            _BAI_FILE_SUFFIX))
  parser.add_argument(
      '--ref_fai',
      help=('FAI index file. Defaults to --ref + "%s" suffix.' %
            _FAI_FILE_SUFFIX))
  parser.add_argument(
      '--ref_gzi',
      help=('GZI index file. Required if --ref is gz. Defaults to '
            '--ref + "%s" suffix.' % _GZI_FILE_SUFFIX))
  parser.add_argument(
      '--logging',
      help=('A folder in Google Cloud Storage to use for storing logs. '
            'Defaults to --staging + "/logs".'))

  # Optinal make_examples args.
  parser.add_argument(
      '--sample_name',
      help=('By default, make_examples extracts sample_name from input BAM '
            'file. However, for BAM file with missing sample_name, this has to '
            'be manually set.'))
  parser.add_argument(
      '--hts_block_size',
      help=('Sets the htslib block size (in bytes). Zero or negative uses '
            'default htslib setting. Currently only applies to SAM/BAM '
            'reading.'))
  parser.add_argument(
      '--gcsfuse',
      action='store_true',
      help=('Only affects make_example step. If set, gcsfuse is used to '
            'localize input bam file instead of copying it with gsutil. '))

  # Optional call_variants args.
  # TODO(b/118876068): Use call_variants default batch_size if not specified.
  parser.add_argument(
      '--call_variants_batch_size',
      type=int,
      default=512,
      help=('Number of candidate variant tensors to batch together during '
            'inference. Larger batches use more memory but are more '
            'computational efficient.'))

  # Optional gVCF args.
  parser.add_argument(
      '--gvcf_outfile',
      help=('Destination path in Google Cloud Storage where the resulting '
            'gVCF file will be stored. This is optional, and gVCF file will '
            'only be generated if this is specified.'))
  parser.add_argument(
      '--gvcf_gq_binsize',
      type=int,
      help=('Bin size in which make_examples job quantizes gVCF genotype '
            'qualities. Larger bin size reduces the number of gVCF records '
            'at a loss of quality granularity.'))

  # Additional optional pipeline parameters.
  parser.add_argument(
      '--regions',
      default=None,
      nargs='+',
      help=('Optional space-separated list of regions to process. Elements can '
            'be region literals (chr20:10-20) or Google Cloud Storage paths '
            'to BED/BEDPE files.'))
  parser.add_argument(
      '--max_non_preemptible_tries',
      type=int,
      default=2,
      help=('Maximum number of times to try running each worker (within a job) '
            'with regular (non-preemptible) VMs. Regular VMs may still crash '
            'unexpectedly, so it may be worth to retry on transient failures. '
            'Note that if max_preemptible_tries is also specified, then '
            'the pipeline would first be run with preemptible VMs, and then '
            'with regular VMs following the value provided here.'))
  parser.add_argument(
      '--network', help=('Optional. The VPC network on GCP to use.'))
  parser.add_argument(
      '--subnetwork', help=('Optional. The VPC subnetwork on GCP to use.'))
  parser.add_argument(
      '--logging_interval_sec',
      type=int,
      default=60,
      help=('Optional. If non-zero, specifies the time interval in seconds for '
            'writing workers log. Otherwise, log is written when the job is '
            'finished.'))

  # Optional GPU args.
  parser.add_argument(
      '--gpu',
      default=False,
      action='store_true',
      help='Use GPUs for the call_variants step.')
  parser.add_argument(
      '--docker_image_gpu',
      help='DeepVariant docker image for GPUs. Required if --gpu is set.')
  parser.add_argument(
      '--accelerator_type',
      default='nvidia-tesla-k80',
      help=('GPU type defined by Compute Engine. Please see '
            'https://cloud.google.com/compute/docs/gpus/ for supported GPU '
            'types.'))

  # Optional TPU args.
  parser.add_argument(
      '--tpu',
      default=False,
      action='store_true',
      help='Use TPU for the call_variants step.')
  parser.add_argument(
      '--gke_cluster_name',
      help=('GKE cluster to run call_variants step with TPU. If empty, a GKE '
            'cluster is created. This is relevant only if --tpu is set.'))
  parser.add_argument(
      '--gke_cluster_region',
      help=('GKE cluster region used for searching an existing cluster or '
            'creating a new one. This is relevant only if --tpu is set.'))
  parser.add_argument(
      '--gke_cluster_zone',
      help=('GKE cluster zone used for searching an existing cluster or '
            'creating a new one. This is relevant only if --tpu is set.'))

  # Optional preemptible args.
  parser.add_argument(
      '--preemptible',
      default=False,
      action='store_true',
      help=('Use preemptible VMs for the pipeline.'))
  parser.add_argument(
      '--max_preemptible_tries',
      type=int,
      default=3,
      help=('Maximum number of times to try running each worker (within a job) '
            'with preemptible VMs. Regular VMs will be used (for the '
            'particular shards assigned to that worker) after this many '
            'preemptions.'))

  # Optional pipeline sharding and machine shapes.
  parser.add_argument(
      '--shards',
      type=int,
      default=8,
      help=('Number of shards to use for the entire pipeline. The number of '
            'shards assigned to each worker is set by dividing --shards by '
            'the number of workers for each job.'))
  parser.add_argument(
      '--make_examples_workers',
      type=int,
      default=1,
      help=('Number of workers (machines) to use for running the make_examples '
            'job.'))
  parser.add_argument(
      '--make_examples_cores_per_worker',
      type=int,
      default=8,
      help='Number of cores for each worker in make_examples.')
  parser.add_argument(
      '--make_examples_ram_per_worker_gb',
      default=30,
      type=int,
      help='RAM (in GB) to use for each worker in make_examples.')
  parser.add_argument(
      '--make_examples_disk_per_worker_gb',
      type=int,
      default=50,
      help='Disk (in GB) to use for each worker in make_examples.')
  parser.add_argument(
      '--call_variants_workers',
      type=int,
      default=1,
      help=('Number of workers (machines) to use for running the call_variants '
            'job.'))
  parser.add_argument(
      '--call_variants_cores_per_worker',
      type=int,
      default=8,
      help='Number of cores for each worker in call_variants.')
  parser.add_argument(
      '--call_variants_ram_per_worker_gb',
      type=int,
      default=30,
      help='RAM (in GB) to use for each worker in call_variants.')
  parser.add_argument(
      '--call_variants_disk_per_worker_gb',
      type=int,
      default=30,
      help='Disk (in GB) to use for each worker in call_variants.')
  parser.add_argument(
      '--postprocess_variants_cores',
      type=int,
      default=8,
      help='Number of cores to use for postprocess_variants.')
  parser.add_argument(
      '--postprocess_variants_ram_gb',
      type=int,
      default=30,
      help='RAM (in GB) to use for postprocess_variants.')
  parser.add_argument(
      '--postprocess_variants_disk_gb',
      type=int,
      default=30,
      help='Disk (in GB) to use for postprocess_variants.')

  # Optional misc args.
  parser.add_argument(
      '--job_name_prefix',
      default='',
      help=('Prefix to add to the name of the jobs. Useful for distinguishing '
            'particular pipeline runs from others (e.g. in billing reports).'))
  parser.add_argument(
      '--operation_label',
      default='',
      help=(
          'Optional label to add to Pipelines API operations. Useful for '
          'finding all operations associated to a particular DeepVariant run.'))
  parser.add_argument(
      '--jobs_to_run',
      nargs='+',
      default=[
          _MAKE_EXAMPLES_JOB_NAME, _CALL_VARIANTS_JOB_NAME,
          _POSTPROCESS_VARIANTS_JOB_NAME
      ],
      choices=[
          _MAKE_EXAMPLES_JOB_NAME, _CALL_VARIANTS_JOB_NAME,
          _POSTPROCESS_VARIANTS_JOB_NAME
      ],
      help=('DeepVariant jobs to run. The DeepVariant pipeline consists of 3 '
            'jobs. By default, the pipeline runs all 3 jobs (make_examples, '
            'call_variants, postprocess_variants) in sequence. '
            'This option may be used to run parts of the pipeline.'))

  pipeline_args = parser.parse_args(argv)
  _validate_and_complete_args(pipeline_args)

  # TODO(b/112148076): Fail fast: validate GKE cluster early on in the pipeline.
  if _MAKE_EXAMPLES_JOB_NAME in pipeline_args.jobs_to_run:
    logging.info('Running make_examples...')
    _run_make_examples(pipeline_args)
    logging.info('make_examples is done!')
  if _CALL_VARIANTS_JOB_NAME in pipeline_args.jobs_to_run:
    logging.info('Running call_variants...')
    _run_call_variants(pipeline_args)
    logging.info('call_variants is done!')
  if _POSTPROCESS_VARIANTS_JOB_NAME in pipeline_args.jobs_to_run:
    logging.info('Running postprocess_variants...')
    _run_postprocess_variants(pipeline_args)
    logging.info('postprocess_variants is done!')


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.INFO,
      format='[%(asctime)s %(levelname)s %(filename)s] %(message)s',
      datefmt='%m/%d/%Y %H:%M:%S')
  run()
