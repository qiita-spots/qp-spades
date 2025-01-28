# -----------------------------------------------------------------------------
# Copyright (c) 2021--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import main
from os import remove, environ, mkdir
from os.path import exists, isdir, join, realpath, dirname
from shutil import rmtree, copyfile
from pathlib import Path
from tempfile import mkdtemp
from json import dumps
from qiita_client.testing import PluginTestCase

from qp_spades import plugin, plugin_details
from qp_spades.qp_spades import spades_to_array, spades


class SpadesTests(PluginTestCase):
    def setUp(self):
        # this will allow us to see the full errors
        self.maxDiff = None

        plugin("https://localhost:8383", 'register', 'ignored')
        self._clean_up_files = []

        self.data = {
            'user': 'demo@microbio.me',
            'command': None,
            'status': 'running',
            'parameters': None}

        self.basedir = dirname(realpath(__file__))

        self.environment = environ["ENVIRONMENT"]

    def tearDown(self):
        for fp in self._clean_up_files:
            if exists(fp):
                if isdir(fp):
                    rmtree(fp)
                else:
                    remove(fp)

    def _generate_testing_files(self):
        prep_info_dict = {
            'SKB8.640193': {'run_prefix': 'S22205_S104'},
            'SKD8.640184': {'run_prefix': 'S22282_S102'}}
        data = {'prep_info': dumps(prep_info_dict),
                # magic #1 = testing study
                'study': 1,
                'data_type': 'Metagenomic'}
        self.pid = self.qclient.post(
            '/apitest/prep_template/', data=data)['prep']

        # inserting artifacts
        in_dir = mkdtemp()
        self._clean_up_files.append(in_dir)

        fp1_1 = join(in_dir, 'S22205_S104_L001_R1_001.fastq.gz')
        fp1_2 = join(in_dir, 'S22205_S104_L001_R2_001.fastq.gz')
        fp2_1 = join(in_dir, 'S22282_S102_L001_R1_001.fastq.gz')
        fp2_2 = join(in_dir, 'S22282_S102_L001_R2_001.fastq.gz')
        source_dir = 'support_files/raw_data'
        copyfile(f'{source_dir}/S22205_S104_L001_R1_001.fastq.gz', fp1_1)
        copyfile(f'{source_dir}/S22205_S104_L001_R2_001.fastq.gz', fp1_2)
        copyfile(f'{source_dir}/S22282_S102_L001_R1_001.fastq.gz', fp2_1)
        copyfile(f'{source_dir}/S22282_S102_L001_R2_001.fastq.gz', fp2_2)

        data = {
            'filepaths': dumps([
                (fp1_1, 'raw_forward_seqs'),
                (fp1_2, 'raw_reverse_seqs'),
                (fp2_1, 'raw_forward_seqs'),
                (fp2_2, 'raw_reverse_seqs')]),
            'type': "per_sample_FASTQ",
            'name': "Test Artifact",
            'prep': self.pid}
        aid = self.qclient.post('/apitest/artifact/', data=data)['artifact']

        self.aid = aid

    def test_spades_to_array(self):
        self._generate_testing_files()

        # testing isolate/no-merge
        params = {
            'type': 'isolate', 'merging': 'no merge', 'input': self.aid,
            'threads': 5}
        out_dir = mkdtemp()
        self._clean_up_files.append(out_dir)
        files, prep = self.qclient.artifact_and_preparation_files(self.aid)
        directory = {dirname(ffs['filepath'])
                     for fs in files.values() for ffs in fs}
        directory = directory.pop()
        prefix_to_name = prep.set_index('run_prefix')['sample_name'].to_dict()

        main_fp, finish_fp = spades_to_array(
            directory, out_dir, prefix_to_name, 'http://mylink',
            'qiita_job_id', params)
        with open(main_fp) as fp:
            obs_main_fp = fp.readlines()
        with open(finish_fp) as fp:
            obs_finish_fp = fp.readlines()
        file_list_fp = join(dirname(finish_fp), 'files_to_process.txt')
        with open(file_list_fp) as fp:
            obs_file_list_fp = fp.readlines()
        params['out_dir'] = out_dir
        params['environment'] = self.environment
        self.assertEqual(''.join(obs_main_fp), EXP_MAIN.format(**params))
        self.assertEqual(
            ''.join(obs_finish_fp), EXP_FINISH.format(**params))
        self.assertEqual(''.join(obs_file_list_fp),
                         EXP_FILE_LIST.format(directory=directory))

        # testing isolate/merge
        # note that we don't need to recreate all the above variables and we
        # can reuse
        params = {
            'type': 'meta', 'merging': 'flash 65%', 'input': self.aid,
            'threads': 5}
        prefix_to_name = prep.set_index('run_prefix')['sample_name'].to_dict()

        main_fp, finish_fp = spades_to_array(
            directory, out_dir, prefix_to_name, 'http://mylink',
            'qiita_job_id', params)
        with open(main_fp) as fp:
            obs_main_fp = fp.readlines()
        with open(finish_fp) as fp:
            obs_finish_fp = fp.readlines()
        params['out_dir'] = out_dir
        params['environment'] = self.environment
        self.assertEqual(
            ''.join(obs_main_fp), EXP_MAIN_FLASH.format(**params))
        self.assertEqual(
            ''.join(obs_finish_fp), EXP_FINISH.format(**params))

        # testing errors
        prefix_to_name = {'S222': '1.SKB8.640193', 'S22': '1.SKD8.640184'}
        with self.assertRaisesRegex(
                ValueError, 'Expected two files to match "S222"'):
            spades_to_array(
                directory, out_dir, prefix_to_name, 'http://mylink',
                'qiita_job_id', params)

        prefix_to_name = list(range(1300))
        with self.assertRaisesRegex(
                ValueError, 'This preparation has more than 1024 samples'):
            spades_to_array(
                directory, out_dir, prefix_to_name, 'http://mylink',
                'qiita_job_id', params)

    def test_spades(self):
        self._generate_testing_files()

        # testing error
        params = {
            'type': 'meta', 'merging': 'flash 65%', 'input': self.aid,
            'threads': 5}
        self.data['command'] = dumps(
            [plugin_details['name'], plugin_details['version'], 'spades'])
        self.data['parameters'] = dumps(params)
        jid = self.qclient.post(
            '/apitest/processing_job/', data=self.data)['job']

        out_dir = mkdtemp()
        self._clean_up_files.append(out_dir)
        success, artifact, msg = spades(self.qclient, jid, params, out_dir)
        self.assertIsNone(artifact)
        self.assertFalse(success)
        self.assertEqual(msg, f'There was no scaffolds.fasta for samples: '
                         '1.SKB8.640193 [S22205_S104], 1.SKD8.640184 '
                         '[S22282_S102]. Contact: qiita.help@gmail.com and '
                         f'add this job id: {jid}')

        # testing success
        mkdir(f'{out_dir}/S22205_S104')
        mkdir(f'{out_dir}/S22282_S102')
        Path(f'{out_dir}/S22205_S104/scaffolds.fasta').touch()
        Path(f'{out_dir}/S22282_S102/scaffolds.fasta').touch()
        success, ainfo, msg = spades(self.qclient, jid, params, out_dir)
        self.assertTrue(success)
        self.assertEqual(1, len(ainfo))
        self.assertEqual(ainfo[0].files,
                         [(f'{out_dir}/S22205_S104/S22205_S104.fasta',
                           'preprocessed_fasta'),
                          (f'{out_dir}/S22282_S102/S22282_S102.fasta',
                           'preprocessed_fasta')])


EXP_MAIN = """#!/bin/bash
#SBATCH -p qiita
#SBATCH --mail-user qiita.help@gmail.com
#SBATCH --job-name qiita_job_id
#SBATCH -N 1
#SBATCH -n 5
#SBATCH --time 200:00:00
#SBATCH --mem 128g
#SBATCH --output {out_dir}/qiita_job_id_%a.log
#SBATCH --error {out_dir}/qiita_job_id_%a.err
#SBATCH --array 1-2%8
cd {out_dir}
{environment}
OUTDIR={out_dir}/
date
hostname
echo ${{SLURM_JOBID}} ${{SLURM_ARRAY_TASK_ID}}
offset=${{SLURM_ARRAY_TASK_ID}}
args=$(head -n $offset ${{OUTDIR}}/files_to_process.txt| tail -n 1)
FWD=$(echo -e $args | awk '{{ print $1 }}')
REV=$(echo -e $args | awk '{{ print $2 }}')
SNAME=$(echo -e $args | awk '{{ print $3 }}')
spades.py --{type} -m 128 -t {ppn} -o $OUTDIR/$SNAME \
--gemcode1-1 ${{FWD}} --gemcode1-2 ${{REV}}
date
"""

EXP_FINISH = """#!/bin/bash
#SBATCH -p qiita
#SBATCH --mail-user qiita.help@gmail.com
#SBATCH --job-name finish-qiita_job_id
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --time 10:00:00
#SBATCH --mem 4g
#SBATCH --output {out_dir}/finish-qiita_job_id.log
#SBATCH --error {out_dir}/finish-qiita_job_id.err
cd {out_dir}
{environment}
date
hostname
echo $SLURM_JOBID
finish_qp_spades http://mylink qiita_job_id {out_dir}
date
"""

EXP_FILE_LIST = """{directory}/S22205_S104_L001_R1_001.fastq.gz\t\
{directory}/S22205_S104_L001_R2_001.fastq.gz\tS22205_S104
{directory}/S22282_S102_L001_R1_001.fastq.gz\t\
{directory}/S22282_S102_L001_R2_001.fastq.gz\tS22282_S102"""


EXP_MAIN_FLASH = """#!/bin/bash
#SBATCH -p qiita
#SBATCH --mail-user qiita.help@gmail.com
#SBATCH --job-name qiita_job_id
#SBATCH -N 1
#SBATCH -n 5
#SBATCH --time 200:00:00
#SBATCH --mem 128g
#SBATCH --output {out_dir}/qiita_job_id_%a.log
#SBATCH --error {out_dir}/qiita_job_id_%a.err
#SBATCH --array 1-2%8
cd {out_dir}
{environment}
OUTDIR={out_dir}/
date
hostname
echo ${{SLURM_JOBID}} ${{SLURM_ARRAY_TASK_ID}}
offset=${{SLURM_ARRAY_TASK_ID}}
args=$(head -n $offset ${{OUTDIR}}/files_to_process.txt| tail -n 1)
FWD=$(echo -e $args | awk '{{ print $1 }}')
REV=$(echo -e $args | awk '{{ print $2 }}')
SNAME=$(echo -e $args | awk '{{ print $3 }}')
flash --threads {threads} --max-overlap=97 --output-directory $OUTDIR \
--output-prefix="$SNAME" ${{FWD}} ${{REV}} --max-mismatch-density=0.1 \
> $OUTDIR/${{SNAME}}.flash.log 2>&1 && spades.py --{type} -m 128 -t \
{ppn} -o $OUTDIR/$SNAME --merge $OUTDIR/${{SNAME}}.extendedFrags.fastq \
--gemcode1-1 ${{FWD}} --gemcode1-2 ${{REV}}
date
"""

if __name__ == '__main__':
    main()
