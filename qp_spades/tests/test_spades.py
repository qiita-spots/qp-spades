# -----------------------------------------------------------------------------
# Copyright (c) 2021--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import main
from os import remove
from os.path import exists, isdir, join, realpath, dirname
from shutil import rmtree, copyfile
from tempfile import mkdtemp
from json import dumps
import pandas as pd
from qiita_client.testing import PluginTestCase

from qp_spades import plugin
from qp_spades.qp_spades import spades_to_array


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
            'threads': 5, 'memory': 200, 'k-mers': '21,33,55,77,99,127'}
        out_dir = mkdtemp()
        self._clean_up_files.append(out_dir)
        artifact_info = self.qclient.get(f"/qiita_db/artifacts/{self.aid}/")
        directory = {dirname(ffs) for _, fs in artifact_info['files'].items()
                     for ffs in fs}
        directory = directory.pop()
        prep_info = self.qclient.get(f'/qiita_db/prep_template/{self.pid}/')
        df = pd.read_csv(prep_info['prep-file'], sep='\t', dtype='str',
                         na_values=[], keep_default_na=True)
        if 'run_prefix' not in df.columns:
            return False, None, 'Missing run_prefix column in your preparation'
        prefix_to_name = df.set_index('run_prefix')['sample_name'].to_dict()

        main_qsub_fp, finish_qsub_fp = spades_to_array(
            directory, out_dir, prefix_to_name, 'http://mylink',
            'qiita_job_id', params)
        with open(main_qsub_fp) as fp:
            obs_main_qsub_fp = fp.readlines()
        with open(finish_qsub_fp) as fp:
            obs_finish_qsub_fp = fp.readlines()
        params['out_dir'] = out_dir
        self.assertEqual(''.join(obs_main_qsub_fp), EXP_MAIN.format(**params))
        self.assertEqual(
            ''.join(obs_finish_qsub_fp), EXP_FINISH.format(**params))

        # testing isolate/merge
        # note that we don't need to recreate all the above variables and we
        # can reuse
        params = {
            'type': 'meta', 'merging': 'flash 65%', 'input': self.aid,
            'threads': 5, 'memory': 200, 'k-mers': '21,33,55,77,99,127'}
        prefix_to_name = df.set_index('run_prefix')['sample_name'].to_dict()

        main_qsub_fp, finish_qsub_fp = spades_to_array(
            directory, out_dir, prefix_to_name, 'http://mylink',
            'qiita_job_id', params)
        with open(main_qsub_fp) as fp:
            obs_main_qsub_fp = fp.readlines()
        with open(finish_qsub_fp) as fp:
            obs_finish_qsub_fp = fp.readlines()
        params['out_dir'] = out_dir
        self.assertEqual(
            ''.join(obs_main_qsub_fp), EXP_MAIN_FLASH.format(**params))
        self.assertEqual(
            ''.join(obs_finish_qsub_fp), EXP_FINISH.format(**params))


EXP_MAIN = """#!/bin/bash
#PBS -M qiita.help@gmail.com
#PBS -N qiita_job_id
#PBS -l nodes=1:ppn=5
#PBS -l walltime=200:00:00
#PBS -l mem=200g
#PBS -o {out_dir}/qiita_job_id_${{PBS_ARRAYID}}.log
#PBS -e {out_dir}/qiita_job_id_${{PBS_ARRAYID}}.err
#PBS -t 1-2%8
#PBS -l epilogue=/home/qiita/qiita-epilogue.sh
cd {out_dir}
source ~/.bash_profile; source activate qp-spades
OUTDIR={out_dir}/
date
hostname
echo ${{PBS_JOBID}} ${{PBS_ARRAYID}}
offset=${{PBS_ARRAYID}}
args=$(head -n $offset ${{OUTDIR}}/files_to_process.txt| tail -n 1)
FWD=$(echo -e $args | awk '{{ print $1 }}')
REV=$(echo -e $args | awk '{{ print $2 }}')
SNAME=$(echo -e $args | awk '{{ print $3 }}')
spades.py --{type} -t {threads} -m {memory} -k {k-mers} -o $OUTDIR/$SNAME \
-1 ${{FWD}} -2 ${{REV}}
date
"""

EXP_FINISH = """#!/bin/bash
#PBS -M qiita.help@gmail.com
#PBS -N merge-qiita_job_id
#PBS -l nodes=1:ppn=1
#PBS -l walltime=10:00:00
#PBS -l mem=4g
#PBS -o {out_dir}/finish-qiita_job_id.log
#PBS -e {out_dir}/finish-qiita_job_id.err
#PBS -l epilogue=/home/qiita/qiita-epilogue.sh
cd {out_dir}
source ~/.bash_profile; source activate qp-spades
date
hostname
echo $PBS_JOBID
finish_woltka http://mylink qiita_job_id {out_dir}
date
"""

EXP_MAIN_FLASH = """#!/bin/bash
#PBS -M qiita.help@gmail.com
#PBS -N qiita_job_id
#PBS -l nodes=1:ppn=5
#PBS -l walltime=200:00:00
#PBS -l mem=200g
#PBS -o {out_dir}/qiita_job_id_${{PBS_ARRAYID}}.log
#PBS -e {out_dir}/qiita_job_id_${{PBS_ARRAYID}}.err
#PBS -t 1-2%8
#PBS -l epilogue=/home/qiita/qiita-epilogue.sh
cd {out_dir}
source ~/.bash_profile; source activate qp-spades
OUTDIR={out_dir}/
date
hostname
echo ${{PBS_JOBID}} ${{PBS_ARRAYID}}
offset=${{PBS_ARRAYID}}
args=$(head -n $offset ${{OUTDIR}}/files_to_process.txt| tail -n 1)
FWD=$(echo -e $args | awk '{{ print $1 }}')
REV=$(echo -e $args | awk '{{ print $2 }}')
SNAME=$(echo -e $args | awk '{{ print $3 }}')
flash --threads {threads} --max-overlap=97 --output-directory $OUTDIR \
--output-prefix="$SNAME" ${{FWD}} ${{REV}} --max-mismatch-density=0.1 \
> $OUTDIR/${{SNAME}}.flash.log 2>&1 && spades.py --{type} -t {threads} \
-m {memory} -k {k-mers} -o $OUTDIR/$SNAME \
--merge $OUTDIR/${{SNAME}}.extendedFrags.fastq \
-1 $OUTDIR/${{SNAME}}.notCombined_1.fastq \
-2 $OUTDIR/${{SNAME}}.notCombined_2.fastq
date
"""

if __name__ == '__main__':
    main()
