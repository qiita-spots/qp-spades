# -----------------------------------------------------------------------------
# Copyright (c) 2021--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import main
from os import remove
from shutil import rmtree, copyfile
from tempfile import mkdtemp
from json import dumps
from os.path import exists, isdir, join, realpath, dirname

from qiita_client.testing import PluginTestCase

from qp_spades import plugin, spades


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

    def test_spades(self):
        self._generate_testing_files()

        # testing isolate/merge
        params = {
            'type': 'isolate', 'merging': 'no merge', 'input': self.aid,
            'threads': 5, 'memory': 200, 'k-mers': '21,33,55,77,99,127'}
        data = {'user': 'demo@microbio.me',
                'command': dumps(['qp-spades', '2021.05', 'spades v3.15.2']),
                'status': 'running',
                'parameters': dumps(params)}
        jid = self.qclient.post('/apitest/processing_job/', data=data)['job']
        out_dir = mkdtemp()
        self._clean_up_files.append(out_dir)

        success, ainfo, msg = spades(self.qclient, jid, params, out_dir)
        self.assertEqual(msg[:20], 'Error running spades')
        self.assertFalse(success)

        # testing meta/no-merge
        params = {
            'type': 'meta', 'merging': 'flash 65%', 'input': self.aid,
            'threads': 5, 'memory': 200, 'k-mers': '21,33,55,77,99,127'}
        data = {'user': 'demo@microbio.me',
                'command': dumps(['qp-spades', '2021.05', 'spades v3.15.2']),
                'status': 'running',
                'parameters': dumps(params)}
        jid = self.qclient.post('/apitest/processing_job/', data=data)['job']
        out_dir = mkdtemp()
        self._clean_up_files.append(out_dir)

        success, ainfo, msg = spades(self.qclient, jid, params, out_dir)
        self.assertEqual(msg, '')
        self.assertTrue(success)
        self.assertEqual(ainfo[0].artifact_type, 'FASTA_preprocessed')
        self.assertEqual(ainfo[0].output_name, 'Preprocessed FASTA')
        self.assertEqual(ainfo[0].files, [
            (join(out_dir, '1.SKB8.640193.fasta'), 'preprocessed_fasta'),
            (join(out_dir, '1.SKD8.640184.fasta'), 'preprocessed_fasta')])


if __name__ == '__main__':
    main()
