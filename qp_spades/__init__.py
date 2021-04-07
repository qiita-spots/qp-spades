# -----------------------------------------------------------------------------
# Copyright (c) 2021, Qiita development team.
#
# Distributed under the terms of the BSD 3-clause License License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from qiita_client import QiitaPlugin, QiitaCommand
from .qp_spades import spades


THREADS = "16"
MEMORY = "200"
KMERS = "21,33,55,77,99,127"

# Initialize the plugin
plugin = QiitaPlugin('qp-spades', '2021.05', 'spades pipeline')

# Define the command
req_params = {'input': ('artifact', ['per_sample_FASTQ'])}
opt_params = {
    'type': ['choice:["meta", "isolate"]', "meta"],
    'merging': ['choice:["no merge", "flash 65%"]', "flash 65%"],
    'threads': ['integer', THREADS],
    'memory': ['integer', MEMORY],
    'k-mers': ['string', KMERS],
    }

outputs = {'Preprocessed FASTA': 'FASTA_preprocessed'}
default_params = {
    'no merging + meta': {
        'type': 'meta', 'merging': 'no merge', 'threads': THREADS,
        'memory': MEMORY, 'k-mers': KMERS},
    'no merging + isolate': {
        'type': 'isolate', 'merging': 'no merge', 'threads': THREADS,
        'memory': MEMORY, 'k-mers': KMERS},
    'merge flash 65% + meta': {
        'type': 'meta', 'merging': 'flash 65%', 'threads': THREADS,
        'memory': MEMORY, 'k-mers': KMERS},
    'merge flash 65% + isolate': {
        'type': 'isolate', 'merging': 'flash 65%', 'threads': THREADS,
        'memory': MEMORY, 'k-mers': KMERS}}

spades_cmd = QiitaCommand(
    "spades v3.15.2", "Isolate and Metagenomic processing via spades", spades,
    req_params, opt_params, outputs, default_params)

plugin.register_command(spades_cmd)
