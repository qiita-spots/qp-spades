# -----------------------------------------------------------------------------
# Copyright (c) 2021, Qiita development team.
#
# Distributed under the terms of the BSD 3-clause License License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from qiita_client import QiitaPlugin, QiitaCommand
from .qp_spades import spades


THREADS = "12"

plugin_details = {'name': 'qp-spades',
                  'version': '2025.02',
                  'description': 'spades assembler pipeline'}


# Initialize the plugin
plugin = QiitaPlugin(**plugin_details)

# Define the command
req_params = {'input': ('artifact', ['per_sample_FASTQ'])}
opt_params = {
    'type': ['choice:["meta"]', "meta"],
    'merging': ['choice:["no merge", "flash 65%"]', "no merge"],
    'threads': ['integer', THREADS],
    }

outputs = {'Preprocessed FASTA': 'FASTA_preprocessed'}
default_params = {
    'Tell-Seq meta + no merge': {
        'type': 'meta', 'merging': 'no merge', 'threads': THREADS}
}

spades_cmd = QiitaCommand(
    "spades", "Assembly via spades", spades,
    req_params, opt_params, outputs, default_params)

plugin.register_command(spades_cmd)
