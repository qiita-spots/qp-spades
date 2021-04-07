# -----------------------------------------------------------------------------
# Copyright (c) 2021-, Qiita development team.
#
# Distributed under the terms of the BSD 3-clause License License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------
import pandas as pd

from os.path import basename

from qiita_client import ArtifactInfo
from qiita_client.util import system_call


def spades(qclient, job_id, parameters, out_dir):
    """Run spades with the given parameters

    Parameters
    ----------
    qclient : tgp.qiita_client.QiitaClient
        The Qiita server client
    job_id : str
        The job id
    parameters : dict
        The parameter values to run split libraries
    out_dir : str
        The path to the job's output directory

    Returns
    -------
    bool, list, str
        The results of the job
    """
    # Step 1 get the rest of the information need to run Bowtie2
    qclient.update_job_step(job_id, "Step 1 of 4: Collecting information")
    artifact_id = parameters['input']
    del parameters['input']

    # Get the artifact filepath information
    artifact_info = qclient.get("/qiita_db/artifacts/%s/" % artifact_id)
    fwd_seqs = sorted(artifact_info['files']['raw_forward_seqs'])
    if 'raw_reverse_seqs' in artifact_info['files']:
        rev_seqs = sorted(artifact_info['files']['raw_reverse_seqs'])
    else:
        return False, None, 'This plugin expects forward and reverse reads.'

    if len(fwd_seqs) != len(rev_seqs):
        mgs = 'There is a different number of forward and reverse read files.'
        return False, None, mgs

    # Get the artifact metadata
    prep_info = qclient.get('/qiita_db/prep_template/%s/'
                            % artifact_info['prep_information'][0])
    df = pd.read_csv(prep_info['prep-file'], sep='\t', dtype='str',
                     na_values=[], keep_default_na=True)
    if 'run_prefix' not in df.columns:
        return False, None, 'Missing run_prefix column in your preparation'
    prefix_to_name = df.set_index('run_prefix')['sample_name'].to_dict()

    # Step 2 generating command
    qclient.update_job_step(job_id, "Step 2 of 4: Generating commands")

    flash_cmd = (f'flash --threads {parameters["threads"]} --max-overlap=%d '
                 f'--max-mismatch-density=0.1 --output-directory {out_dir} '
                 '--output-prefix="%s" %s %s')
    spades_cmd = (
        f'spades.py --{parameters["type"]} -t {parameters["threads"]} '
        f'-m {parameters["memory"]} -k {parameters["k-mers"]} -1 %s -2 %s '
        f'-o {out_dir}/%s')

    commands = []
    outfiles = []
    overlap = None

    for fwd_fp, rev_fp in zip(fwd_seqs, rev_seqs):
        fwd_fn = basename(fwd_fp)

        run_prefix = None
        for rp in prefix_to_name:
            if fwd_fn.startswith(rp):
                if run_prefix is None:
                    run_prefix = rp
                else:
                    msg = ('Multiple run prefixes match this fwd '
                           'file: {fwd_fn}')
                    return False, None, msg

        rev_fn = basename(rev_fp)
        # if we have reverse reads, make sure the matching pair also
        # matches the run prefix:
        if not rev_fn.startswith(run_prefix):
            msg = ('Reverse read does not match this run prefix.\nRun prefix: '
                   '%s\nForward read: %s\nReverse read: %s\n' % (
                    run_prefix, fwd_fn, rev_fn))
            return False, None, msg

        sample_name = prefix_to_name[run_prefix]
        out_fp = f'{out_dir}/{sample_name}.fasta'
        if parameters['merging'] == 'no merge':
            cmd = (f'{spades_cmd} > {out_dir}/%s.spades.log 2>&1; '
                   f'mv {out_dir}/{sample_name}/scaffolds.fasta {out_fp}' % (
                    fwd_fp, rev_fp, sample_name, sample_name))
        elif parameters['merging'].startswith('flash '):
            if overlap is None:
                # get read lenght quickly; note that we are going to assume
                # that (1) the forward and reverse are the same lenght and (2)
                # all file pairs have the same lenght so only calculate once
                std_out, std_err, return_value = system_call(
                    f'gunzip -c {fwd_fp} | head -n 2')
                if return_value != 0:
                    error_msg = (f"Error uncompressing: {fwd_fp}\n"
                                 f"Std out: {std_out}\nStd err: {std_err}\n")
                    return False, None, error_msg

                read_length = len(std_out.split('\n')[1])
                percentage = int(parameters['merging'][6:-1])/100
                overlap = int(read_length * percentage)

            flash_fwd = f'{out_dir}/{sample_name}.notCombined_1.fastq'
            flash_rev = f'{out_dir}/{sample_name}.notCombined_2.fastq'
            flash_merge = f'{out_dir}/{sample_name}.extendedFrags.fastq'
            cmd = (f'{flash_cmd} > {out_dir}/%s.flash.log 2>&1; '
                   f'{spades_cmd} --merge %s > '
                   f'{out_dir}/%s.spades.log 2>&1; '
                   f'mv {out_dir}/{sample_name}/scaffolds.fasta {out_fp}' % (
                    overlap, sample_name, fwd_fp, rev_fp, sample_name,
                    flash_fwd, flash_rev, sample_name, flash_merge,
                    sample_name))
        else:
            return False, None, 'Not a valid merging scheme'

        commands.append(cmd)
        outfiles.append((out_fp, 'preprocessed_fasta'))

    len_commands = len(commands)
    msg = f"Step 3 of 4: Executing commands (%d/{len_commands})"
    for i, cmd in enumerate(commands):
        qclient.update_job_step(job_id, msg % (i+1))
        std_out, std_err, return_value = system_call(cmd)
        if return_value != 0:
            error_msg = ("Error running spades; for more information send an "
                         "email to qiita.help@gmail.com and add this job_id: "
                         f"{job_id} \n")
            return False, None, error_msg

    # Step 4 generating artifacts
    msg = "Step 4 of 4: Generating new artifact"
    qclient.update_job_step(job_id, msg)
    ainfo = [ArtifactInfo(
        'Preprocessed FASTA', 'FASTA_preprocessed', outfiles)]

    return True, ainfo, ""
