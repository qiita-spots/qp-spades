# -----------------------------------------------------------------------------
# Copyright (c) 2021-, Qiita development team.
#
# Distributed under the terms of the BSD 3-clause License License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------
import pandas as pd
from os import environ
from os.path import join, exists
from glob import glob
from subprocess import run, PIPE

from qiita_client import ArtifactInfo
from qiita_client.util import system_call

# resources per job
WALLTIME = '200:00:00'
MAX_RUNNING = 8
FINISH_WALLTIME = '10:00:00'
FINISH_MEMORY = '4g'


def spades_to_array(directory, output_dir, prefix_to_name, url,
                    job_id, params):
    environment = environ["ENVIRONMENT"]
    ppn = params["threads"]
    memory = params["memory"]

    # 1. create file list
    num_samples = len(prefix_to_name)
    if num_samples > 1024:
        raise ValueError('This preparation has more than 1024 samples, '
                         'which is the limit; please split in multiple.')

    files = []
    for prefix, sample_name in prefix_to_name.items():
        fps = sorted(glob(join(directory, prefix + '*')))
        # this should never occur but better to confirm
        if len(fps) != 2:
            error_msg = f'Expected two files to match "{prefix}"'
            raise ValueError(error_msg)
        files.append('\t'.join([fps[0], fps[1], prefix]))

    # 2. format main comand
    command = (
        f'spades.py --{params["type"]} -t {ppn} -m {memory} '
        f'-k {params["k-mers"]} -o $OUTDIR/$SNAME')
    if params['merging'].startswith('flash '):
        # get read length quickly; note that we are going to assume
        # that (1) the forward and reverse are the same length and (2)
        # all file pairs have the same length so only calculate once
        fp = glob(join(directory, list(prefix_to_name)[0] + '*'))[0]
        std_out, std_err, return_value = system_call(
                f'zcat -c {fp} | head -n 2')
        if return_value != 0:
            error_msg = (f"Error uncompressing: {fp}\n"
                         f"Std out: {std_out}\nStd err: {std_err}\n")
            raise ValueError(error_msg)
        read_length = len(std_out.split('\n')[1])
        percentage = int(params['merging'][6:-1])/100
        overlap = int(read_length * percentage)

        command = (
            # flash
            f'flash --threads {ppn} --max-overlap={overlap} '
            '--output-directory $OUTDIR '
            '--output-prefix="$SNAME" ${FWD} ${REV} '
            '--max-mismatch-density=0.1 > $OUTDIR/${SNAME}.flash.log 2>&1'
            ' && '
            # spades
            f'{command} '
            '--merge $OUTDIR/${SNAME}.extendedFrags.fastq '
            '-1 $OUTDIR/${SNAME}.notCombined_1.fastq '
            '-2 $OUTDIR/${SNAME}.notCombined_2.fastq')
    else:
        command = '%s -1 ${FWD} -2 ${REV}' % command

    # 3. create qsub for array submission
    mqsub = [
        '#!/bin/bash',
        '#PBS -M qiita.help@gmail.com',
        f'#PBS -N {job_id}',
        f'#PBS -l nodes=1:ppn={ppn}',
        f'#PBS -l walltime={WALLTIME}',
        f'#PBS -l mem={memory}g',
        f'#PBS -o {output_dir}/{job_id}' + '_${PBS_ARRAYID}.log',
        f'#PBS -e {output_dir}/{job_id}' + '_${PBS_ARRAYID}.err',
        f'#PBS -t 1-{num_samples}%{MAX_RUNNING}',
        '#PBS -l epilogue=/home/qiita/qiita-epilogue.sh',
        f'cd {output_dir}',
        f'{environment}',
        f'OUTDIR={output_dir}/',
        'date',
        'hostname',
        'echo ${PBS_JOBID} ${PBS_ARRAYID}',
        'offset=${PBS_ARRAYID}',
        'args=$(head -n $offset ${OUTDIR}/files_to_process.txt| tail -n 1)',
        "FWD=$(echo -e $args | awk '{ print $1 }')",
        "REV=$(echo -e $args | awk '{ print $2 }')",
        "SNAME=$(echo -e $args | awk '{ print $3 }')",
        f'{command}',
        'date']

    # 4. create qsub to finish job in Qiita
    fqsub = [
        '#!/bin/bash',
        '#PBS -M qiita.help@gmail.com',
        f'#PBS -N merge-{job_id}',
        '#PBS -l nodes=1:ppn=1',
        f'#PBS -l walltime={FINISH_WALLTIME}',
        f'#PBS -l mem={FINISH_MEMORY}',
        f'#PBS -o {output_dir}/finish-{job_id}.log',
        f'#PBS -e {output_dir}/finish-{job_id}.err',
        '#PBS -l epilogue=/home/qiita/qiita-epilogue.sh',
        f'cd {output_dir}',
        f'{environment}',
        'date',
        'hostname',
        'echo $PBS_JOBID',
        f'finish_qp_spades {url} {job_id} {output_dir}\n'
        "date"]

    # write files
    with open(join(output_dir, 'files_to_process.txt'), 'w') as f:
        f.write('\n'.join(files))
    main_qsub_fp = join(output_dir, f'{job_id}.qsub')
    with open(main_qsub_fp, 'w') as job:
        job.write('\n'.join(mqsub))
        job.write('\n')
    finish_qsub_fp = join(output_dir, f'{job_id}.finish.qsub')
    with open(finish_qsub_fp, 'w') as job:
        job.write('\n'.join(fqsub))
        job.write('\n')

    return main_qsub_fp, finish_qsub_fp


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
    msg = "Step 3 of 4: Checking resulting files"
    qclient.update_job_step(job_id, msg)

    artifact_id = parameters['input']
    artifact_info = qclient.get("/qiita_db/artifacts/%s/" % artifact_id)
    # Get the artifact metadata
    prep_info = qclient.get('/qiita_db/prep_template/%s/'
                            % artifact_info['prep_information'][0])
    df = pd.read_csv(prep_info['prep-file'], sep='\t', dtype='str',
                     na_values=[], keep_default_na=True)
    snames = df.sample_name.values

    missing = []
    outfiles = []
    for sname in snames:
        scaffold = join(out_dir, sname, 'scaffolds.fasta')
        if exists(scaffold):
            new_scaffold = join(out_dir, sname, f'{sname}.fasta')
            run(['mv', scaffold, new_scaffold], stdout=PIPE)
            outfiles.append((new_scaffold, 'preprocessed_fasta'))
        else:
            missing.append(sname)

    if missing:
        error_msg = (
            'There was no scaffolds.fasta for samples: %s. Contact: '
            'qiita.help@gmail.com and add this job id: %s' % (
                ', '.join(missing), job_id))
        return False, None, error_msg

    # Step 4 generating artifacts
    msg = "Step 4 of 4: Generating new artifact"
    qclient.update_job_step(job_id, msg)
    ainfo = [ArtifactInfo(
        'Preprocessed FASTA', 'FASTA_preprocessed', outfiles)]

    return True, ainfo, ""
