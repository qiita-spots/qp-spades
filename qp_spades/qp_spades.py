# -----------------------------------------------------------------------------
# Copyright (c) 2021-, Qiita development team.
#
# Distributed under the terms of the BSD 3-clause License License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------
from os import environ
from os.path import join, exists
from glob import glob
from subprocess import run, PIPE

from qiita_client import ArtifactInfo
from qiita_client.util import system_call

# resources per job
WALLTIME = '200:00:00'
MAX_RUNNING = 8
MEMORY = '128g'
FINISH_WALLTIME = '10:00:00'
FINISH_MEMORY = '4g'


def spades_to_array(directory, output_dir, prefix_to_name, url,
                    job_id, params):
    environment = environ["ENVIRONMENT"]
    ppn = params["threads"]

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
        f'spades.py --{params["type"]} -m {MEMORY.replace("g", "")} '
        f'-t {ppn} -o $OUTDIR/$SNAME')
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
            '--gemcode1-1 $OUTDIR/${SNAME}.notCombined_1.fastq '
            '--gemcode1-2 $OUTDIR/${SNAME}.notCombined_2.fastq')
    else:
        command = '%s --gemcode1-1 ${FWD} --gemcode1-2 ${REV}' % command

    # 3. create command for array submission
    marray = [
        '#!/bin/bash',
        '#SBATCH -p qiita',
        '#SBATCH --mail-user qiita.help@gmail.com',
        f'#SBATCH --job-name {job_id}',
        '#SBATCH -N 1',
        f'#SBATCH -n {ppn}',
        f'#SBATCH --time {WALLTIME}',
        f'#SBATCH --mem {MEMORY}',
        f'#SBATCH --output {output_dir}/{job_id}_%a.log',
        f'#SBATCH --error {output_dir}/{job_id}_%a.err',
        f'#SBATCH --array 1-{num_samples}%{MAX_RUNNING}',
        f'cd {output_dir}',
        f'{environment}',
        f'OUTDIR={output_dir}/',
        'date',
        'hostname',
        'echo ${SLURM_JOBID} ${SLURM_ARRAY_TASK_ID}',
        'offset=${SLURM_ARRAY_TASK_ID}',
        'args=$(head -n $offset ${OUTDIR}/files_to_process.txt| tail -n 1)',
        "FWD=$(echo -e $args | awk '{ print $1 }')",
        "REV=$(echo -e $args | awk '{ print $2 }')",
        "SNAME=$(echo -e $args | awk '{ print $3 }')",
        f'{command}',
        'date']

    # 4. create command to finish job in Qiita
    fcmd = [
        '#!/bin/bash',
        '#SBATCH -p qiita',
        '#SBATCH --mail-user qiita.help@gmail.com',
        f'#SBATCH --job-name finish-{job_id}',
        '#SBATCH -N 1',
        '#SBATCH -n 1',
        f'#SBATCH --time {FINISH_WALLTIME}',
        f'#SBATCH --mem {FINISH_MEMORY}',
        f'#SBATCH --output {output_dir}/finish-{job_id}.log',
        f'#SBATCH --error {output_dir}/finish-{job_id}.err',
        f'cd {output_dir}',
        f'{environment}',
        'date',
        'hostname',
        'echo $SLURM_JOBID',
        f'finish_qp_spades {url} {job_id} {output_dir}\n'
        "date"]

    # write files
    with open(join(output_dir, 'files_to_process.txt'), 'w') as f:
        f.write('\n'.join(files))
    main_fp = join(output_dir, f'{job_id}.slurm')
    with open(main_fp, 'w') as job:
        job.write('\n'.join(marray))
        job.write('\n')
    finish_fp = join(output_dir, f'{job_id}.finish.slurm')
    with open(finish_fp, 'w') as job:
        job.write('\n'.join(fcmd))
        job.write('\n')

    return main_fp, finish_fp


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

    files, prep = qclient.artifact_and_preparation_files(artifact_id)
    prep_info = prep.set_index('run_prefix')['sample_name'].to_dict()

    outfiles = []
    for run_prefix, sname in prep_info.items():
        scaffold = join(out_dir, run_prefix, 'scaffolds.fasta')
        new_scaffold = join(out_dir, run_prefix, f'{run_prefix}.fasta')
        if exists(scaffold):
            run(['mv', scaffold, new_scaffold], stdout=PIPE)
        else:
            run(['touch', new_scaffold], stdout=PIPE)
        outfiles.append((new_scaffold, 'preprocessed_fasta'))

    # Step 4 generating artifacts
    msg = "Step 4 of 4: Generating new artifact"
    qclient.update_job_step(job_id, msg)
    ainfo = [ArtifactInfo(
        'Preprocessed FASTA', 'FASTA_preprocessed', outfiles)]

    return True, ainfo, ""
