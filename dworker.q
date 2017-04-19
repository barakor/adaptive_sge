#!/bin/bash

#$ -S /bin/bash
#$ -N dworker

function usage
{
    echo "
    ATTENTION: This script was written with the inttention of being called by another
    script.
    This scripts launches a worker proccess. it should
    be launched to a cluster managment program.
    usage:
    -s | --source )         virtual env path, bash activate

    -o | --output )         cluster output directory

    -py | --PYTHONPATH )     python project path, argument passed to dask-worker

    -d | --DJANGO_MODULE )  django settings module, usually 'DJANGOPROJECTNAME.settings'

    -t | --nthreads )       number of nthreads for the dask-worker, if you are on a shared cluster
                            it might be politer to use 1 here

    -i | --ip )             distributed scheduler ip address

    -p | --port )           distributed scheduler port address

    -h | --help )           print this help info
                            "
}
while [ "$1" != "" ]; do
    case $1 in
        -s | --source )         shift
                                source_dir=$1
                                source $1/activate
                                ;;
        -o | --output )         shift
                                cluster_output_dir=$1
                                ;;
        -py | --PYTHONPATH )    shift
                                pythonpath=$1
                                ;;
        -d | --DJANGO_MODULE )  shift
                                django_settings_module=$1
                                ;;
        -t | --nthreads )       shift
                                nthreads=$1
                                ;;
        -i | --ip )             shift
                                ip=$1
                                ;;
        -p | --port )           shift
                                port=$1
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done

#$ -o $cluster_output_dir/o.$JOB_ID.$TASK_ID.$USER.$HOSTNAME.stdout
#$ -e $cluster_output_dir/o.$JOB_ID.$TASK_ID.$USER.$HOSTNAME.stderr
echo $JOB_ID
echo $TASK_ID
echo $USER
echo $HOSTNAME
ulimit -c 10
source $1/activate
echo $pythonpath
PYTHONPATH=$pythonpath DJANGO_SETTINGS_MODULE=$django_settings_module dask-worker --nthreads $nthreads $ip:$port
