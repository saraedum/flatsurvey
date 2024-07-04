set -eo pipefail

HOME=/tmp/jrueth

cd $HOME

ENV=/tmp/jrueth/$1

source $ENV/bin/activate

SCHEDULER=/beegfs/jrueth/scheduler.$1.json

echo "Connecting from `hostname` to scheduler $SCHEDULER"

MKL_NUM_THREADS=1 SAGE_NUM_THREADS=1 OMP_NUM_THREADS=1 DOT_SAGE=/tmp/sage.jrueth$1 DASK_DISTRIBUTED__WORKER__PRELOAD=sage.all dask-worker --scheduler-file $SCHEDULER --nthreads 1 --nworkers 1 --no-nanny --preload flatsurvey.worker.dask

