set -eo pipefail

MINIFORGE=/tmp/jrueth/miniforge
source "${MINIFORGE}/etc/profile.d/conda.sh"
conda activate flatsurvey

SCHEDULER=/beegfs/jrueth/scheduler.$1.json

dask-scheduler --scheduler-file=$SCHEDULER --port=`shuf -i 32768-60999 -n 1`

