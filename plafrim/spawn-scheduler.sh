set -eo pipefail

MINIFORGE=/tmp/jrueth/miniforge
source "${MINIFORGE}/etc/profile.d/conda.sh"
conda activate flatsurvey

SCHEDULER=/beegfs/jrueth/scheduler.$1.json

dask-scheduler --scheduler-file=$SCHEDULER --port 38325

