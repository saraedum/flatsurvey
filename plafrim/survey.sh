set -eo pipefail

MINIFORGE=/tmp/jrueth/miniforge
source "${MINIFORGE}/etc/profile.d/conda.sh"
conda activate flatsurvey

SCHEDULER=/beegfs/jrueth/scheduler.flatsurvey-$1.json

mkdir -p /beegfs/jrueth/flatsurvey
flatsurvey --scheduler=$SCHEDULER ngons --vertices $1 local-cache --json /beegfs/jrueth/flatsurvey/orbit-closure.json orbit-closure json --prefix=/beegfs/jrueth/flatsurvey/
