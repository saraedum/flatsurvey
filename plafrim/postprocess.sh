set -eo pipefail

MINIFORGE=/tmp/jrueth/miniforge
source "${MINIFORGE}/etc/profile.d/conda.sh"
conda activate flatsurvey

pushd /beegfs/jrueth/flatsurvey
flatsurvey-maintenance join *.json
rm ngon-*.json
