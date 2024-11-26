set -eo pipefail

# TODO: Adapt to pixi.
MINIFORGE=/tmp/jrueth/miniforge
rm -rf $MINIFORGE
wget -O miniforge.sh "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
sh miniforge.sh -b -p $MINIFORGE
source "${MINIFORGE}/etc/profile.d/conda.sh"
source "${MINIFORGE}/etc/profile.d/mamba.sh"

mamba env create -n flatsurvey -f environment.yml
mamba activate flatsurvey
pip install .

mamba install -n base -y conda-pack
rm -f /beegfs/jrueth/flatsurvey.tar.gz
conda pack -n flatsurvey -o /beegfs/jrueth/flatsurvey.tar.gz

