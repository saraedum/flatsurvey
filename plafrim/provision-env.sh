set -eo pipefail

conda deactivate || true

ENV=/tmp/jrueth/$1

rm -rf $ENV

echo "Restoring conda environment"

mkdir -p $ENV

tar zxf /beegfs/jrueth/flatsurvey.tar.gz -C $ENV

source $ENV/bin/activate

conda-unpack

python -c 'import cppyy'
