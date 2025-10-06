set -eo pipefail
shopt -s globstar

# Go to the flatsurvey root directory.
cd "$(dirname "$0")"/..

cp -R /beegfs/jrueth/flatsurvey/history/ /beegfs/jrueth/flatsurvey/backup
salloc --ntasks=1 --time=72:00:00 srun --pty pixi run flatsurvey-maintenance join /beegfs/jrueth/flatsurvey/$1/**/*.json /beegfs/jrueth/flatsurvey/history/**/*.json --outdir /beegfs/jrueth/flatsurvey/history/$1
rm /beegfs/jrueth/flatsurvey/$1/**/*.json
