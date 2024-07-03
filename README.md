Automation scripts for the [flatsurf](https://github.com/flatsurf) stack to survey large sets of objects.

To perform a full survey, use [`flatsurvey`](./flatsurvey/survey.py). To investigate a single object, run [`flatsurvey-worker`](./flatsurvey/worker/worker.py).

Here, we check that the (1, 1, 1, 6) quadrilateral has dense orbit closure::

```
flatsurvey-worker ngon -a 1 -a 1 -a 1 -a 6 orbit-closure --deform log
```

Here is a typical survey that collects data about triangles, quadrilaterals, and pentagons:

```
mkdir -p surveyname
nice flatsurvey ngons --vertices 3 ngons --vertices 4 ngons --vertices 5 orbit-closure --deform json --prefix=./surveyname
```

# Run Surveys in a Cluster

Install the requirements and flatsurvey in a place that is accessible to all workers.

```
pushd flatsurvey
mamba env create -n flatsurvey environment.yml
mamba activate flatsurvey
pip install -e .
popd
```

Optionally, you might want to build mesh (which is currently not on conda-forge yet)

TODO: On plafrim this does not work because they are lacking down internet access. We should package this on conda-forge instead.

```
git clone https://github.com/plasma-umass/mesh
pushd mesh
make
popd
```

Reserve resources in your cluster (this is a slurm specific command):

```
salloc --ntasks=64 --time=0:30:00 --constraint="diablo|bora|brise|sirocco|zonda|miriel|souris"
```

Spawn a dask scheduler:

```
dask-scheduler --scheduler-file=/beegfs/jrueth/scheduler.json --port 30821
```

Make the shared environment fast (usually the shared environment created above
is on some NFS that is slow, so we copy it to local disk):

TODO: This script could be a bit smarter about doing the work exactly once on each host.

```
$ cat establish-conda.sh
#!/usr/bin/bash
cd /tmp

touch /tmp/jrueth.flock

conda list --prefix /tmp/flatsurvey-tmp && exit 0

echo "Deleting cloned environment..."

rm -rf /tmp/flatsurvey-tmp

sleep 10

echo | flock /tmp/jrueth.flock mamba create --prefix /tmp/flatsurvey-tmp --quiet --copy --offline --clone flatsurvey || echo "Failed to create environment. It probably already exists."
$ srun sh establish-conda.sh
```

Spawn dask workers:

```
$ cat spawn-worker.sh
#!/usr/bin/bash
cd /tmp

echo "Connecting to scheduler.json$1"
LD_PRELOAD=~/TODO/libmesh MKL_NUM_THREADS=1 SAGE_NUM_THREADS=1 OMP_NUM_THREADS=1 DOT_SAGE=/tmp/sage.jrueth$1 DASK_DISTRIBUTED__WORKER__PRELOAD=sage.all mamba run --prefix /tmp/flatsurvey-tmp dask-worker --scheduler-file /beegfs/jrueth/scheduler.json$1 --nthreads 1 --nworkers 1 --no-nanny --preload flatsurvey.worker.dask
$ srun sh spawn-worker.sh
```

Start the survey:

```
mkdir -p /beegfs/jrueth/flatsurvey
flatsurvey --scheduler=/beegfs/jrueth/scheduler.json ngons --vertices 3 orbit-closure --deform json --prefix=/beegfs/jrueth/flatsurvey/
```

...

# Troubleshooting

* For large surveys, RAM might become the limiting factor. It appears that we
  are not actually leaking memory but are hit by memory fragmentation during
  the Boshernitzan criterion. The issue can be fully mitigated by replacing
  malloc with [Mesh](https://github.com/plasma-umass/Mesh), i.e., setting
  `LD_PRELOAD=/path/to/libmesh.so`.
* SageMath (or one of its dependencies) might decide that it's beneficial to
  parallelize things further. However, this can easily overload a system.
  Typically, some linear algebra might spawn a process for each CPU on the
  system which could then easily lead to CPU² many processes if you run a
  lengthy survey. To disable this behaviour, set `MKL_NUM_THREADS=1
  OMP_NUM_THREADS=1 SAGE_NUM_THREADS=1`.
