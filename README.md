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

Install the requirements and flatsurvey and package it for the workers:

```
sh plafrim/create-env.sh
```

Spawn a dask scheduler:

```
sh plafrim/spawn-scheduler.sh flatsurvey-3
```

Reserve resources in your cluster (this is a slurm specific command):

```
salloc --ntasks=768 --time=13:30:00 --constraint="diablo|bora|brise|sirocco|zonda|miriel|souris|kona"
```

Spawn the workers:

```
for host in $(scontrol show hostnames); do sleep 1; srun --nodes=1 --ntasks=1 --exclusive -w $host sh plafrim/provision-env.sh flatsurvey-3 & done
wait
srun sh plafrim/spawn-worker.sh flatsurvey-3
```

Start the survey:

```
sh plafrim/survey.sh 3
```

Post-process the survey:

```
sh plafrim/postprocess.sh
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
