import os, sys
sys.path.insert(0, "../../src")
import pkgutil, jobs as jobsdir
from typing import Dict, Set, Iterator
from collections import defaultdict

# utility to append aws arns with paths
def arn_concat(*paths):
    patharr = []
    for path in list(paths):
        patharr += [ p for p in path.split('/') if p ]
    return '/'.join(patharr)


# prepare map of project + set[target table]
def get_tables_by_project(job_dependencies:Dict[str,object]) -> Dict[str,Set[str]]:
    table_catalog = defaultdict(set)

    for jobname in etl_job_names():
        modulename = jobname.split('.')[0]

        job_dependency = job_dependencies[jobname] if jobname in job_dependencies else None
        if not job_dependency:
            raise Exception(f"job dependency not found for job => {jobname} from catalog => {job_dependencies.keys()}")

        table_name = job_dependency['target_table']
        table_catalog[modulename].add(table_name)

    return table_catalog

# get iterable of elt job names in format module.jobname
def etl_job_names() -> Iterator[str]:
    for _, modname, ispkg in pkgutil.walk_packages(path=jobsdir.__path__, prefix='jobs.'):
        if not ispkg:
            yield '.'.join(modname.split('.')[1:-1])
