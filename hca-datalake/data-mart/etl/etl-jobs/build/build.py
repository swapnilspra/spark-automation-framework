import os,sys
sys.path.insert(0, "../src/")

import argparse
from setuptools import setup,find_packages
import common.utils
import json
import glob


import pkgutil, jobs as jobsdir


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
       if isinstance(obj, set):
          return list(obj)
       return json.JSONEncoder.default(self, obj)

setup(name='hca_etl',
    version='0.1',
    description='hca etl jobs',
    url='',
    author='NayaTech',
    author_email='oren.elias@gmail.com',
    packages=find_packages('../src'),
    package_dir={
        '': '../src',
    },
    package_data={
        'metadata':['**/*']
    },
    zip_safe=True)

import shutil

src_directory = os.path.abspath(os.path.join(os.path.dirname( __file__ ),'..','src'))
scripts_directory = os.path.abspath(os.path.join(os.path.dirname( __file__ ),'..','scripts'))
dist_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'dist'))

scripts_to_copy = ['glue_job_runner.py','glue_sync_table.py']
for script in scripts_to_copy:
    print(f"copying {script}")
    shutil.copyfile(
        os.path.join(scripts_directory, script), 
        os.path.join(dist_dir,script))

# prepare the zip directory for the lambda
glue_notification_dist_dir = os.path.join(dist_dir,"lambda-glue-notification")
os.makedirs(glue_notification_dist_dir, exist_ok=True)

job_dependencies = common.utils.get_catalog_dependencies()
with open(os.path.join(glue_notification_dist_dir,'job_dependencies.json'), 'w') as outfile:
    json.dump(job_dependencies, outfile, cls=SetEncoder)

catalog = common.utils.get_catalog()
with open(os.path.join(glue_notification_dist_dir,'catalog.json'), 'w') as outfile:
    json.dump(catalog, outfile, cls=SetEncoder)

shutil.copyfile(os.path.join(src_directory,'lambdas','glue_notification.py'), os.path.join(glue_notification_dist_dir,'glue_notification.py'))

# resulting structure of {dist_dir} => 
# ├── hca_etl-0.1-py3.6.egg
# ├── environment.ini
# ├── glue_job_runner.py
# ├── glue_sync_table.py
# └── lambda-glue-notification
#     ├── catalog.json
#     ├── glue_notification.py
#     └── job_dependencies.json
# └── extra-packages
#     ├── pg8000-1.13.2-py3-none-any.whl
#     ├── python_dateutil-2.8.1-py2.py3-none-any.whl
#     └── scramp-1.1.0-py3-none-any.whl
# NOTE pulumi will add additional file to {dist_dir}/environment.ini

extra_packages_dist_dir = os.path.join(dist_dir,"extra-packages")
os.makedirs(extra_packages_dist_dir, exist_ok=True)
for f in glob.glob("./pkg/*.whl"):
    shutil.copyfile(f, os.path.join(extra_packages_dist_dir, os.path.basename(f)))

# example => python build.py bdist_egg