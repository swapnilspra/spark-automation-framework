import pulumi, glob, os, mimetypes, configparser
from datetime import datetime
from pulumi_aws import s3
from typing import Dict


class EtlJobDist(pulumi.ComponentResource):
    distribution_obj: s3.BucketObject
    
    def __init__(self, name,
            scripts_bucket:s3.Bucket=None,
            scripts_version:str=None,
            dist_dir:str=None,
            tags:Dict[str,str]=None,
            opts=None):

        super().__init__('hca:EtlJobDistribution', name, None, opts)
        self.dist_dir = dist_dir
        self.scripts_bucket = scripts_bucket

        tags = tags if tags is None else {}
        
        # upload scripts to working/ for versioning/archiving
        self.versioned_scripts_prefix = f"working/{scripts_version}/{pulumi.get_project()}/{pulumi.get_stack()}"

        # jobs will point to scripts/ to keep consistent paths
        self.scripts_prefix = f"scripts/{pulumi.get_project()}/{pulumi.get_stack()}"

        # identify all files in dist/ for upload
        distfiles = [ f for f in glob.glob(self.dist_dir+'/*') if os.path.isfile(f)] + self.get_packages()
        print(f"found dist files to dump to s3 => {distfiles}")

        merged_tags = tags.copy()
        merged_tags.update({
            'hca:datalake_version': scripts_version
        })

        self.distribution_obj = s3.BucketObject('archive',
            bucket=self.scripts_bucket,
            source=pulumi.AssetArchive({
                os.path.basename(f): pulumi.FileAsset(f) for f in distfiles
            }),
            key=os.path.join(self.versioned_scripts_prefix, f"{pulumi.get_project()}_{pulumi.get_stack()}_{datetime.now().strftime('%Y%m%d%H%M%S')}_distribution.zip"),
            tags=merged_tags,
            opts=pulumi.ResourceOptions(parent=self))
        
        self.register_outputs({})

    def get_egg(self, egg_prefix):
        return [ f for f in glob.glob(self.dist_dir+f"/{egg_prefix}*.egg") ]
    
    def get_packages(self):
        return [ f for f in glob.glob(self.dist_dir+'/extra-packages/*.whl') ]

    def get_scripts(self):
        return [ f for f in glob.glob(self.dist_dir+'/*.py') ]

    def get_prefixed_s3_uri(self, file):
        return self.scripts_bucket.id.apply(lambda b: f"s3://{os.path.join(b, self.scripts_prefix, os.path.basename(file))}")

    def get_extra_py_files(self):
        return self.scripts_bucket.id.apply(lambda b: ','.join([ f"s3://{os.path.join(b, self.scripts_prefix, os.path.basename(f))}" for f in self.get_packages()+self.get_egg('hca_etl') ]))