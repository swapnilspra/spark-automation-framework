from setuptools import setup,find_packages

setup(name='hca_etl',
      version='0.1',
      description='hca etl jobs',
      url='',
      author='Naya',
      author_email='oren.elias@gmail.com',
      packages=find_packages('src'),
    package_dir={'': 'src',
    },
    package_data={
        'metadata':['**/*.json']
    },
    zip_safe=False)