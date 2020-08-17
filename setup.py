from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.readlines()

with open('VERSION') as f:
    version = f.read()

long_description = 'Mason Dask Exeuction Jobs'

setup(
    name='mason_dask',
    version=version,
    author='Kyle Prifogle',
    author_email='kyle.prifogle@samtec.com',
    url='',
    description='',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='',
    packages=find_packages(),
    install_requires=requirements,
    zip_safe=False,
    include_package_data=True,
    python_requires='>=3.6',
)

