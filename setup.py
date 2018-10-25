from setuptools import setup, find_packages
import os, time

if os.path.exists("VERSION.txt"):
    # this file can be written by CI tools (e.g. Travis)
    with open("VERSION.txt") as version_file:
        version = version_file.read().strip().strip("v")
else:
    version = str(time.time())

# Run
setup(
    name="migdar-data-pipelines",
    version=version,
    packages=find_packages(exclude=["tests", "test.*"]),
    install_requires=["datapackage-pipelines"],
    url='https://github.com/hasadna/migdar-data-pipelines',
    license='MIT'
)
