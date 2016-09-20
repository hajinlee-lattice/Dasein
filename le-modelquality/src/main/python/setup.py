
from distutils.core import setup
from distutils.extension import Extension
from distutils.util import convert_path

version = 'Pre'

with open("README.txt", "rb") as f:
    long_descr = f.read()

setup(
    name = "lattice_modelquality",
    namespace_packages = ['lattice'],
    packages = ['lattice','lattice.modelquality','lattice.modelquality.conf'],
    package_dir = {'lattice.modelquality':convert_path('lattice/modelquality')},
    package_data = {'lattice.modelquality.conf':['*']},
    version = version,
    description = "Utitilies to use the modelquality APIs",
    long_description = long_descr,
    author = "Michael Wilson",
    author_email = "mwilson@lattice-engines.com"
    )
#entry_points = {
#    "console_scripts": ['bootstrap = bootstrap.bootstrap:main']
#    },
