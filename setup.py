import os
from distutils.core import setup

setup(
    name = 'flatsurvey',
    version = '0.1.0',
    packages = ['flatsurvey', 'flatsurvey.cache', 'flatsurvey.jobs', 'flatsurvey.pipeline', 'flatsurvey.aws', 'flatsurvey.surfaces', 'flatsurvey.reporting', 'flatsurvey.worker', 'flatsurvey.cache', 'flatsurvey.ui'],
    license = 'GPL 3.0+',
    long_description = open('README.md').read(),
    include_package_data=True,
    install_requires = [
        "pinject",
    ],
    entry_points = {
        'console_scripts': [
            'flatsurvey=flatsurvey.__main__:survey',
            'flatsurvey-worker=flatsurvey.worker.__main__:worker',
        ],
    },
    package_data = {
        "flatsurvey.aws": ["*.graphql"],
    },
)

