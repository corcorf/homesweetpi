#!/usr/bin/env python
# pylint: disable=C0103
"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

with open('requirements.txt') as requirements_file:
    requirements = requirements_file.read()

setup_requirements = ['pytest-runner', ]

with open('requirements_dev.txt') as requirements_file:
    test_requirements = requirements_file.read()

setup(
    author="Flann Corcoran",
    author_email='corcorf@posteo.net',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="A home assistant and monitoring suite built with python and a\
                 network of Raspberry Pis",
    entry_points={
        'console_scripts': [
            'homesweetpi=homesweetpi.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='homesweetpi',
    name='homesweetpi',
    packages=find_packages(include=['homesweetpi', 'homesweetpi.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/corcorf/homesweetpi',
    version='0.1.0',
    zip_safe=False,
)
