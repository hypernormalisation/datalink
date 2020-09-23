from setuptools import setup, find_packages
from os import path
here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='datalink',
    version='0.1.3',

    description=('Create simple interfaces to SQL that make working with '
                 'data as simple as working with dictionaries.'),

    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/hypernormalisation/datalink',
    author='Stephen Ogilvy',
    author_email='sogilvy@protonmail.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Database :: Front-Ends',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],

    keywords='development database sql pandas',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    python_requires='>=3.6',
    install_requires=['sqlalchemy',
                      'dataset',
                      'sqlalchemy',
                      'sqlalchemy_utils',
                      'traits==5.2.0',
                      'pandas'],

    project_urls={
        'Bug Reports': 'https://github.com/hypernormalisation/datalink/issues',
        'Source': 'https://github.com/hypernormalisation/datalink',
    },
)
