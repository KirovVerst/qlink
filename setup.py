from setuptools import setup
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='qlink',
    version='0.1a1',
    description='Entity Resolution and Record Linkage library',
    long_description=long_description,
    url='https://github.com/KirovVerst/qlink',
    author='KirovVerst',
    author_email='kirov.verst@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules'

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.6',
    ],
    keywords=['deduplication', 'record linkage', 'entity resolution', 'match'],
    packages=["qlink"],
)
