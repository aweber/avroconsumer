from setuptools import setup
import sys

requires = 'requires/python{0}.txt'.format(sys.version_info[0])
with open(requires) as handle:
    requirements = [line.strip() for line in handle.readlines()]

classifiers = ['Development Status :: 4 - Beta',
               'Intended Audience :: Developers',
               'License :: OSI Approved :: BSD License',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.6',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.5',
               'Topic :: Software Development :: Libraries',
               'Topic :: Software Development :: Libraries :: Python Modules']

setup(name='avroconsumer',
      version='0.6.1',
      description="Simplified PostgreSQL client built upon Psycopg2",
      maintainer="Gavin M. Roy",
      maintainer_email="gavinr@aweber.com",
      url="https://github.com/aweber/avroconsumer",
      install_requires=requirements,
      license='BSDv3',
      package_data={'': ['LICENSE', 'README.rst']},
      py_modules=['avroconsumer'],
      classifiers=classifiers)
