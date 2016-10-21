from setuptools import setup

classifiers = ['Development Status :: 5 - Production/Stable',
               'Intended Audience :: Developers',
               'License :: OSI Approved :: BSD License',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.5',
               'Topic :: Software Development :: Libraries',
               'Topic :: Software Development :: Libraries :: Python Modules']

setup(name='avroconsumer',
      version='1.0.0',
      description='Base consumer class for working with Avro datums',
      maintainer='Gavin M. Roy',
      maintainer_email='gavinr@aweber.com',
      url='https://github.com/gmr/avroconsumer',
      install_requires=['rejected', 'fastavro'],
      license='BSDv3',
      package_data={'': ['LICENSE', 'README.rst']},
      py_modules=['avroconsumer'],
      classifiers=classifiers)
