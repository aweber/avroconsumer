from setuptools import setup

classifiers = ['Development Status :: 4 - Beta',
               'Intended Audience :: Developers',
               'License :: OSI Approved :: BSD License',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.6',
               'Programming Language :: Python :: 2.7',
               'Topic :: Software Development :: Libraries',
               'Topic :: Software Development :: Libraries :: Python Modules']

setup(name='avroconsumer',
      version='0.6.2',
      description="Rejected consumer mixins for serializing and deserializing Avro datum",
      maintainer="Gavin M. Roy",
      maintainer_email="gavinr@aweber.com",
      url="https://github.com/aweber/avroconsumer",
      install_requires=['rejected', 'avro'],
      license='BSDv3',
      package_data={'': ['LICENSE', 'README.rst']},
      py_modules=['avroconsumer'],
      classifiers=classifiers)
