import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

requires = [
    'boto>=2.2.1',
]

tests_requires = requires + [
    'django>=1.3',
    'pyramid'
]

setup(name='pynamo',
      version='0.1',
      description='Pynamo is a simple interface to DynamoDB for Python',
      long_description=README + '\n\n' +  CHANGES,
      classifiers=[
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: System :: Distributed Computing"
        ],
      author='Samuel Sutch',
      author_email='sam@ficture.it',
      url='https://github.com/samuraisam/pynamo',
      keywords='web pyramid pylons python dynamodb django databse',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=requires,
      test_suite="tests")
