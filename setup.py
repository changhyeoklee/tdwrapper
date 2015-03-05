from setuptools import setup

setup(name='tdwrapper',
      version='0.0.1',
      description='Teradata utility wrapper for Python',
      url='https://github.com/changhyeoklee/tdwrapper',
      author='Changhyeok Lee',
      author_email='Changhyeoklee@gmail.com',
      license='MIT',
      packages=['tdwrapper'],
      install_requires=[
          'subprocess32',
          'pandas',
      ],
      zip_safe=False)
