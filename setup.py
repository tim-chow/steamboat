from setuptools import setup, find_packages

setup(
    name='steamboat',
    version='0.0.3a1',
    packages=find_packages(),
    test_suite='steamboat_tests',
    install_requires=[
        'futures',
        'requests',
        'tornado'
    ]
)
