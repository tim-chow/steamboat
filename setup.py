from setuptools import setup, find_packages

setup(
    name='steamboat',
    version='0.0.2a1',
    packages=find_packages(),
    test_suite='steamboat_tests',
    install_requires=[
        'futures',
        'requests'
    ],

    author='jingjiang6',
    author_email='jingjiang6@staff.sina.com.cn'
)

