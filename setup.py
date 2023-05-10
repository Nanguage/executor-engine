from setuptools import setup, find_namespace_packages
import re


classifiers = [
    "Development Status :: 3 - Alpha",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "License :: OSI Approved :: MIT License",
    "Intended Audience :: Science/Research",
    "Intended Audience :: Developers",
]


keywords = [
    'Job Management',
]


URL = "https://github.com/Nanguage/executor-engine"


def get_version():
    with open("executor/engine/__init__.py") as f:
        for line in f.readlines():
            m = re.match("__version__ = '([^']+)'", line)
            if m:
                return m.group(1)
        raise IOError("Version information can not found.")


def get_long_description():
    return f"See {URL}"


def get_requirements_from_file(filename):
    requirements = []
    with open(filename) as f:
        for line in f.readlines():
            line = line.strip()
            if len(line) == 0:
                continue
            if line and not line.startswith('#'):
                requirements.append(line)
    return requirements


def get_install_requires():
    return get_requirements_from_file('requirements.txt')


def get_doc_requires():
    return get_requirements_from_file('docs/requirements.txt')


requires_test = ['pytest', 'pytest-cov', 'pytest-asyncio', 'flake8', 'mypy']
packages_for_dev = ["pip", "setuptools", "wheel", "twine", "ipdb"]

requires_dev = packages_for_dev + requires_test + get_doc_requires()

requires_dask = ['dask', 'distributed', 'nest_asyncio']


setup(
    name='executor-engine',
    author='Weize Xu',
    author_email='vet.xwz@gmail.com',
    version=get_version(),
    license='MIT',
    description='Package for manage job executions.',
    long_description=get_long_description(),
    keywords=keywords,
    url=URL,
    packages=find_namespace_packages(include=['executor.*']),
    include_package_data=True,
    zip_safe=False,
    classifiers=classifiers,
    install_requires=get_install_requires(),
    extras_require={
        'dev': requires_dev + requires_dask,
        'dask': requires_dask,
        'all': requires_dask,
    },
    python_requires='>=3.8, <4',
)
