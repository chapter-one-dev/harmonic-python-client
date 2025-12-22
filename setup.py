from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="harmonic-python-client",
    version="0.1.0",
    author="Chapter One",
    author_email="jamesin@chapterone.com",
    description="Python client for the Harmonic API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chapter-one-dev/harmonic-python-client",
    packages=find_packages(),
    package_data={
        "harmonic_client": ["payload_data/*.graphql"],
    },
    include_package_data=True,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=[
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
        "google-cloud-bigquery>=3.38.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
        ],
    },
)
