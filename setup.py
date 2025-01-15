from setuptools import setup, find_packages

setup(
    name="tap-cm360",
    version="0.0.1",
    description="Singer tap for CM360, built with the Meltano Singer SDK.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Peter Wang",
    author_email="peter@wearetogether.co.nz",
    url="https://github.com/Together-NZ/tap-cm360",  # Update with your repository URL
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    packages=find_packages(include=["tap_cm360", "tap_cm360.*"]),
    python_requires=">=3.9",
    install_requires=[
        "singer-sdk",
        "requests",
        "oauth2client",
        "google-api-python-client",
        "httplib2",
    ],
    extras_require={
        "s3": ["fs-s3fs~=1.1.1"],
    },
    entry_points={
        "console_scripts": [
            "tap-cm360=tap_cm360.tap:Tapcm360.cli",
        ],
    },
    include_package_data=True,
    license="Apache-2.0",
)
