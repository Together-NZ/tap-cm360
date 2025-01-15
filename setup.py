from setuptools import setup, find_packages

setup(
    name="tap-cm360",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "singer-sdk",
        "requests",
        "oauth2client",
        "google-api-python-client",
        "httplib2",
    ],
    include_package_data=True,
)
