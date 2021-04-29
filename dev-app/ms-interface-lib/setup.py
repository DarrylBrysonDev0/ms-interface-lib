import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="microsrv_interface_darryl_bryson", 
    version="0.0.1",
    author="Darryl Bryson",
    author_email="Darryl.Bryson@no-reply.com",
    description="Custom interface classes for microservice communication",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DarrylBrysonDev0/ms-interface-lib.git",
    project_urls={
        "Bug Tracker": "https://github.com/DarrylBrysonDev0/ms-interface-lib/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
)