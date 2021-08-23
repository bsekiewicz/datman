from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(

    name="datman",
    version="0.9.1",
    url="https://github.com/bsekiewicz/datman",

    author="Bartosz Sękiewicz",
    author_email="bartosz.pawel.sekiewicz@gmail.com",

    description="Managment of databases connections",

    long_description=long_description,
    long_description_content_type="text/markdown",

    license="MIT",

    classifiers=[
        "Programming Language :: Python :: 3",
    ],

    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=['pandas==1.2.4', 'psycopg2-binary==2.8.6', 'minio==7.0.3'],
    include_package_data=False,

)

