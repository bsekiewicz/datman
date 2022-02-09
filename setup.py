from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(

    name="datman",
    version="0.9.5",
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
    install_requires=['pandas>=1.3.4', 'psycopg2>=2.9.3', 'minio>=7.1.2'],
    include_package_data=False,

)
