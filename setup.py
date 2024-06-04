import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="otf2_parser",
    version="0.1",
    author="Onewbiek",
    author_email="yankun0213@gmail.com",
    description="An otf2 parser that convert otf2 to csv",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/onewbiek/otf2csv",
    packages=['otf2_parser'],                  # package for import: after installaion, import otf2_parser
    install_requires=[
        "otf2",
        "typing",
    ],
    classifiers=[
        "Programming Language :: Python :: 3 :: only",
    ],
    python_requires='>=3.6',
    entry_points={
        "console_scripts": [
            "otf2parser=otf2_parser.otf2csv:main"
        ]
    },
)
