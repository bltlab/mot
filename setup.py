#! /usr/bin/env python

from os import path

from setuptools import find_packages, setup


def setup_package() -> None:
    root = path.abspath(path.dirname(__file__))
    with open(path.join(root, "README.md"), encoding="utf-8") as f:
        long_description = f.read()

    setup(
        name="motext",
        version="0.2.1",
        packages=find_packages(include=("motext", "motext.*")),
        # Package type information
        package_data={"motext": ["py.typed"]},
        python_requires=">=3.8",
        license="MIT",
        description="motext: The interface to the Multilingual Open Text (MOT) corpus",
        long_description=long_description,
        install_requires=[
            "click",
        ],
        entry_points="""
            [console_scripts]
            motext=motext.scripts.motext:cli
        """,
        classifiers=[
            "Development Status :: 4 - Beta",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Topic :: Scientific/Engineering :: Artificial Intelligence",
        ],
        url="https://github.com/bltlab/mot",
        long_description_content_type="text/markdown",
        author="Constantine Lignos",
        author_email="lignos@brandeis.edu",
    )


if __name__ == "__main__":
    setup_package()
