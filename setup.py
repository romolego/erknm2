"""Setup script"""
from setuptools import setup, find_packages

setup(
    name="erknm",
    version="1.0.0",
    description="Робот сбора и инкрементальной загрузки открытых данных ФГИС ЕРКНМ",
    packages=find_packages(),
    install_requires=[
        "playwright>=1.40.0",
        "psycopg2-binary>=2.9.9",
        "python-dotenv>=1.0.0",
        "lxml>=4.9.3",
        "requests>=2.31.0",
        "click>=8.1.7",
    ],
    entry_points={
        "console_scripts": [
            "erknm=erknm.cli:cli",
        ],
    },
    python_requires=">=3.8",
)








