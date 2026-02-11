from setuptools import find_packages, setup


setup(
    name="bmll-basalt",
    version="0.1.0",
    description="Basalt intraday analytics pipeline",
    packages=find_packages(exclude=("intraday_analytics*",)),
    include_package_data=True,
    install_requires=[],
    entry_points={
        "console_scripts": [
            "basalt=basalt.basalt:main",
        ],
        "basalt.cli": [
            "dagster=basalt.dagster.cli_ext:get_cli_extension",
        ],
    },
)
