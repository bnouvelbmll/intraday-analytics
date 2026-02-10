from setuptools import find_packages, setup


setup(
    name="intraday-analytics",
    version="0.0.0",
    description="Intraday analytics pipeline",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[],
    entry_points={
        "console_scripts": [
            "beaf=intraday_analytics.beaf:main",
        ]
    },
)
