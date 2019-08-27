from setuptools import setup

setup(
    name="wrc_livetiming",
    version='0.0.2',
    py_modules=['wrc_livetiming'],
    install_requires=[
        'Click',
        'requests',
        'pandas',
        'isodate',
        'sqlite-utils',
        'kml2geojson',
        'fake_useragent'
    ],
    entry_points='''
        [console_scripts]
        wrc_rallies=wrc_livetiming:cli_showrallies
        wrc_championship_data=wrc_livetiming:cli_get_championship
        wrc_get=wrc_livetiming:cli_get
        wrc_full_run=wrc_livetiming:cli_fullRun
        wrc_set_metadata=wrc_livetiming:cli_metadata
    ''',
)
