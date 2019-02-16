from setuptools import setup

setup(
    name="wrc_livetiming",
    version='0.0.1',
    py_modules=['wrc_livetiming'],
    install_requires=[
        'Click',
        'requests',
        'pandas',
        'isodate',
        'sqlite-utils',
        'kml2geojson'
    ],
    entry_points='''
        [console_scripts]
        wrc_rallies=wrc_livetiming:cli_showrallies
        wrc_championship_data=wrc_livetiming:cli_get_championship
        wrc_get_stage=wrc_livetiming:cli_getOne
        wrc_get_all=wrc_livetiming:cli_getAll
        wrc_full_run=wrc_livetiming:cli_fullRun
        wrc_set_metadata=wrc_livetiming:cli_metadata
    ''',
)
