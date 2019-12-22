import setuptools

setuptools.setup(
    name='fs-omero-pyfs',
    version='0.0.1',
    url='https://github.com/manics/fs-omero-pyfs',
    author='Simon Li',
    license='BSD 3-Clause',
    description='OMERO PyFilesystem2 filesystem',
    packages=setuptools.find_packages(),
    install_requires=[
        'notebook',
        'fs>=2<=3',
    ],
    entry_points={
        'fs.opener': [
            'omero = fs_omero_pyfs:OmeroFSOpener',
            'omero+ws = fs_omero_pyfs:OmeroFSOpener',
            'omero+wss = fs_omero_pyfs:OmeroFSOpener',
        ]
    },
    python_requires='>=3.5',
    tests_requires=[
        'pytest>=5,<=6',
    ],
    classifiers=[
        'Framework :: OMERO',
        'Framework :: PyFilesystem',
    ],
)
