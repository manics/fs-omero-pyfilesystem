import setuptools

setuptools.setup(
    name='fs-omero-pyfs',
    version='0.0.2',
    url='https://github.com/manics/fs-omero-pyfs',
    author='Simon Li',
    license='BSD 3-Clause',
    description='OMERO PyFilesystem2 filesystem',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=setuptools.find_packages(),
    install_requires=[
        'omero-py>=5.6.dev8',
        'fs>=2,<=3',
    ],
    entry_points={
        'fs.opener': [
            'omero = fs_omero_pyfs:OmeroFSOpener',
            'omero+ws = fs_omero_pyfs:OmeroFSOpener',
            'omero+wss = fs_omero_pyfs:OmeroFSOpener',
        ]
    },
    python_requires='>=3.5',
    tests_require=[
        'pytest>=5,<=6',
    ],
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Filesystems',
    ],
)
