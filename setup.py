import setuptools

setuptools.setup(
    name='fs-omero-pyfs',
    version='0.0.5',
    url='https://github.com/manics/fs-omero-pyfs',
    author='Simon Li',
    license='MIT',
    description='OMERO PyFilesystem2 filesystem',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=setuptools.find_packages(),
    install_requires=[
        'omero-py>=5.6.dev9',
        'fs>=2,<3',
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
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Filesystems',
    ],
)
