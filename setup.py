import os
import setuptools

pkgdir = os.path.dirname(__file__)
with open(os.path.join(pkgdir, 'README.md')) as f:
    long_description = f.read()

setuptools.setup(
    name='s3watcher',
    version='0.1.0',
    author='Dan Nicholson',
    author_email='nicholson@endlessm.com',
    description='Watch AWS S3 bucket contents',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/dbnicholson/s3watcher',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: '
        'GNU General Public License v2 or later (GPLv2+)',
        'Operating System :: POSIX',
    ],
    python_requires='>=3.7',
    install_requires=[
        'boto3',
        'msgpack',
    ],
)
