import setuptools

setuptools.setup(
    name='speyer',
    version='0.1.0',
    auther='Mathew Odden',
    auther_email='locke105@gmail.com',
    packages=setuptools.find_packages(),
    install_requires=[
        'requests',
        'paramiko'
    ]
)
