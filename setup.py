from setuptools import setup, find_packages

setup(
    name="dreyfus",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'scikit-learn',
        'transformers',
        'plotly',
        'matplotlib',
        'torch',
        'seaborn',  # Also commonly used with matplotlib
    ],
) 