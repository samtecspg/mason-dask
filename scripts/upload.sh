pip3 install twine
python setup.py sdist
twine upload dist/* --config-file=~/.pypirc --verbose
