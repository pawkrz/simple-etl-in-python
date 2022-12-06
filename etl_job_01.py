import requests, zipfile, io

zip_file_url = 'https://files.grouplens.org/datasets/movielens/ml-1m.zip'
r = requests.get(zip_file_url)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall("data/")