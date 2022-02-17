from imdb import IMDb

ia = IMDb('s3', 'postgresql://ivan:password@localhost/imdb')

results = ia.search_movie('the matrix')
for result in results:
    print(result.movieID, result)

matrix = results[0]
ia.update(matrix)
print(matrix.keys())