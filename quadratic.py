import math
def quadratic(a, b, c):
	if [False for i in (a, b, c) if not isinstance(i, (int, float))]:
		raise TypeError('invalid type')
	q = b * b - 4 * a * c
	if a == 0:
		return print('a can\'t be 0')
	elif q < 0:
		return print('don\'t have a valid root')
	else:
		return (-b + math.sqrt(q)) / (2 * a), (-b -math.sqrt(q)) / (2 * a), 
