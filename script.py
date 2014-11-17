import random

population = range(1,100000)

i=1
while(i<=100000):
	a = random.randrange(0, 30, 1)
	b = random.sample(population, a)
	print "%3d"%i,
	for j in range(a):
		print "%3d" % b[j],
	print
	i+=1

