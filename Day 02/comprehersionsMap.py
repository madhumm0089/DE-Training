square = [x ** 2 for x in range(10)]
evens = [x for x in range(10) if x % 2 ==0]

print(square)
print(evens)

sq = list(map(lambda x: x**2, range(10)))
even = list(filter(lambda i: i % 2 == 0, range(10)))

print(sq)
print(even)