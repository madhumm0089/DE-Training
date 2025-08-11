x =10 
def modify(val):
    # global x
    # x = x + 5
    return val + 5
print(modify(x))
print(x)

a = [1,2]
b = a
c = list(a)

print(a is b)
print(a == b)
print(a is c)