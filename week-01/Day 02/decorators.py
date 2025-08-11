import argparse
def log_time(func):
    print('i am here in log time')
    # print(func)
    def wrapper(*args, **kwargs):
        # print(*args)
        # print(**kwargs)
        print('I am from logtime wrapper')
        result = func(*args, **kwargs)
        print('function ended')
        return result
    return wrapper

@log_time
def process():
    print('I am printing from process method')

process()
# def sample():
#     print()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description= 'process some data')
    parser.add_argument('name', type=str)
    parser.add_argument('age', type=int)
    args = parser.parse_args()
    print(args.name)
    print(args.age)


#passing key word argument

    