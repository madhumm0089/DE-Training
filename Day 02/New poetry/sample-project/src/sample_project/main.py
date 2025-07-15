import argparse
from sample_project import greet

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("name")
    args=parser.parse_args()
    result=greet(args.name)
    print(result)

if __name__=="main_":
    main()