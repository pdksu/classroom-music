import argparse
from ast import arg

def ap(args=None):
    p = argparse.ArgumentParser(description="test")
    p.add_argument('-n','--number')
    args = p.parse_args(args=args)
    print(args)
    return(args)
