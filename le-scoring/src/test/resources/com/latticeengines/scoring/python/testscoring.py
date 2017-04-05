import sys

def main(argv):
    for i in range(1, len(argv)):
        sys.stdout.write(argv[i])
    sys.stderr.write('testEvaluate')

if __name__ == "__main__":
    sys.exit(main(sys.argv))
