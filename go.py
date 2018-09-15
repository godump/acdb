import subprocess
import sys


def call(command):
    print(command)
    r = subprocess.call(command, shell=True)
    if r != 0:
        sys.exit(r)

def ccdb():
    call('go install -i github.com/mohanson/acdb/ccdb/cmd/ccdb')
    call('go test -v ./ccdb')

def main():
    call('go install -i github.com/mohanson/acdb')
    call('go test -v')


if __name__ == '__main__':
    main()
