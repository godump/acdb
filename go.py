import subprocess
import sys


def call(command):
    print(command)
    r = subprocess.call(command, shell=True)
    if r != 0:
        sys.exit(r)


def test():
    call('go test -v')


def main():
    call(f'go install -i github.com/mohanson/acdb')
    call(f'go install -i github.com/mohanson/acdb/cmd/acdb')


if __name__ == '__main__':
    main()
