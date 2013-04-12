from __future__ import print_function


def generate_complements(sequence, sum_):
    unpaired = []
    for i in sequence:
        for j in unpaired:
            if i + j == sum_:
                unpaired.remove(j)
                yield i, j
                break
        else:
            unpaired.append(i)


def main(sequence, sum_):
    for complement in generate_complements(sequence, sum_):
        numbers = (int(i) if int(i) == i else i for i in complement)
        print(*numbers)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('numbers', metavar='N', type=float, nargs='+')
    parser.add_argument('-s', '--sum', metavar='SUM', type=float)
    args = parser.parse_args()
    main(args.numbers, args.sum)
