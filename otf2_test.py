#! /usr/bin/env python3

import time
import argparse
import csv
from typing import Dict, Any
import otf2

from collections import defaultdict

def file_name(handle: otf2.definitions.IoHandle) -> str:
    ''' Extract the correct name from `IoHandle`, which is either the file name
        or the handle name. '''
    try:
        return handle.file.name
    except AttributeError:
        return handle.name


def ignored_file(filename):
    ignored_prefixes = {'/sys/', '/proc', '/etc/', 'STDIN_FILENO', 'STDOUT_FILENO', 'STDERR_FILENO', '/cvmfs/'}
    for prefix in ignored_prefixes:
        if filename.startswith(prefix):
            return True
    return False


def isIoOperation(function):
    IoOperations = {'read', 'write', 'fgets', 'fread', 'pread', 'pwrite', 'MPI_File_write_at_all', 'MPI_File_read_at_all'}
    if function in IoOperations:
        return True
    return False


def otf2_to_csv(tracefile: str, csvfile: str) -> None:
    ''' Open `tracefile` and write it as CSV in to `csvfile`. '''
    rank_stat = defaultdict(dict)
    region_set = set()

    with otf2.reader.open(tracefile) as trace:

        with open(csvfile, "w") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['function', 'filename', 'rank', 'start', 'end', 'size', 'offset'])

            for location, event in trace.events:

                if isinstance(event, otf2.events.IoOperationBegin):

                    writer.writerow([event])


def main() -> None:

    parser = argparse.ArgumentParser()
    parser.add_argument("tracefile", help="path to otf2 trace file", type=str)
    parser.add_argument("outfile", help="path to csv output file", type=str)

    args = parser.parse_args()
    otf2_to_csv(args.tracefile, args.outfile)


if __name__ == "__main__":
    main()
