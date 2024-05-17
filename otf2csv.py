#! /usr/bin/env python3

import csv
import time
import otf2
import argparse

from typing import Dict, Any
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
    posixOperations = {'read', 'write', 'fgets', 'fread', 'fwrite', 'pread', 'pwrite', 'pread64', 'pwrite64'}
    mpiOperations = {'MPI_File_iread', 'MPI_File_iread_shared', 'MPI_File_iread_at', 
                    'MPI_File_iwrite', 'MPI_File_iwrite_shared', 'MPI_File_iwrite_at',
                    'MPI_File_read_all_begin', 'MPI_File_read_all', 'MPI_File_read_at', 'MPI_File_read_at_all', 'MPI_File_read_at_all_begin',
                    'MPI_File_write_all_begin', 'MPI_File_write_all', 'MPI_File_write_at', 'MPI_File_write_at_all', 'MPI_File_write_at_all_begin',
                    'MPI_File_read_ordered_begin', 'MPI_File_read_ordered',
                    'MPI_File_write_ordered_begin', 'MPI_File_write_ordered',
                    'MPI_File_read_shared', 'MPI_File_write_shared'}
    
    if function in posixOperations or function in mpiOperations:
        return True
    return False


def handleIoOperation():
    pass


def handleMetaOperation():
    pass


def otf2_to_csv(tracefile: str, csvfile: str) -> None:
    ''' Open `tracefile` and write it as CSV in to `csvfile`. '''
    rank_stat = defaultdict(dict)
    region_set = set()

    TIMER_GRANULARITY = 1000000
    start_time = 0

    with otf2.reader.open(tracefile) as trace:

        with open(csvfile, "w") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['function', 'filename', 'rank', 'start', 'end', 'size', 'offset'])

            for location, event in trace.events:
                if isinstance(event, otf2.events.ProgramBegin):
                    start_time = event.time

                elif isinstance(event, otf2.events.Enter):
                    region_name = event.region.name

                    if isIoOperation(region_name):
                        rank_stat[location.group.name]['function'] = region_name

                        if 'offset' not in rank_stat[location.group.name]:
                            rank_stat[location.group.name]['offset'] = 0

                # elif isinstance(event, otf2.events.Leave):
                #     region_name = event.region.name

                #     if isIoOperation(region_name):
                #         rank_stat[location.group.name].clear()

                elif isinstance(event, otf2.events.IoOperationBegin):
                    filename = file_name(event.handle)

                    if filename and not ignored_file(filename):

                        attributes: Dict[str, Any] = {}
                        if event.attributes:
                            attributes = {attr.name.lower(): value for attr, value in event.attributes.items()}

                        rank_stat[location.group.name]['start'] = (event.time - start_time) / TIMER_GRANULARITY
                        rank_stat[location.group.name]['filename'] = filename
                        rank_stat[location.group.name]['size'] = event.bytes_request
                        if 'offset' in attributes:
                            rank_stat[location.group.name]['offset'] = attributes['offset']

                elif isinstance(event, otf2.events.IoOperationComplete):
                    filename = file_name(event.handle)
                    if filename and not ignored_file(filename):
                        rank = location.group.name

                        function = rank_stat[rank]['function']
                        filename = rank_stat[rank]['filename']
                        start = rank_stat[rank]['start']
                        end = (event.time - start_time) / TIMER_GRANULARITY
                        size = rank_stat[rank]['size']
                        offset = rank_stat[rank]['offset']
                        writer.writerow([function, filename, rank.split()[2], start, end, size, offset])

                        rank_stat[rank]['offset'] += size

                else: continue


def main() -> None:

    parser = argparse.ArgumentParser()
    parser.add_argument("tracefile", help="path to otf2 trace file", type=str)
    parser.add_argument("outfile", help="path to csv output file", type=str)

    args = parser.parse_args()
    otf2_to_csv(args.tracefile, args.outfile)


if __name__ == "__main__":
    main()
