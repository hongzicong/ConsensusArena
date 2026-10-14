#!/usr/bin/env python3
"""Observe LISTEN without opening a connection or consuming an upstream handshake."""
from pathlib import Path
import sys
import time


def listening(port, tables=('/proc/net/tcp', '/proc/net/tcp6')):
    for table in tables:
        path = Path(table)
        if not path.exists():
            continue
        for line in path.read_text().splitlines()[1:]:
            fields = line.split()
            if len(fields) >= 4 and fields[3] == '0A' and int(fields[1].rsplit(':', 1)[1], 16) == port:
                return True
    return False


if __name__ == '__main__':
    port = int(sys.argv[1])
    marker = Path(sys.argv[2])
    deadline = time.monotonic() + 60
    while not listening(port):
        if time.monotonic() >= deadline or (marker.parent / 'stop').exists():
            raise SystemExit('replica listener did not become ready')
        time.sleep(.05)
    marker.touch()
