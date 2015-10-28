"""Xtrabackup script

Usage:
    pyxtrabackup-inc-auto <repository> --user=<user> [options]
    pyxtrabackup-inc-auto (-h | --help)
    pyxtrabackup --version

Options:
    -h --help                   \
    Show this screen.
    -d --debug                  \
    Enable verbose error
    --version                   \
    Show version.
    --user=<user>               \
    MySQL user.
    --password=<pwd>            \
    MySQL password.
    --host=<host>               \
    MySQL server.
    --cycle=<cycle>             \
    Cycle day [default: 7].
    --keep=<keep>               \
    Numbers of cycle to keep. If not specified, then keep everything.
    --log-file=<log>            \
    Log file [default: /var/log/mysql/pyxtrabackup.log].
    --out-file=<log>            \
    Output file [default: /var/log/mysql/xtrabackup.out].
    --backup-threads=<threads>  \
    Threads count [default: 1].
    --no-compress               \
    Do not create a compressed archive of the backup.
"""
from docopt import docopt
import sys
import logging
from xtrabackup.backup_tools_auto import BackupToolAuto


def main():
    arguments = docopt(__doc__, version='3.1.2')
    try:
        backup_tool = BackupToolAuto(
            arguments['--log-file'], arguments['--out-file'],
            arguments['--no-compress'], arguments['--cycle'],
            arguments['--keep'], arguments['--debug'])

        backup_tool.start_auto_incremental_backup(
            arguments['<repository>'],
            arguments['--user'],
            arguments['--password'],
            arguments['--host'],
            arguments['--backup-threads'])
    except Exception:
        logger = logging.getLogger(__name__)
        logger.error("pyxtrabackup failed.", exc_info=arguments['--debug'])
        exit(1)
    exit(0)


if __name__ == '__main__':
    sys.exit(main())
