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
    --fluent                    \
    Enable logging to fluent.
    --fluent-host=<fluent-host> \
    Fluent hostname [default: 127.0.0.1].
    --fluent-tag=<fluent-tag>   \
    Fluent tag [default: debug.backup.db].
"""
from docopt import docopt
import sys
import logging
from xtrabackup.backup_tools_auto import BackupToolAuto
import xtrabackup.log_fluent as log_fluent

def main():
    arguments = docopt(__doc__, version='3.1.2')
    exit_flag = 0
    level = 'INFO'
    try:
        backup_tool = BackupToolAuto(
            arguments['--log-file'],
            arguments['--out-file'],
            cycle=arguments['--cycle'],
            keep=arguments['--keep'],
            fluent=arguments['--fluent'],
            debug=arguments['--debug'])

        backup_tool.start_auto_incremental_backup(
            arguments['<repository>'],
            arguments['--user'],
            arguments['--password'],
            arguments['--host'],
            arguments['--backup-threads'])
    except Exception:
        logger = logging.getLogger(__name__)
        logger.error("pyxtrabackup failed.", exc_info=arguments['--debug'])
        exit_flag = 1
        level = 'ERROR'

    if arguments['--fluent']:
        try:
            log_fluent.send_buffer(arguments['--fluent-host'],
                                   arguments['--fluent-tag'],
                                   level)
        except Exception as error:
            logger = logging.getLogger(__name__)
            logger.error("Log to fluent failed. {}".format(error),
                         exc_info=arguments['--debug'])

    exit(exit_flag)


if __name__ == '__main__':
    sys.exit(main())
