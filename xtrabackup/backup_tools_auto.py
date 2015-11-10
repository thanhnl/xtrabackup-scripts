from xtrabackup.command_executor import CommandExecutor
from xtrabackup.exception import ProcessError
from xtrabackup.http_manager import HttpManager
import xtrabackup.filesystem_utils as filesystem_utils
import xtrabackup.log_manager as log_manager
import xtrabackup.exception as exception
import xtrabackup.timer as timer
import logging
import os


class BackupToolAuto:

    def __init__(self, log_file, output_file,
                 cycle=7, keep=None,
                 fluent=False, debug=False):
        self.debug = debug
        self.cycle = int(cycle)
        self.keep = keep
        self.log_manager = log_manager.LogManager()
        self.stop_watch = timer.Timer()
        self.setup_logging(log_file, fluent)
        try:
            with open(output_file, 'a+'):
                pass
        except Exception as error:
            self.logger.error('Output file error: {}'.format(error),
                              exc_info=self.debug)
            raise
        self.command_executor = CommandExecutor(output_file)
        self.http = HttpManager()

    def setup_logging(self, log_file, fluent):
        self.logger = logging.getLogger(__name__)
        self.log_manager.attach_file_handler(self.logger, log_file)
        if fluent:
            try:
                self.log_manager.attach_fluent_buffer(self.logger)
            except Exception as error:
                self.logger.error(
                    "Fail to attach fluent buffer. {}".format(error),
                    exc_info=self.debug)

    def check_prerequisites(self, repository):
        try:
            filesystem_utils.check_required_binaries(['innobackupex', 'tar'])
            filesystem_utils.check_path_existence(repository)
        except exception.ProgramError as error:
            self.logger.error('Prerequisites check failed. {}'.format(error),
                              exc_info=self.debug)
            raise

    def prepare_repository(self, repository):
        try:
            self.backup_repository = filesystem_utils.create_sub_repository(
                repository, '')
            self.logger.info(
                "Prepare new repository: {}".format(self.backup_repository))
        except Exception as error:
            self.logger.error('Unable to create repository: {}'.format(error),
                              exc_info=self.debug)
            raise

    def prepare_archive_folder(self, incremental):
        if incremental:
            backup_prefix = ''.join(['inc_', str(self.incremental_step), '_'])
        else:
            backup_prefix = 'base_'
        self.final_archive_folder = filesystem_utils.prepare_archive_folder(
            self.backup_repository, backup_prefix)
        self.logger.info("Archive folder: {}".format(self.final_archive_folder))

    def exec_incremental_backup(self, user, password, thread_count, host):
        self.stop_watch.start_timer()
        try:
            self.command_executor.exec_incremental_backup(
                user,
                password,
                thread_count,
                self.last_lsn,
                self.workdir,
                host)
        except ProcessError:
            self.logger.error(
                'An error occured during the incremental backup process.',
                exc_info=self.debug)
            self.clean(self.workdir)
            raise
        self.stop_watch.stop_timer()
        self.logger.info("Incremental backup duration: {}".format(
            self.stop_watch.duration_in_seconds()))

    def exec_full_backup(self, user, password, thread_count, host=None):
        self.stop_watch.start_timer()
        try:
            self.command_executor.exec_filesystem_backup(
                user,
                password,
                thread_count,
                self.workdir,
                host)
        except ProcessError:
            self.logger.error(
                'An error occured during the backup process.',
                exc_info=self.debug)
            self.clean(self.workdir)
            raise
        self.stop_watch.stop_timer()
        self.logger.info("Base backup duration: {}".format(
            self.stop_watch.duration_in_seconds()))

    def clean(self, path):
        filesystem_utils.delete_directory_if_exists(path)

    def save_incremental_data(self, incremental):
        try:
            if incremental:
                self.incremental_step += 1
            else:
                self.incremental_step = 0
            self.last_lsn = filesystem_utils.retrieve_value_from_file(
                self.workdir + '/xtrabackup_checkpoints',
                '^to_lsn = (\d+)$')
            filesystem_utils.write_array_to_file(
                self.incremental_data,
                ['BASEDIR=' + self.final_archive_folder,
                 'LSN=' + self.last_lsn,
                 'INCREMENTAL_STEP=' + str(self.incremental_step)])
            self.logger.info("Incremental data saved")
        except Exception as error:
            self.logger.error(
                "Unable to save the incremental backup data: {}".format(error),
                exc_info=self.debug)
            self.clean(self.workdir)
            raise

    def load_incremental_data(self):
        try:
            self.base_dir = filesystem_utils.retrieve_value_from_file(
                self.incremental_data,
                '^BASEDIR=(.*)$')
            self.last_lsn = filesystem_utils.retrieve_value_from_file(
                self.incremental_data,
                '^LSN=(\d+)$')
            self.incremental_step = int(
                filesystem_utils.retrieve_value_from_file(
                    self.incremental_data,
                    '^INCREMENTAL_STEP=(\d+)$'))
            self.logger.info("Incremental data loaded")
        except Exception as error:
            self.logger.error(
                "Unable to load the incremental backup data: {}".format(error),
                exc_info=self.debug)
            raise

    def check_repository(self, repository):
        try:
            self.archive_folders_list = filesystem_utils.get_archive_list(repository)
        except Exception as error:
            self.logger.error(
                    "Error get archive list: {}".format(error),
                    exc_info=self.debug)
            raise
        if len(self.archive_folders_list) > 0:
            try:
                self.backup_repository = filesystem_utils.check_cycle(
                    self.archive_folders_list, self.cycle)
                self.logger.info("Use repository: {}".format(
                    self.backup_repository))
            except Exception as error:
                self.logger.error(
                    "Error scanning repository: {}".format(error),
                    exc_info=self.debug)
                raise
        else:
            self.backup_repository = None

    def cleanup_repository(self):
        keep_number = int(self.keep)
        archive_list_len = len(self.archive_folders_list)
        if archive_list_len > keep_number:
            if self.backup_repository in self.archive_folders_list:
                index = archive_list_len - keep_number
            else:
                index = archive_list_len - keep_number + 1
            cleanup_list = self.archive_folders_list[0:index]
            for path in cleanup_list:
                try:
                    self.clean(path)
                    self.logger.info("Deleting: {}".format(path))
                except Exception as error:
                    self.logger.error(
                        "Unable to delete: {}, error {}".format(path, error),
                        exc_info=self.debug)
                    raise
        else:
            self.logger.info("No repository need to be cleaned up")

    def start_auto_incremental_backup(self, repository, user, password,
                                      host, threads):
        self.logger.info("*** Starting auto incremental backup ***")
        self.check_prerequisites(repository)
        self.check_repository(repository)
        if self.backup_repository is None:
            self.prepare_repository(repository)
        self.backup_list_file = os.path.join(self.backup_repository,
                                             "xtrabackup_list.txt")
        self.workdir = os.path.join(self.backup_repository, "xtratmp")
        self.incremental = filesystem_utils.has_base_backup(self.backup_repository)
        self.incremental_data = os.path.join(self.backup_repository,
                                             'pyxtrabackup-incremental')
        if self.incremental:
            self.load_incremental_data()
            self.prepare_archive_folder(self.incremental)
            self.exec_incremental_backup(user, password, threads, host)
        else:
            self.prepare_archive_folder(self.incremental)
            self.exec_full_backup(user, password, threads, host)
        self.save_incremental_data(self.incremental)
        try:
            filesystem_utils.move_file(self.workdir, self.final_archive_folder)
            self.logger.info("Archive moved")
        except Exception as error:
            self.logger.error('Unable to move archive folders: {}'.format(error),
                              exc_info=self.debug)
            raise
        try:
            filesystem_utils.append_to_file(
                self.backup_list_file,
                os.path.basename(self.final_archive_folder))
            self.logger.info("Archive folder list updated")
        except Exception as error:
            self.logger.error('Unable to update archive folder list: {}'.format(error),
                              exc_info=self.debug)
            raise
        try:
            self.clean(self.workdir)
            self.logger.info("Deleting: {}".format(self.workdir))
        except Exception as error:
            self.logger.error('Unable to clean up working dir: {}'.format(error),
                              exc_info=self.debug)
            raise
        if self.keep:
            self.cleanup_repository()
        self.logger.info("*** Finish auto incremental backup ***")
