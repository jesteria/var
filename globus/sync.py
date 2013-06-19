import collections
import datetime
import os
import sqlite3
import stat
import time

from globusonline.transfer import api_client


DB_NAME = 'sync.db'
TABLE_NAME_FILES = 'files'
TABLE_NAME_DIRS = 'dirs'

SYNC_LEVEL = 3


def watch(username, token, local_name, local_path,
          remote_name, remote_path, wait):
    """Sync `local_path` on `local_name` with `remote_path` on `remote_name`
    every `wait` seconds, using the Globus API.

    """
    while True:
        perform_sync(username, token, local_name, local_path,
                     remote_name, remote_path)
        time.sleep(wait)


def perform_sync(username, token, local_name, local_path,
                 remote_name, remote_path, timeout=120):
    """Sync `local_path` on `local_name` with `remote_path` on `remote_name`,
    using the Globus API.

    """
    # Initialize local record of filesystem:
    connection = FSTable.get_connection(DB_NAME)
    files = Files(connection, TABLE_NAME_FILES)
    dirs = Dirs(connection, TABLE_NAME_DIRS)

    # Initialize API client:
    api = api_client.TransferAPIClient(username, goauth=token)
    api.endpoint_autoactivate(local_name)
    api.endpoint_autoactivate(remote_name)

    # Initialize Transfer and Delete sync lists:
    deadline = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout)
    _code, _message, data = api.submission_id()
    xfer_submission_id = data['value']
    xfer = api_client.Transfer(
        xfer_submission_id, local_name, remote_name, deadline,
        sync_level=SYNC_LEVEL, verify_checksum=True
    )

    _code, _message, data = api.submission_id()
    del_submission_id = data['value']
    delete = api_client.Delete(del_submission_id, remote_name, deadline,
                               recursive=True)

    new_dirpaths = [] # New directories to be added recursively
    abspath = os.path.abspath(local_path)
    for dirpath, dirnames, filenames in os.walk(abspath):
        recursively_included = any(is_ancestor(new_dirpath, dirpath)
                                   for new_dirpath in new_dirpaths)

        # Check for directories removed from this level:
        for expected_dir in dirs.under(dirpath):
            if os.path.basename(expected_dir) not in dirnames:
                # Directory removed. Recursively delete:
                dirs.delete(expected_dir, recursive=True)
                files.delete(expected_dir, recursive=True)
                remote_dirpath = get_remote_path(
                    remote_path, abspath, expected_dir)
                delete.add_item(remote_dirpath)

        # Check for directories added to this level:
        for dirname in dirnames:
            dirname_path = os.path.join(dirpath, dirname)
            try:
                dirs.get(dirname_path)
            except LookupError:
                # New dir. Just use recursive xfer feature:
                if not recursively_included:
                    remote_dirpath = get_remote_path(
                        remote_path, abspath, dirname_path)
                    xfer.add_item(dirname_path, remote_dirpath, recursive=True)
                    # Record recursive xfer for this walk:
                    new_dirpaths.append(dirname_path)
                # Record directory for future walks:
                dirs.add(dirname_path)

        # Check for files removed from this level:
        for expected_file in files.under(dirpath):
            if os.path.basename(expected_file.path) not in filenames:
                # File removed.
                files.delete(expected_file.path)
                remote_filepath = get_remote_path(
                    remote_path, abspath, expected_file.path)
                delete.add_item(remote_filepath)

        # Check for files added to or updated at this level:
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            norm_filepath = filepath.rstrip(os.path.sep)
            timestamp = os.lstat(filepath)[stat.ST_MTIME]
            try:
                file_ = files.get(norm_filepath)
            except LookupError:
                # File unknown. Record before xfer:
                files.add(norm_filepath, timestamp)
            else:
                if timestamp == file_.modified:
                    # File unmodified since last check. Skip xfer:
                    continue

                # File modified. Update record before xfer:
                files.update(file_, modified=timestamp)

            if not recursively_included:
                # Add file to transfer:
                remote_filepath = get_remote_path(
                    remote_path, abspath, filepath)
                xfer.add_item(filepath, remote_filepath)

    # Request deletes:
    if delete.items:
        _code, _reason, data = api.delete(delete)
        delete_id = data['task_id']
    else:
        delete_id = None

    # Request transfers:
    if xfer.items:
        _code, _reason, data = api.transfer(xfer)
        xfer_id = data['task_id']
    else:
        xfer_id = None

    xfer_complete = not xfer_id or wait_for_task(api, xfer_id)
    delete_complete = not delete_id or wait_for_task(api, delete_id)
    if xfer_complete and delete_complete:
        connection.commit() # Commit local record
    else:
        print("Some tasks did not complete")

    connection.close()


def wait_for_task(api, task_id, timeout=(2*60), step=1.5):
    """Check the status of a requested task until `timeout` is reached or
    status is no longer reported as "ACTIVE".

    Returns True if task is no longer reported as "ACTIVE".

    """
    status = 'ACTIVE'
    while timeout > 0 and status == 'ACTIVE':
        _code, _reason, data = api.task(task_id, fields="status")
        status = data['status']
        time.sleep(step)
        timeout -= step
    return status != 'ACTIVE'


def get_remote_path(remote_base, local_base, local_path):
    """For a given base destination path, a base origin path, and absolute
    file path, return the full appropriate destination path.

    """
    norm_local_base = os.path.join(local_base, '')
    if os.path.isdir(local_path):
        norm_path = os.path.join(local_path, '')
    else:
        norm_path = local_path
    remote_path = norm_path.replace(norm_local_base, '', 1)
    return os.path.join(remote_base, remote_path)


def is_ancestor(parent, child):
    """Determine whether `parent` directory path contains `child`
    (at any level).

    """
    # Ensure parent ends with separator (to avoid similarly named siblings):
    normalized_parent = os.path.join(parent, '')
    return child.startswith(normalized_parent)


class FSTable(object):
    """Abstract model of filesystem paths in a SQLite database."""
    @classmethod
    def make_obj(cls, result):
        """Map db query result to Python object appropriate to model."""
        raise NotImplementedError

    @staticmethod
    def _get_len(path):
        return len(path.strip(os.path.sep).split(os.path.sep))

    @staticmethod
    def get_connection(db_name):
        return sqlite3.connect(db_name)

    @classmethod
    def connect(cls, db_name, table_name):
        return cls(self.get_connection(db_name), table_name)

    def __init__(self, connection, table_name):
        self.connection = connection
        self.table_name = table_name

    def close(self):
        self.connection.close()

    def execute(self, statement):
        cursor = self.connection.cursor()
        cursor.execute(statement)
        return cursor

    def _add(self, *values):
        values = 'NULL,' + ','.join(
            "'{0}'".format(val) if isinstance(val, basestring) else str(val)
            for val in values
        )
        self.execute(
            "INSERT INTO {table} VALUES ({values});".format(
                table=self.table_name,
                values=values,
            )
        )

    def query(self, statement):
        cursor = self.execute(statement)
        results = cursor.fetchall()
        return (self.make_obj(result) for result in results)

    def filter(self, *paths):
        return self.query(
            "SELECT * FROM {table} WHERE path IN ({paths});".format(
                table=self.table_name,
                paths=','.join("'{0}'".format(path) for path in paths),
            )
        )

    def get(self, path):
        try:
            return self.filter(path).next()
        except StopIteration:
            raise LookupError("No node found at '{path}'".format(path=path))

    def under(self, parent):
        return self.query(
            "SELECT * FROM {table} "
            "WHERE path GLOB '{expression}' AND length={length}"
            .format(
                table=self.table_name,
                expression=os.path.join(parent, '*'),
                length=(self._get_len(parent) + 1)
            )
        )

    def delete(self, path, recursive=False):
        where = "path='{0}'".format(path)
        if recursive:
            where += " OR path GLOB '{0}'".format(os.path.join(path, '*'))
        self.execute(
            "DELETE FROM {table} WHERE {where}".format(
                table=self.table_name,
                where=where,
            )
        )


class Dirs(FSTable):

    @classmethod
    def make_obj(cls, result):
        _id, path, _length = result
        return path

    def add(self, path):
        self._add(path, self._get_len(path))


class Files(FSTable):

    File = collections.namedtuple('File', 'id, path, length, modified')

    @classmethod
    def make_obj(cls, result):
        return cls.File._make(result)

    def add(self, path, modified):
        self._add(path, self._get_len(path), modified)

    def update(self, file_, path=None, modified=None):
        path = file_.path if path is None else path
        modified = file_.modified if modified is None else modified
        length = self._get_len(path)
        self.execute(
            "UPDATE {table} "
            "SET path='{path}', length={length}, modified={modified} "
            "WHERE id={id};"
            .format(
                table=self.table_name,
                path=path,
                length=length,
                modified=modified,
                id=file_.id,
            )
        )
        return file_._replace(path=path, length=length, modified=modified)
