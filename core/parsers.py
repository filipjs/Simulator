# -*- coding: utf-8 -*-
import logging
import os
import sys
import time
from datetime import timedelta
from abc import ABCMeta, abstractmethod
from entities import Job, User


def get_parser(filename):
    """
    Return a parser based on the file extension.
    """
    ext = os.path.splitext(filename)[1]
    if ext == '.swf':
        p = SWFParser()
    elif ext == '.icm':
        p = ICMParser()
    else:
        logging.warn('No parser found for the extension %s' % ext)
        p = DefaultParser()
    logging.info('Using the %s' % p)
    return p


class BaseParser(object):
    """
    Base class for reading and parsing different workload files.
    """

    __metaclass__ = ABCMeta

    REQUIRED = ['job_id', 'submit', 'run_time', 'proc', 'user_id']
    OPTIONAL = ['time_limit']

    def parse_workload(self, filename, serial):
        """
        Parse the file and create :mod: `entities` from the data.

        Args:
          filename: path to a workload file.
          serial: limit the number of CPUs to this value.

        Returns:
          a list of `Jobs` already linked to `User` instances.
          a dictionary of created `User` instances.
        """
        self.jobs = []
        self.users = {}

        self._missing = 0
        self._invalid = 0
        self._serialized = 0

        f = open(filename)
        for i, line in enumerate(f):
            if not line.strip():
                continue
            if not self._accept(line, i):
                continue

            stats = self._parse(line)

            if not self._validate(stats):
                continue

            if serial:
                max_proc = min(stats['proc'], serial)
                count = stats['proc'] / max_proc
                stats['proc'] = max_proc
            else:
                count = 1

            assert count > 0, 'invalid job count'
            if count > 1:
                self._serialized += (count - 1)

            for i in range(count):
                self._next_job(stats)
        f.close()

        if self._missing:
            logging.warn('Skipped %s incomplete job records' % self._missing)
        if self._invalid:
            logging.warn('Skipped %s invalid job records' % self._invalid)
        logging.info('Parsing completed:')
        logging.info('    Retrieved {} job records and {} user records'.format(
                     len(self.jobs) - self._serialized, len(self.users)))
        if self._serialized:
            logging.info('    Serialization added {} extra jobs.'.format(
                         self._serialized))
        return self.jobs, self.users

    def _next_job(self, stats):
        """
        Add the job to the result list.
        Replace the user id with a `User` instance.
        """
        uid = stats['user_id']
        if uid not in self.users:
            self.users[uid] = User(uid)
        self.jobs.append( Job(stats, self.users[uid]) )

    def _parse(self, line):
        """
        Default implementation, requires a `self.fields`.

        Create `stats` from the line by splitting and extracting
        the appropriate values.

        You can override this method in a subclass if parsing
        the line requires a more complex logic.

        Returned values **MUST** be integer type.
        """
        values = map(float, line.split())
        values = map(int, values)
        return {name: values[field]
                for name, field in self.fields.iteritems()}

    def _validate(self, stats, ids=set()):
        """
        Do the following:
          1) Check if `stats` contain required values.
          2) Check if the job has an unique ID.
          3) Fill in missing optional data.
        """
        non_zero = ['run_time', 'proc']

        for name in self.REQUIRED:
            if name in stats:
                min_val = 1 if name in non_zero else 0
                if stats[name] < min_val:
                    self._invalid += 1
                    return False
            else:
                self._missing += 1
                return False

        if stats['job_id'] in ids:
            self._invalid += 1
            return False
        ids.add(stats['job_id'])

        for name in self.OPTIONAL:
            if name in stats:
                stats[name] = max(stats[name], 0)
            else:
                stats[name] = 0
        return True

    @abstractmethod
    def _accept(self, line, num):
        """
        Return if this line should be parsed.
        """
        raise NotImplemented

    def __str__(self):
        return self.__class__.__name__


class DefaultParser(BaseParser):
    """
    A default parser for files without a recognized extension.

    Algorithm:
      1) Try to read field numbers from the first line of the file.
      2) Generate field numbers based on the amount of data,
         following the ordering in self.REQUIRED.
    """

    def _prepare_fields(self, line):
        if line[0] == '{':
            # part 1)
            from ast import literal_eval
            self.fields = literal_eval(line)
            return False  # line is consumed
        else:
            # part 2)
            names = self.REQUIRED + self.OPTIONAL
            data_count = len(line.split())
            # take minimum
            field_count = min(len(names), data_count)

            self.fields = {names[i]: i
                           for i in range(field_count)}
            return True  # line with actual data

    def _accept(self, line, num):
        if num == 0:
            return self._prepare_fields(line)
        return True


class SWFParser(BaseParser):
    """
    Parser for the .swf workload files.
    """

    fields = {
        'job_id': 0,
        'submit': 1,
        'wait_time': 2,
        'run_time': 3,
        'proc': 4,
        'time_limit': 8,
        'user_id': 11,
        'partition': 15
    }

    def _accept(self, line, num):
        if line[0] == ';':
            return False  # skip comments
        return True


class ICMParser(BaseParser):
    """
    Parser for the ICM workload dump.
    """

    fields = {
        'job_id': 0,
        'user_id': 2,
        'submit': 4,
        'run_time': 7,
        'time_limit': 8,
        'proc': 11
    }

    def _accept(self, line, num):
        if len(line.split('|')) > 20:
            return True
        return False

    def _from_time_str(self, time_str):
        return time.mktime(time.strptime(time_str, '%Y-%m-%dT%H:%M:%S'))

    def _from_delta_str(self, delta_str):
        if '-' in delta_str:
            days, rest = delta_str.split('-')
            days = int(days)
        else:
            rest = delta_str
            days = 0
        t = map(int, rest.split(':'))
        delta = timedelta(days=days, hours=t[0], minutes=t[1], seconds=t[2])
        return delta.total_seconds()

    def _parse(self, line, uids={}):
        """
        Custom logic to parse ICM database extract.
        """
        stats = line.split('|')
        stats = {name: stats[field]
                 for name, field in self.fields.iteritems()}

        if stats['user_id'] not in uids:
            uids[stats['user_id']] = len(uids) + 1
        stats['user_id'] = uids[stats['user_id']]
        stats['submit'] = self._from_time_str(stats['submit'])
        stats['run_time'] = self._from_delta_str(stats['run_time'])
        stats['time_limit'] = self._from_delta_str(stats['time_limit'])

        for name in stats:
            stats[name] = int(stats[name])
        return stats
