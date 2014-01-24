# -*- coding: utf-8 -*-
import logging
import os
import sys
from abc import ABCMeta, abstractmethod
from entities import Job, User


class InvalidStatsError(Exception): pass


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

	REQUIRED = ['job_id', 'user_id', 'submit', 'run_time', 'proc']
	OPTIONAL = ['nodes', 'pn_cpus']

	def parse_workload(self, filename, serial):
		"""
		Parse the file and create :mod: `entities` from the data.

		Args:
		  filename: path to some workload file.
		  serial: change parallel jobs to multiple serial ones.

		Returns:
		  a list of `Jobs` already linked to `User` instances.
		  a dictionary of created `User` instances.
		"""
		self.jobs = []
		self.users = {}
		skipped = 0

		f = open(filename)
		for i, line in enumerate(f):
			if not self._accept(line, i):
				continue

			stats = self._parse(line)
			stats = self._validate(stats)

			if stats['run_time'] <= 0 or stats['proc'] <= 0:
				skipped += 1
				continue  # skip incomplete data
			if serial:
				max_proc = min(stats['proc'], serial)
				count = stats['proc'] / max_proc
				assert count > 0, 'invalid job count'
				stats['proc'] = max_proc
			else:
				count = 1

			for i in range(count):
				self._next_job(stats)
		f.close()

		if skipped:
			logging.warn('Skipped %s incomplete job records' % skipped)
		logging.info('Parsing completed. Retrieved {} job records and {}'
			     ' user records.'.format(len(self.jobs), len(self.users)))
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
		the line requires more complex logic.
		"""
		values = map(int, line.split())
		return {name: values[field]
			for name, field in self.fields.iteritems()}

	def _validate(self, stats, ids=set()):
		"""
		Do the following:
		  1) Check if `stats` contain required values.
		  2) Check if job has unique ID.
		  3) Auto-complete optional data.

		Raises `InvalidStatsError`.
		"""
		non_negative = ['job_id', 'user_id', 'submit']
		err = None

		for name in self.REQUIRED:
			if name in stats:
				if name in non_negative and stats[name] < 0:
					err = name + 'has negative value'
				stats[name] = max(0, stats[name])
			else:
				err = name + 'value is missing'

		if err is None:
			if stats['job_id'] in ids:
				err = 'duplicate ID number'
			ids.add(stats['job_id'])

		if err is not None:
			msg = 'job {}: {}'.format(stats.get('job_id'), err)
			raise InvalidStatsError(msg)

		for name in self.OPTIONAL:
			stats[name] = max(0, stats.get(name, 0))
		return stats

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

	def _get_fields(self, line):
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
			return self._get_fields(line)
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
		'req_time': 8,
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

	fields = {} #TODO

	def _accept(self, line, num):
		return True
