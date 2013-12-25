# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
import os
import sys
from entities import Job, User


def get_parser(filename):
	"""
	Return a parser based on the file extension.
	"""
	ext = os.path.splitext(filename)[1]
	if ext == '.swf':
		return SWFParser()
	elif ext == '.sql':
		return ICMParser()
	else:
		print "ERROR: No parser found for the extension", ext
		sys.exit(1)


class BaseParser(object):
	"""
	Base class for reading and parsing different workload files.
	"""

	__metaclass__ = ABCMeta

	def parse_workload(self, filename, serial):
		"""
		A wrapper around the main parse method overloaded in subclasses.

		Args:
		  filename: path to some workload file.
		  serial: change parallel jobs to multiple serial ones.

		Returns:
		  a list of `Jobs` already linked to `User` instances.
		  a dictionary of created `User` instances.
		"""
		self.jobs = []
		self.users = {}

		f = open(filename)
		for line in f:
			stats = self._parse(line)
			if stats is not None:
				if stats['run_time'] <= 0 or stats['proc'] <= 0:
					continue  # skip incomplete data
				if serial:
					count = stats['proc']
					stats['proc'] = 1
				else:
					count = 1
				for i in range(count):
					self._next_job(stats)
		f.close()
		return self.jobs, self.users

	def _next_job(self, stats):
		"""
		Add the job to the result list.
		Replace the user id with a `User` instance.
		"""
		uid = stats['user']
		if uid not in self.users:
			self.users[uid] = User(uid)
		self.jobs.append( Job(stats, self.users[uid]) )

	@abstractmethod
	def _parse(self, line):
		"""
		Parse a single line from the workload file.

		Return a dictionary with job statistics or `None`
		if that line did not contain job information.
		"""
		raise NotImplemented


class SWFParser(BaseParser):
	"""
	Parser for the .swf workload files.
	"""

	# field numbers
	job_id = 0
	submit = 1
	wait_time = 2
	run_time = 3
	proc = 4
	user_id = 11
	partition = 15

	def _parse(self, line):
		if line[0] == ';':
			return None  # skip comments
		stats = map(int, line.split())
		return {
			'id': stats[self.job_id],
			'user': stats[self.user_id],
			'proc': stats[self.proc],
			'submit': stats[self.submit],
			'run_time': stats[self.run_time],
		}


class ICMParser(BaseParser):
	"""
	Parser for the ICM workload dump.
	"""

	# field numbers

	def _parse(self, line):
		raise NotImplemented
		#TODO
