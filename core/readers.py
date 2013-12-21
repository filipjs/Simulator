#!/usr/bin/env python
# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from entities import Job, User


class BaseReader(object):
	"""
	Base class for reading and parsing different workload files.
	"""

	__metaclass__ = ABCMeta

	def parse_workload(self, filename, serial):
		"""
		A wrapper around the main parse method overloaded in subclasses.
		IN:
		- filename - path to some workload file
		- serial - change parallel jobs to multiple serial ones
		OUT:
		- a list of `Jobs` already linked to `User` instances
		- a dictionary of created `User` instances
		"""
		self.jobs = []
		self.users = {}
		self._parse(filename, serial)
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
	def _parse(self, filename, serial):
		"""
		Parse a specific type of a workload file.
		"""
		raise NotImplemented


class SWFReader(BaseReader):
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

	def _parse(self, filename, serial):
		for line in open(filename):
			if line[0] == ';':
				continue # skip comments
			stats = map(int, line.split())
			if stats[SWF.run_time] <= 0 or stats[SWF.proc] <= 0:
				continue # skip incomplete data
			if serial:
				count = stats[SWF.proc]
				stats[SWF.proc] = 1
			else:
				count = 1
			for i in range(count):
				BaseReader.next_job(self, {
					'id': stats[self.job_id],
					'user': stats[self.user_id],
					'proc': stats[self.proc],
					'submit': stats[self.submit],
					'run_time': stats[self.run_time],
				})


class ICMReader(BaseReader):
	"""
	Parser for the ICM workload dump.
	"""

	# field numbers
	def _parse(self, filename, serial):
		raise NotImplemented
		#TODO
