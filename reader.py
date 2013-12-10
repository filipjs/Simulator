# -*- coding: utf-8 -*-
from abc import ABCMeta
from entities import Job


class BaseReader(object):
	__metaclass__ = ABCMeta

	@abstractmethod
	def parse(self, filename, serial):
		"""
		Parse a workload file and return a list of "Jobs":
			- filenamae - path to workload file
			- serial - change parallel jobs to multiple serial ones
		"""
		raise NotImplemented


class SwfReader(BaseReader):
	"""
	Field numbers in a .swf file.
	"""
	job_id = 0
	submit = 1
	wait_time = 2
	run_time = 3
	proc = 4
	user_id = 11
	partition = 15

	def parse(self, filename, serial):
		jobs = []
		for line in open(swf_file):
			if line[0] != ';': # skip comments
				stats = map(int, line.split())
				if stats[SWF.run_time] <= 0 or stats[SWF.proc] <= 0:
					continue
				if serial:
					count = stats[SWF.proc]
					stats[SWF.proc] = 1
				else:
					count = 1
				for i in range(count):
					jobs.append(
						Job({
							'id': stats[self.job_id],
							'user': stats[self.user_id],
							'proc': stats[self.proc],
							'submit': stats[self.submit],
							'run_time': stats[self.run_time],
						})
					)
		return jobs

class ICMReader(BaseReader):
	"""
	Field numbers in an ICM log file.
	"""

	def parse(self, filename, serial):
		raise NotImplemented
		#TODO
