# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

"""
Submitters:
	A submitter is used to simulate a job owner who sets the (possibly inaccurate)
	job time limit. Jobs with run times higher than this limit will be ended
	prematurely during simulation.

Customizing:
	Create a new subclass of `BaseSubmitter` and override the required methods.
	To add new settings to use in your subclass see :mod: `settings` documentation.
"""


class BaseSubmitter(object):
	"""
	Submitters base class. Subclasses are required to override:

	1) _get_limit

	You can access the `Settings` using `self._settings`.
	"""

	__metaclass__ = ABCMeta

	def __init__(self, settings):
		"""
		Init the class with a `Settings` instance.
		"""
		self._settings = settings

	def time_limit(self, job):
		"""
		Public wrapper method.
		Run and check the correctness of `_get_limit`.
		"""
		prev = job.time_limit
		limit = self._get_limit(job)
		assert job.time_limit == prev, 'time limit was changed'
		assert isinstance(limit, int), 'invalid time limit type'
		assert limit > 0, 'invalid time limit'
		return limit

	@abstractmethod
	def _get_limit(self, job):
		"""
		Estimate the job time limit from a user perspective.

		Note:
		  **DO NOT** set the `job.time_limit` yourself.
		"""
		raise NotImplemented


class OracleSubmitter(BaseSubmitter):
	"""
	A submitter that perfectly predicts the real run time.
	"""

	def _get_limit(self, job):
		return job.run_time


class FromWorkloadSubmitter(BaseSubmitter):
	"""
	Use the values from the workload file.
	"""

	def _get_limit(self, job):
		# Fix missing or erroneous time limits, otherwise
		# those jobs will be killed by the simulator.
		return max(job.run_time, job.time_limit)

class DefaultTimeSubmitter(BaseSubmitter):
	"""
	Always return a predefined valued.
	Uses `_settings.default_limit` as that value.
	"""

	def _get_limit(self, job):
		return self._settings.default_limit
