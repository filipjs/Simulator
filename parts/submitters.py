# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

"""
Submitters:
	A submitter is used to simulate a job owner who sets the (possibly inaccurate)
	job time limit. We are however assuming that the time limit **CANNOT** be lower
	than the job real run time.
	A submitter is also used to set the required number of nodes for the job.

Customizing:
	Create a new subclass of `BaseSubmitter` and override the required methods.
	To add new settings to use in your subclass see :mod: `settings` documentation.
"""


class BaseSubmitter(object):
	"""
	Submitters base class. Subclasses are required to override:

	1) _get_limit
	2) (optionally) modify_configuration

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
		assert limit >= job.run_time, 'invalid time limit'
		return limit

	@abstractmethod
	def _get_limit(self, job):
		"""
		Estimate the job time limit from a user perspective.

		Note:
		  **DO NOT** set the `job.time_limit` yourself.
		"""
		raise NotImplemented

	def modify_configuration(self, job):
		"""
		Override this if you want to change the job node configuration.

		For example you do not want to alter your workload file but still
		want to use this feature and you have prepared some rules to
		determine the `nodes` and/or the `pn_cpus` values.

		Note:
		  You **HAVE TO** to modify the job attributes `job.nodes`
		  and/or `job.pn_cpus` yourself.
		"""
		pass


class OracleSubmitter(BaseSubmitter):
	"""
	A submitter that perfectly predicts the real run time.
	"""

	def _get_limit(self, job):
		return job.run_time


class FromWorkloadSubmitter(BaseSubmitter):
	"""
	Use the value supplied in the workload file.
	"""

	def _get_limit(self, job):
		return job.time_limit


class DefaultTimeSubmitter(BaseSubmitter):
	"""
	Always return a predefined valued.
	Uses `_settings.default_limit` as that value.
	"""

	def _get_limit(self, job):
		return max(job.run_time, self._settings.default_limit)
