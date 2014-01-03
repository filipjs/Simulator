# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

"""
Submitters:
	A submitter is used to simulate a job owner who sets the (possibly inaccurate)
	job time limit. We are however assuming that the time limit **CANNOT** be lower
	than the job run time.
	A submitter is also used to set the required number of nodes for the job.

Customizing:
	Create a new subclass of `BaseSubmitter` and override the required methods.
	To add new settings to use in your subclass see :mod: `settings` documentation.
"""


class BaseSubmitter(object):
	"""
	Submitters base class. Subclasses are required to override:

	1) _get_limit
	2) (optionally) _get_nodes

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
		limit = self._get_limit(job)
		assert job.time_limit is None, 'time limit already set'
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

	def config(self, job):
		"""
		Public wrapper method.
		Run and check the correctness of `_get_nodes`.
		"""
		nodes = self._get_nodes(job)
		assert job.nodes is None, 'nodes already set'
		assert job.pn_cpus is None, 'cpu per node already set'
		if nodes:
			pn_cpus = job.proc / nodes
			assert nodes * pn_cpus == job.proc, 'invalid configuration'
		else:
			pn_cpus = 0
		return nodes, pn_cpus

	def _get_nodes(self, job):
		"""
		Specify the number of nodes to run the job on.

		The default implementation return `0` as a sign to
		turn the feature off.

		Note:
		  **DO NOT** set the `job.nodes` or `job.pn_cpus` yourself.

		  In rare cases you can modify the `job.proc` if you feel
		  that otherwise there isn't a valid configuration.
		"""
		return 0


class OracleSubmitter(BaseSubmitter):
	"""
	A submitter that perfectly predicts the real run time.
	"""

	def _get_limit(self, job):
		return job.run_time


class DefaultTimeSubmitter(BaseSubmitter):
	"""
	Always return a predefined valued.
	Uses `_settings.default_limit` as that value.
	"""

	def _get_limit(self, job):
		return max(job.runtime, self._settings.default_limit)
