# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

"""
Estimators:
	An estimator is used to predict a job run time.
	Note, that this is different from the job *time limit*.

Customizing:
	Create a new subclass of `BaseEstimator` and override the required methods.
	To add new settings to use in your subclass see :mod: `settings` documentation.
"""


class BaseEstimator(object):
	"""
	Estimators base class. Subclasses are required to override:

	1) _get_initial
	2) _get_next

	You can access the `Settings` using `self._settings`.
	"""

	__metaclass__ = ABCMeta

	def __init__(self, settings):
		"""
		Init the class with a `Settings` instance.
		"""
		self._settings = settings

	def initial_estimate(self, job):
		"""
		Public wrapper method.
		Run and check the correctness of `_get_initial`.
		"""
		est = self._get_initial(job)
		assert job.estimate is None, 'estimate already set'
		assert isinstance(est, int), 'invalid estimate type'
		assert est > 0, 'invalid initial estimate'
		return est

	def next_estimate(self, job):
		"""
		Public wrapper method.
		Run and check the correctness of `_get_next`.
		"""
		prev = job.estimate
		est = self._get_next(job, prev)
		assert job.estimate == prev, 'estimate was changed'
		assert isinstance(est, int), 'invalid estimate type'
		assert est > prev, 'invalid next estimate'
		return est

	@abstractmethod
	def _get_initial(self, job):
		"""
		Estimate the run time right after the job submission.
		You can access the `job.time_limit` and `job.user`.
		At this point the job is not yet assigned to a campaign.

		Note:
		  **DO NOT** modify the `job.estimate` yourself.
		"""
		raise NotImplemented

	@abstractmethod
	def _get_next(self, job, prev_est):
		"""
		Provide an improved run time estimate, the previous
		one turned out to be too short. You can access the
		`job.time_limit`, `job.user` and `job.camp`.

		Note:
		  **DO NOT** modify the `job.estimate` yourself.
		"""
		raise NotImplemented


class NaiveEstimator(BaseEstimator):
	"""
	An estimator that does nothing.
	"""

	def _get_initial(self, job):
		"""
		Return the worst case estimate.
		"""
		return job.time_limit

	def _get_next(self, job, prev_est):
		"""
		Raise an exception since this shouldn't be called.
		"""
		raise Exception('job exceeded its time limit')


class PreviousNEstimator(BaseEstimator):
        """
	Computes the estimation as an average from N last completed jobs.

	Uses `_settings.last_completed` as N.

	(Implementation from "Backfilling Using System-Generated Predictions Rather
	than User Runtime Estimates", Tsafrir, D.; Etsion, Y.;
	Feitelson, D.G., http://dx.doi.org/10.1109/TPDS.2007.70606)
	"""

	def __init__(self, *args):
		"""
		Check the correctness of N.
		"""
		BaseEstimator.__init__(self, *args)
		assert self._settings.last_completed > 0, 'invalid last completed'

	def _get_initial(self, job):
		"""
		Set the time limit to the average of the last N jobs.
		"""
		N = self._settings.last_completed

		if len(job.user.completed_jobs) >= N:
			last_runtimes = [ j.run_time for j in job.user.completed_jobs[-N:] ]
			assert len(last_runtimes) == N, 'missing jobs'
			return sum(last_runtimes) / N
		else:
			return job.time_limit

	def _get_next(self, job, prev_est):
		"""
		Return the worst case, don't try to guess again.
		"""
		return job.time_limit
