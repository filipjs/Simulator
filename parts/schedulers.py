# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

"""
Schedulers:
	A scheduler is used to decide about the job priority during the simulation process.

Customizing:
	Create a new subclass of `BaseScheduler` and override the required methods.
	To add new settings to use in your subclass see :mod: `settings` documentation.
"""


class BaseScheduler(object):
	"""
	Schedulers base class. Subclasses are required to override:

	1) _job_camp_key
	2) _job_priority_key

	You can access the `Settings` using `self._settings`.
	"""

	__metaclass__ = ABCMeta

	def __init__(self, settings):
		"""
		Init the class with a `Settings` instance.
		"""
		self._settings = settings

	@abstractmethod
	def _job_camp_key(self, job):
		"""
		``Key`` function for the ``list.sort`` method.
		Extract a comparison key from the job for the
		inside-campaign sort.

		Note:
		  Lower value corresponds to a **HIGHER** priority.
		"""
		raise NotImplemented

	@abstractmethod
	def _job_priority_key(self, job):
		"""
		``Key`` function for the ``list.sort`` method.
		Extract a comparison key from the job for the
		scheduler waiting list sort.

		Note:
		  Lower value corresponds to a **HIGHER** priority.
		"""
		raise NotImplemented


class OStrichScheduler(BaseScheduler):
	"""
	Default implementation of the OStrich algorithm.
	"""

	def _job_camp_key(self, job):
		"""
		Order by shorter run time estimate.
		Ties are ordered by earlier submit.
		"""
		return (job.estimate, job.submit)

	def _job_priority_key(self, job):
		"""
		Priority ordering for the scheduler:
		1) faster ending campaigns
		2) earlier created campaigns
		3) user ID (needed to break previous ties)
		4) priority inside campaigns
		"""
		end = job.camp.time_left + job.camp.offset
		end = float(end) / job.user.shares
		# The `end` should be further multiplied by
		#   `Simulator.active_shares` / `Simulator.cpu_used`.
		# However, that gives the same value for all the jobs
		# and we only need the ordering, not the absolute value.
		return (end, job.camp.created, job.user.ID, job.camp_index)


class FairshareScheduler(BaseScheduler):
	"""
	SLURM implementation of the Fairshare algorithm.
	"""

	def _job_camp_key(self, job):
		"""
		Do nothing. Not using campaigns to determine the priority.
		"""
		return job.submit

	def _job_priority_key(self, job):
		"""
		Prioritize the jobs based on the owner's account service level.
		Ties are ordered by earlier submit.
		"""
		fairshare = job.user.cpu_clock_used / job.user.shares
		# The full formula for SLURM fairshare priority is:
		#   pow(2.0, -(effective_usage / shares_norm)), where
		#     effective_usage = my usage / global usage
		#     shares_norm = my share / total shares
		# However, we are only interested in the ordering, so we can
		# skip the constant values to greatly simplify the formula.
		return (fairshare, job.submit)
