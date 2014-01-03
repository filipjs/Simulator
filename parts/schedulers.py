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

	1) job_priority_key

	You can access the `Settings` using `self._settings`.
	"""

	__metaclass__ = ABCMeta

	def __init__(self, settings):
		"""
		Init the class with a `Settings` instance.
		"""
		self._settings = settings

	def update_stats(self, stats):
		"""
		Update the run time statistics.
		Stats consist of: cpu_used, active_shares, total_usage.
		"""
		self._stats = stats

	@abstractmethod
	def job_priority_key(self, job):
		"""
		``Key`` function for the ``list.sort`` method.
		Extract a comparison key from the job for the
		scheduler waiting list sort.

		Note:
		  Lower value corresponds to a **HIGHER** priority.
		"""
		raise NotImplemented


class OStrich(BaseScheduler):
	"""
	Default implementation of the OStrich algorithm.
	"""

	def _job_camp_index(self, job):
		"""
		This assumes that the compared jobs will be from
		the same campaign.
		Inside campaigns order by shorter run time estimate.
		In case of ties order by earlier submit.
		"""
		return (job.estimate, job.submit)

	def job_priority_key(self, job):
		"""
		Priority ordering for the scheduler:
		1) faster ending campaigns
		2) earlier created campaigns
		3) camp ID, user ID (needed to break previous ties)
		4) priority inside campaigns
		     (tied here iff jobs are from the same campaign)
		"""
		end = job.camp.time_left + job.camp.offset
		end = float(end) / job.user.shares
		# The `end` should be further multiplied by
		#   `_stats.active_shares` / `_stats.cpu_used`.
		# However, that gives the same value for all the jobs
		# and we only need the ordering, not the absolute value.
		camp_prio = self._job_camp_index(job)
		return (end, job.camp.created, job.camp.ID, job.user.ID, camp_prio)


class SlurmFairshare(BaseScheduler):
	"""
	SLURM implementation of the Fairshare algorithm.
	"""

	def job_priority_key(self, job):
		"""
		Prioritize the jobs based on the owner's account service level.
		Ties are ordered by earlier submit.

		The full formula for SLURM fairshare priority is:
		  pow(2.0, -(effective_usage / shares_norm))

		  effective_usage = my usage / global usage
		  shares_norm = my share / total shares

		The priority then is multiplied by some weight (usually around 10k-100k).
		This means, that at high over-usage all priorities are the same.
		"""
		if not self._stats['total_usage']:
			fairshare = 1
		else:
			effective = job.user.cpu_clock_used / self._stats['total_usage']
			fairshare = 2.0 ** -(effective / job.user.shares)
		prio = int(fairshare * 100000)
		return (-prio, job.submit)
