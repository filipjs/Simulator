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
		self._stats = None

	def set_stats(self, stats):
		"""
		Supply a simple class which links to the simulator
		run time statistics.

		Stats consist of:
		  cpu_used, active_shares, total_usage.
		"""
		self._stats = stats

	def clear_stats(self):
		"""
		Remove the statistics after the simulation is over.
		"""
		self._stats = None

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

	def __str__(self):
		return self.__class__.__name__


class OStrich(BaseScheduler):
	"""
	Default implementation of the OStrich algorithm.
	"""

	def job_priority_key(self, job):
		"""
		Priority ordering for the scheduler:
		1) faster ending campaigns
		2) earlier created campaigns
		3) camp ID, user ID (needed to break previous ties)
		4) priority inside campaigns
		     (tied here iff jobs are from the same campaign)

		Inside campaigns order by shorter run time estimate.
		In case of ties order by earlier submit.
		"""
		camp, user = job.camp, job.user
		end = camp.time_left / user.shares
		# The `end` should be further multiplied by
		#   `_stats.active_shares` / `_stats.cpu_used`.
		# However, that gives the same value for all the jobs
		# and we only need the ordering, not the absolute value.
		return (end, camp.created, camp.ID, user.ID,
			job.estimate, job.submit, job.ID)

class Fairshare(BaseScheduler):
	"""
	SLURM implementation of the fairshare algorithm.
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
		if not self._stats.total_usage:
			fairshare = 1
		else:
			user = job.user
			effective = user.cpu_clock_used / self._stats.total_usage
			shares_norm = user.shares  # already normalized
			fairshare = 2.0 ** -(effective / shares_norm)
		prio = int(fairshare * 100000)  # higher value -> higher priority
		return (-prio, job.submit, job.ID)
