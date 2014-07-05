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

	Note:
	  You can also change the value of `only_virtual` or `only_real`
	  if your scheduler is using only the information about virtual campaigns
	  or the user/system CPU usage, respectively.
	  This can result in about 10%-30% faster simulation.

	"""

	__metaclass__ = ABCMeta

	only_virtual = False
	only_real = False

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
		Create a comparison key from the job for the
		scheduler to use in sorting pending jobs.

		Note:
		  Lower value corresponds to a **HIGHER** priority.
		"""
		raise NotImplemented

	def __str__(self):
		return self.__class__.__name__


class OStrich(BaseScheduler):
	"""
	Implementation of the OStrich algorithm.
	"""

	only_virtual = True

	def job_priority_key(self, job):
		"""
		Priority ordering for the scheduler:
		1) earlier ending campaigns
		2) earlier created campaigns
		3) user ID, camp ID (needed to break previous ties)
		4) priority inside campaigns
		     (ties here iff jobs are from the same campaign)

		Inside campaigns order by shorter run time estimate.
		In case of ties order by earlier submit.
		"""
		camp, user = job.camp, job.user
		end = camp.time_left / user.shares  # lower value -> higher priority
		# The `end` should be further multiplied by
		#   `_stats.active_shares` / `_stats.cpu_used`.
		# However, that gives the same value for all the jobs
		# and we only need the ordering, not the absolute value.
		return (end, camp.created, user.ID, camp.ID,
			job.estimate, job.submit, job.ID)


class Fairshare(BaseScheduler):
	"""
	Implementation of the SLURM multifactor plugin.

	Currently only supports the "fairshare" component.
	"""

	only_real = True

	def job_priority_key(self, job):
		"""
		Prioritize the jobs based on the owner's account service level.
		Ties are ordered by earlier submit.

		The full formula for SLURM fairshare priority is:
		  pow(2.0, -(effective_usage / shares_norm))

		  effective_usage = my usage / global usage
		  shares_norm = my share / total shares

		The priority then is multiplied by some weight (usually around 10k-100k).

		Note:
		  Starting from SLURM 14.xx there is also a FairShareDampeningFactor
		  which if needed could be implemented here.
		  The new formula is as follows:
		    pow(2.0, -((usage_efctv / shares_norm) / damp_factor))
		"""
		if not self._stats.total_usage:
			fairshare = 1
		else:
			user = job.user
			effective = user.cpu_clock_used / self._stats.total_usage
			shares_norm = user.shares  # already normalized
			fairshare = 2.0 ** -(effective / shares_norm)
		prio = int(fairshare * 100000)  # higher value -> higher priority
		# TODO if needed change the constant to a configuration setting
		# TODO and add more components to the priority value
		return (-prio, job.submit, job.ID)
