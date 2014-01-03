# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

"""
Selectors:
	A selector is used to assign each submitted job
	to an appropriate campaign or to make a decision
	to create a new campaign.

Customizing:
	Create a new subclass of `BaseSelector` and override the required methods.
	To add new settings to use in your subclass see :mod: `settings` documentation.
"""


class BaseSelector(object):
	"""
	Selectors base class. Subclasses are required to override:

	1) _get_camp

	You can access the `Settings` using `self._settings`.
	"""

	__metaclass__ = ABCMeta

	def __init__(self, settings):
		"""
		Init the class with a `Settings` instance.
		"""
		self._settings = settings

	def find_campaign(self, job):
		"""
		Public wrapper method.
		Run and check the correctness of `_get_camp`.
		"""
		camp = self._get_camp(job)
		if camp is not None:
			assert job.camp is None, \
			  'job already in some campaign'
			assert job not in camp.active_jobs, \
			  'job already in the campaign'
			assert camp not in job.user.active_camps, \
			  'not an active campaign'
		return camp

	@abstractmethod
	def _get_camp(self, job):
		"""
		Select the campaign to which the job will be added.

		Note:
		  This campaign **MUST BE** in the `user.active_camps`.
		  Also **DO NOT** add the job to any of the campaign
		    job lists yourself.

		Returns:
		  the selected campaign or `None` if the job starts
		    a new campaign.
		"""
		raise NotImplemented


class VirtualSelector(BaseSelector):
	"""
	Campaign selection based only on the virtual schedule.
	Uses `_settings.threshold` in the calculations.
	"""

	def _get_camp(self, job):
		"""
		Check the job submit time against the last campaign
		creation time extended by threshold value.
		Create a new campaign if the submit time is out of range.
		"""
		user = job.user

		if user.active_camps:
			last = user.active_camps[-1]
			if job.submit < last.created + self._settings.threshold:
				return last
		elif user.completed_camps:
			# a campaign could end without surpassing the threshold
			last = user.completed_camps[-1]
			if job.submit < last.created + self._settings.threshold:
				# move the campaign back to active ones
				user.completed_camps.pop()
				user.active_camps.append(last)
				return last
		# we need a new campaign
		return None
