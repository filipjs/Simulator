#!/usr/bin/env python
# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class BaseSelector(object):
	"""
	"""

	__metaclass__ = ABCMeta

	def __init__(self, settings):
		self._settings = settings

	@abstractmethod
	def find_campaign(self, job):
		"""
		Find and return the campaign to which the job will be added.
		Return None if this job starts a new campaign.
		"""
		raise NotImplemented


class VirtualSelector(BaseSelector):
	"""
	"""

	def __init__(self, *args):
		BaseSelector.__init__(self, *args)

	def find_campaign(self, job):
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
