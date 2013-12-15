#!/usr/bin/env python
# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class BaseSelector(object):
	"""
	"""

	__metaclass__ = ABCMeta

	@abstractmethod
	def find_campaign(self, job, settings):
		"""
		Find and return the campaign to which the job will be added.
		Return None if this job starts a new campaign.
		#TODO IN:OUT
		"""
		raise NotImplemented


class VirtualSelector(BaseSelector):
	"""
	"""

	def find_campaign(self, job, settings):
		"""
		Check the job submit time against the last campaign
		creation time extended by threshold value.
		Create a new campaign if the submit time is out of range.
		"""
		user = job.user

		if user.active_camps:
			last = user.active_camps[-1]
			if job.submit < last.created + settings.threshold:
				return last
		elif user.completed_camps:
			# a campaign could end without surpassing the threshold
			last = user.completed_camps[-1]
			if job.submit < last.created + settings.threshold:
				# move the campaign back to active ones
				user.completed_camps.pop()
				user.active_camps.append(last)
				return last
		# we need a new campaign
		return None
