#!/usr/bin/env python
# -*- coding: utf-8 -*-

from simulation import BaseSimulator # TODO zmiana lib pozniej
from entities import Campaign


class OStrichSimulator(BaseSimulator):
	"""
	"""

	CAMP_THRESHOLD = 10 * 60

	def __init__(self, jobs, users, cpus):
		BaseSimulator.__init__(self, jobs, users, cpus)

	def _find_campaign(self, user, job):
		"""
		Check job submit time against the last campaign creation time
		extended by `camp_threshold` value.
		Create new one if the submit time is out of range.
		"""
		if user.active_camps:
			last = user.active_camps[-1]
			if job.submit < last.created + self.CAMP_THRESHOLD:
				return last
		elif user.completed_camps:
			# a campaign could end without passing the threshold value
			last = user.completed_camps[-1]
			if job.submit < last.created + self.CAMP_THRESHOLD:
				user.completed_camps.pop()
				user.active_camps.append(last)
				return last
		# need a new campaign
		next_id = len(user.active_camps) + len(user.completed_camps) + 1
		return Campaign(next_id, user, job.submit)

	def _job_camp_key(self, job):
		"""
		Order by shortest run time estimate.
		"""
		return (job.estimate, job.submit)

	def _job_priority_key(self, job):
		"""
		Priority ordering for the scheduler:
		1) shortest campaigns first
		2) inside campaigns use their default ordering
		"""
		return (job.camp.time_left, job.camp.created, job.camp_index)
