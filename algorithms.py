#!/usr/bin/env python
# -*- coding: utf-8 -*-

from simulation import BaseSimulator # TODO zmiana lib pozniej
from entities import Campaign


class CommonSimulator(BaseSimulator):
	"""
	"""

	def __init__(self, *args):
		super(CommonSimulator, self).__init__(*args)

	##
	## Campaign selection algorithms
	##

	def _virtual_based_campaigns(self, job, user):
		"""
		Check the job submit time against the last campaign creation time
		extended by `camp_threshold` value.
		Create a new campaign if the submit time is out of range.
		"""
		CAMP_THRESHOLD = 10 * 60 # in seconds

		if user.active_camps:
			last = user.active_camps[-1]
			if job.submit < last.created + self.CAMP_THRESHOLD:
				return last
		elif user.completed_camps:
			# a campaign could end without passing the threshold value
			last = user.completed_camps[-1]
			if job.submit < last.created + self.CAMP_THRESHOLD:
				# move the campaign back to active
				user.completed_camps.pop()
				user.active_camps.append(last)
				return last
		# need a new campaign
		next_id = len(user.active_camps) + len(user.completed_camps) + 1
		new_camp = Campaign(next_id, user, job.submit)
		user.active_camps.append(new_camp)
		return new_camp

	# pick one
	_find_campaign = _virtual_based_campaigns


class OStrichSimulator(CommonSimulator):
	"""
	"""

	def __init__(self, *args):
		super(OStrichSimulator, self).__init__(*args)

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


class FairshareSimulator(CommonSimulator):
	"""
	"""

	def __init__(self, *args):
		super(FairshareSimulator, self).__init__(*args)

	def _job_camp_key(self, job):
		"""
		Do nothing.
		"""
		return job.ID

	def _job_priority_key(self, job):
		"""
		Order by the most under-serviced user account.
		"""
		#TODO some value of job.user._cpu_clock / job.user.fair_shares
		return None
