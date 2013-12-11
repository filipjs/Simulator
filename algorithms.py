#!/usr/bin/env python
# -*- coding: utf-8 -*-

from simulation import BaseSimulator # TODO zmiana lib pozniej
from entities import Campaign


class CommonSimulator(BaseSimulator):
	"""
	"""

	def __init__(self, jobs, users, cpus):
		super(CommonSimulator, self).__init__(jobs, users, cpus)

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

	##
	## Job run time estimate algorithms
	##

	def _clairvoyance(self, job, user):
		"""
		Perfect estimate.
		"""
		return job.run_time

	def _round_up(self, job, user):
		"""
		Round up to the nearest selected time unit.
		"""
		unit = 60 * 60 # in seconds
		count = (job.run_time / unit) + 1
		return count * unit

	def _default_mode(self, job, user):
		"""
		Some predefined value, e.g. partition max time limit.
		"""
		return 60 * 60 * 24 * 7 # in seconds

	# pick one
	_get_job_estimate = _clairvoyance


class OStrichSimulator(CommonSimulator):
	"""
	"""

	def __init__(self, jobs, users, cpus):
		super(OStrichSimulator, self).__init__(jobs, users, cpus)

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

	def __init__(self, jobs, users, cpus):
		super(FairshareSimulator, self).__init__(jobs, users, cpus)

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
