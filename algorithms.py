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
		extended by threshold value.
		Create a new campaign if the submit time is out of range.
		"""
		if user.active_camps:
			last = user.active_camps[-1]
			if job.submit < last.created + self.settings.threshold:
				return last, False
		elif user.completed_camps:
			# a campaign could have ended without passing the threshold
			last = user.completed_camps[-1]
			if job.submit < last.created + self.settings.threshold:
				# move the campaign back as active
				user.completed_camps.pop()
				user.active_camps.append(last)
				return last, False
		# need a new campaign
		new_camp = Campaign(user.camp_count, user, job.submit)
		user.camp_count += 1
		user.active_camps.append(new_camp)
		return new_camp, True

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
		Ties are ordered by earlier submit.
		"""
		return (job.estimate, job.submit)

	def _job_priority_key(self, job):
		"""
		Priority ordering for the scheduler:
		1) shortest ending campaigns
		2) earliest campaigns
		3) inside campaigns use the job existing ordering
		"""
		end = job.camp.time_left + job.camp.offset
		end = float(end) / job.user.shares
		# the end should be further multiplied by (active_shares / cpu_used)
		# but that is a constant value and we are only interested in the ordering
		# and not the absolute value, so we can skip that
		return (end, job.camp.created, job.camp_index)


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
		fairshare = job.user.cpu_clock_used / job.user.shares
		# the full formula for fairshare priority is:
		# 	pow(2.0, -(usage_efctv / shares_norm))
		# effective usage = my usage / global usage
		# shares_norm = my share / total shares
		# we are however only interested in the ordering so we can
		# skip the constant values greatly simplifying the formula
		return (fairshare, job.submit)
