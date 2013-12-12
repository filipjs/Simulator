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

		#TODO EL OH EL x 2, a moze by tak offset to time left dodac -.-
		#TODO no i trzeba podzielic przez ost_shares right? riiiiight??
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
