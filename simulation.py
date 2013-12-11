#!/usr/bin/env python
# -*- coding: utf-8 -*-

import heapq
import math
from abc import ABCMeta


class Events(object):
	"""
	The ordering of the events is important,
	it is used in a priority queue to break ties.
	"""
	new_job = 0
	job_end = 1
	campaign_end = 2


class PriorityQueue(object):
	"""
	A priority queue of <time, event, entity>, ordered by ascending time.
	Ties are ordered by event type.
	"""
	REMOVED = 'removed-event'

	def __init__(self):
		self._pq = []
		self._entries = {}

	def add(self, time, event, entity):
		"""
		Add an entity event to the queue.
		"""
		key = (event, entity) # must be a unique key
		if key in self._entries:
			self._remove_event(key)
		entry = [time, event, entity]
		self._entries[key] = entry
		heapq.heappush(self._pq, entry)

	def pop(self):
		"""
		Remove and return the next upcoming event.
		Raise KeyError if queue is empty.
		"""
		if not self.empty():
			time, event, entity = heapq.heappop(self._pq)
			key = (event, entity)
			del self._entries[key]
			return time, event, entity
		raise KeyError('pop from an empty priority queue')

	def empty(self):
		"""
		Check if queue is empty.
		"""
		self._pop_removed()
		return bool(self._pq)

	def _remove_event(self, key):
		"""
		Mark an existing event as removed.
		"""
		entry = self._entries.pop(key)
		entry[-1] = self.REMOVED

	def _pop_removed(self):
		"""
		Process the queue to the first non-removed event.
		"""
		while self._pq and self._pq[0][-1] == self.REMOVED:
			heapq.heappop(self._pq)


class BaseSimulator(object):
	"""
	"""

	__metaclass__ = ABCMeta

	def __init__(self, jobs, users, cpus, settings):
		self.cpu_limit = cpus
		self.cpu_used = 0
		self.future_jobs = jobs
		self.running_jobs = []
		self.waiting_jobs = []
		self.users = users
		self.total_shares = 0
		self.settings = settings

	def run(self):
		"""
		"""
		self.results = []

		self.prev_event = None
		self.pq = PriorityQueue()

		count = 0
		submits = len(self.future_jobs)

		while count < submits or not self.pq.empty():
			if count < submits:
				self.pq.add(
					self.future_jobs[count].submit,
					Events.new_job,
					self.future_jobs[count]
				)
				count += 1
			# the queue cannot be empty here
			time, event, entity = self.pq.pop()

			if self.prev_event is not None:
				self._process_period(time - self.prev_event)

			# TODO lista -> <time, utility>
			# TODO printy eventow aka job end,
			# TODO i jednak camp start??? bo utility wtedy

			if event == Events.new_job:
				self._new_job_event(entity, time)
			elif event == Events.job_end:
				self.job_end_event(entity, time)
			elif event == Events.campaign_end:
				self.camp_end_event(entity, time)
			else:
				raise Exception('unknown event')
			# update event time
			self.prev_event = time
		# return simulation results
		return self.results

	def _share_value(self, user):
		"""
		"""
		share = float(u.shares) / self.total_shares
		return share * self.cpu_used

	def _process_period(self, period):
		"""
		Distribute virtual time to active users.
		Also account real work done by all users.
		"""
		for u in self.users:
			if u.active:
				u.virtual_work(period * self._share_value(u))
			u.real_work(period)

	def _update_camp_estimates(self, time):
		"""
		Update estimated campaign end times in the virtual schedule.
		Only the first campaign is considered from each user,
		since the subsequent campaings are guaranteed to end later.
		"""
		for u in self.users:
			if u.active:
				first_camp = u.active_camps[0]
				est = first_camp.time_left / self._share_value(u)
				est = time + int(math.ceil(est))
				self.pq.add(
					est,
					Events.campaign_end,
					first_camp
				)

	def new_job_event(self, job, time):
		"""
		"""
		if not job.user.active:
			# user will be now active after this job submission
			self.total_shares += job.user.ost_shares

		camp = self._find_campaign(job, job.user)
		camp.add_job(job)
		camp.sort_jobs(key=self._job_camp_key)

		self.waiting_jobs.append(job)
		self.waiting_jobs.sort(key=self._job_priority_key)

		prev_used = self.cpu_used

		self._schedule()
		self._backfill()

		if prev_used != self.cpu_used:
			# we need to recalculate the campaign estimates
			self._update_camp_estimates(time)

	def job_end_event(self, job, time):
		"""
		"""
		job.execution_ended(time)
		# the job estimated run time could be different from the
		# real run time, so we need to redistribute any extra
		# virtual time from the mentioned difference
		job.user.virtual_work(0)

		prev_used = self.cpu_used

		# 'remove' the job from the processors
		self.running_jobs.remove(job)
		self.cpu_used -= job.proc

		self._schedule()
		self._backfill()

		if prev_used != self.cpu_used:
			# we need to recalculate the campaign estimates
			self._update_camp_estimates(time)

	def camp_end_event(self, camp, time):
		"""
		"""
		assert camp.user.active_camps[0] == camp
		assert camp.time_left == 0

		camp.user.active_camps.pop(0)
		camp.user.completed_camps.append(camp)

		if not camp.user.active:
			# user became inactive
			self.total_shares -= camp.user.ost_shares
			# we need to recalculate the campaign estimates
			self._update_camp_estimates(time)

	@abstractmethod
	def _find_campaign(self, job, user):
		"""
		Find and return the campaign to which the job will be added.
		"""
		raise NotImplemented

	@abstractmethod
	def _job_camp_key(self, job):
		"""
		Job key function for the inner campaign sort.
		"""
		raise NotImplemented

	@abstractmethod
	def _job_priority_key(self, job):
		"""
		Job key function for the scheduler waiting queue sort.
		"""
		raise NotImplemented
