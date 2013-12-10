#!/usr/bin/env python
# -*- coding: utf-8 -*-

import heapq
import math
from abc import ABCMeta
from entities import Job, Campaign, User


class Events(object):
	"""
	Ordering of events is important, it is used
	in priority queue to break ties.
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
		key = (event, entity) # must be an unique key
		if key in self._entries:
			self._remove_event(key)
		entry = [time, event, entity]
		self._entries[key] = entry
		heapq.heappush(self._pq, entry)
	def pop(self):
		"""
		Remove and return the next upcoming event.
		Return 'None' if queue is empty.
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

	def __init__(self, jobs, users, cpus):
		self.cpu_limit = cpus
		self.cpu_used = 0
		self.future_jobs = jobs
		self.running_jobs = []
		self.waiting_jobs = []
		self.users = users
		self.total_shares = 0

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

			# if it is the first event then last_time = time
			last_time = self.prev_event or time

			if event == Events.new_job:
				self._new_job_event(entity, time, last_time)
			elif event == Events.job_end:
				self.job_end_event(entity, time, last_time)
			elif event == Events.campaign_end:
				self.camp_end_event(entity, time, last_time)
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

	def _update_camp_events(self, time):
		"""
		Update estimated end times of campaigns.
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

	def new_job_event(self, job, time, last_time):
		"""
		"""
		self._process_period(time - last_time)

		if not job.user.active:
			# user will be now active after this job submission
			self.total_shares += user.shares

		camp = self._find_campaign(job.user, job)
		camp.add_job(job)
		camp.sort_jobs(key=self._job_camp_key)

		self.waiting_jobs.append(job)
		self.waiting_jobs.sort(key=self._job_priority_key)

		self._schedule()
		self._backfill()

		# this can be done only after a scheduling pass,
		# because the new job can change the number of cpus used
		# and the following estimates would be inaccurate
		self._update_camp_events(time)

	def job_end_event(self, job, time, last_time):
		"""
		"""
		self._process_period(time - last_time)

		job.execution_ended(time)
		# the job estimated run time could be different from the
		# real run time, so we need to redistribute any extra
		# virtual time from the mentioned difference
		job.user.virtual_work(0)

		# 'remove' the job from the processors
		self.running_jobs.remove(job)
		self.cpu_used -= job.proc

		self._schedule()
		self._backfill()

		# this can be done only after a scheduling pass,
		# because the new job can change the number of cpus used
		# and the following estimates would be inaccurate
		self._update_camp_events(time)

	def camp_end_event(self, camp, time, last_time):
		raise NotImplemented
		#TODO zmienic event na remove active user?
	@abstractmethod
	def _find_campaign(self, user, job):
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
		return (job.camp.time_left, job.camp.created, job.camp_index)
