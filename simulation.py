#!/usr/bin/env python
# -*- coding: utf-8 -*-

import heapq
import math
from abc import ABCMeta
from entities import Job, Campaign, User


class Events(object):
	new_job = 0
	job_end = 1
	new_campaign = 2
	campaign_end = 3


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
				#TODO run job if free cpu + backfill?
			elif event == Events.job_end:
				self.job_end_event(entity, time, last_time)
				#TODO run job if in queue + backfill
			elif event == Events.new_campaign:
				#TODO zmienic event na new active user?
				self.new_camp_event(entity, time, last_time)
			elif event == Events.campaign_end:
				#TODO zmienic event na remove active user?
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

	def _distribute_virtual(self, period):
		""" Distribute virtual time shares to active users.
		"""
		for u in self.users:
			if u.active:
				u.virtual_work(period * self._share_value(u))

	def _update_camp_events(self, time):
		"""
		"""
		for u in self.users:
			if u.active:
				first_camp = u._active_camps[0]
				est = first_camp.time_left / self._share_value(u)
				est = time + int(math.ceil(est))
				self.pq.add(
					est,
					Events.campaign_end.
					first_camp
				)

	def new_job_event(self, job, time, last_time):
		"""
		"""
		self._distribute_virtual(time - last_time)

		user = self.users[job.userID]
		if not user.active:
			# user is active after this job submission
			self.total_shares += user.shares

		camp = self._add_new_job(user, job)
		camp.sort_jobs(self._camp_job_cmp)

		self.waiting_jobs.append(job)
		self.waiting_jobs.sort(key=self._priority_job_key)

		self._schedule()
		self._backfill()

		# this can be done only after scheduling the new job,
		# becuase it can change the number of cpus used
		self._update_camp_events(time)

	@abstractmethod
	def job_end_event(self, job, time, last_time):
		raise NotImplemented
	@abstractmethod
	def new_camp_event(self, camp, time, last_time):
		raise NotImplemented
	@abstractmethod
	def camp_end_event(self, camp, time, last_time):
		raise NotImplemented

	@abstractmethod
	def _add_new_job(self, user, job):
		"""
		Add the job to an appropriate user campaign.
		Return: the campaign to which the job was added.
		"""
		raise NotImplemented
	@abstractmethod
	def _camp_job_key(self, job):
		""" Job key function for the inner campaign sort.
		"""
		raise NotImplemented

	def _priority_job_key(self, job):
		"""
		"""
		return (job.camp.time_left, job.camp.created, job.camp_index)
