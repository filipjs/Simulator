#!/usr/bin/env python
# -*- coding: utf-8 -*-
import heapq
import itertools
import math
from abc import ABCMeta, abstractmethod


class Events(object):
	"""
	The ordering of the events is important,
	it is used in a priority queue to break ties.
	"""
	decay_period = 0
	new_job = 1
	job_end = 2
	campaign_end = 3


class PriorityQueue(object):
	"""
	A priority queue of <time, event, entity>, ordered by time.
	Ties are ordered by event type.
	"""
	REMOVED = 'removed-event'

	def __init__(self):
		self._pq = []
		self._entries = {}
		self._counter = itertools.count()

	def add(self, time, event, entity):
		"""
		Add an entity event to the queue.
		"""
		key = (event, entity) # must be a unique key
		if key in self._entries:
			self._remove_event(key)
		# counter prevents the comparison of entities,
		# in case the time and the event are the same
		entry = [time, event, next(self._counter), entity]
		self._entries[key] = entry
		heapq.heappush(self._pq, entry)

	def pop(self):
		"""
		Remove and return the next upcoming event.
		Raise KeyError if queue is empty.
		"""
		if not self.empty():
			time, event, _, entity = heapq.heappop(self._pq)
			key = (event, entity)
			del self._entries[key]
			return time, event, entity
		raise KeyError('pop from an empty priority queue')

	def empty(self):
		"""
		Check if the queue is empty.
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
	Base class with the simulation structure.
	Algorithm specific parts must be created in subclasses..
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
		Proceed with the simulation.
		"""
		self.results = []

		self.prev_event = None    # time of the previous event
		self.pq = PriorityQueue() # events priority queue

		# add the first decay event
		# the first job submit is the simulation 'time zero'
		self._add_next_decay(self.future_jobs[0].submit)

		count = 0
		submits = len(self.future_jobs)

		while count < submits or not self.pq.empty():
			# no need to add more than one event of this type
			if count < submits:
				self.pq.add(
					self.future_jobs[count].submit,
					Events.new_job,
					self.future_jobs[count]
				)
				count += 1
			# the queue cannot be empty here
			time, event, entity = self.pq.pop()

			# process the time skipped between events
			if self.prev_event is not None:
				self._process_period(time - self.prev_event)

			# TODO lista -> <time, utility>
			# TODO printy eventow aka job end,
			# TODO i jednak camp start??? bo utility wtedy

			# do work based on the event type
			if event == Events.new_job:
				self.new_job_event(entity, time)
			elif event == Events.job_end:
				self.job_end_event(entity, time)
			elif event == Events.campaign_end:
				self.camp_end_event(entity, time)
			elif event == Events.decay_period:
				self.decay_period_event(time)
			else:
				raise Exception('unknown event')

			# add/update events
			if event == Events.decay_period:
				# don't add the next event if the queue is empty,
				# this indicates that the simulation has ended
				if not self.pq.empty():
					self._add_next_decay(time)
			else:
				# for other events just update campaign_ends
				self._update_camp_estimates(time)

			# update event timer
			self.prev_event = time
		# return simulation results
		return self.results

	def _add_next_decay(self, time):
		"""
		Add the next decay event from now.
		"""
		self.pq.add(
			time + self.settings.decay,
			Events.decay_period,
			None # there is no entity attached
		)

	def _share_value(self, user):
		"""
		Calculate the user share of the virtual resources.
		"""
		share = float(u.shares) / self.total_shares
		return share * self.cpu_used

	def _process_period(self, period):
		"""
		Distribute virtual time to active users.
		Also account the real work done by the jobs.
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
				est = time + int(math.ceil(est)) # must be int
				self.pq.add(
					est,
					Events.campaign_end,
					first_camp
				)

	def _schedule(self):
		"""
		Try to execute the highest priority jobs from
		the waiting_jobs list.
		"""

		#sort the jobs using the defined ordering
		self.waiting_jobs.sort(key=self._job_priority_key)

		while self.waiting_jobs:
			free = self.cpu_limit - self.cpu_used
			if self.waiting_jobs[0].proc <= free:
				# execute the job
				job = self.waiting_jobs.pop(0)
				job.start_execution(#TODO TIME)
				self.cpu_used += job.proc
				self.running_jobs.append(job)

				self.pq.add(
					#TODO time + job.run_time
					Events.job_end,
					job
				)
			else:
				# only top priority can be scheduled
				break

		#TODO backfilling
		# if still left free and not empty waiting -> self._backfill()

	def new_job_event(self, job, time):
		"""
		Add the job to a campaign and do a scheduling pass.
		Update the user activity status.
		"""
		if not job.user.active:
			# user will be active after this job submission
			self.total_shares += job.user.ost_shares

		camp, fresh = self._find_campaign(job, job.user)
		camp.add_job(job)
		camp.sort_jobs(key=self._job_camp_key)

		#TODO if fresh: print <camp start event> aka utility
		self.waiting_jobs.append(job)
		self._schedule()

	def job_end_event(self, job, time):
		"""
		Free the resources and do a scheduling pass.
		"""
		job.execution_ended(time)
		# the job estimated run time could be higher than the
		# real run time, so we need to redistribute any extra
		# virtual time created by the mentioned difference
		job.user.virtual_work(0)

		# remove the job from the processors
		self.running_jobs.remove(job)
		self.cpu_used -= job.proc
		self._schedule()

	def camp_end_event(self, camp, time):
		"""
		Remove the campaign. Update the user activity status.
		"""
		assert camp.user.active_camps[0] == camp
		assert camp.time_left == 0

		camp.user.active_camps.pop(0)
		camp.user.completed_camps.append(camp)

		if not camp.user.active:
			# user became inactive
			self.total_shares -= camp.user.ost_shares

	def decay_period_event(self, time):
		"""
		Reduce the importance of the previous cpu usage.
		Add the next decay_period event.
		"""
		pass
		#TODO -> po prostu old * 1/2??

	@abstractmethod
	def _find_campaign(self, job, user):
		"""
		Find and return the campaign to which the job will be added.
		Also return True/False if that campaign was just created.
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
