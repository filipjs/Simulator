#!/usr/bin/env python
# -*- coding: utf-8 -*-
import heapq
import itertools
import math


class Events(object):
	"""
	The ordering of the events is important since
	it is used in a priority queue to break ties.
	"""
	new_job = 1
	job_end = 2
	campaign_end = 3
	force_decay = 4


class PriorityQueue(object):
	"""
	A priority queue of <time, event, entity>, ordered by time.
	Ties are ordered by the event type.
	"""
	_REMOVED = 'removed-event'

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
		entry[-1] = self._REMOVED

	def _pop_removed(self):
		"""
		Process the queue to the first non-removed event.
		"""
		while self._pq and self._pq[0][-1] == self._REMOVED:
			heapq.heappop(self._pq)


class Simulator(object):
	"""
	Defines the flow of the simulation.
	Simultaneously maintains statistics about
	virtual campaigns and effective cpu usage.
	"""

	__metaclass__ = ABCMeta

	def __init__(self, jobs, users, cpus, settings, parts):
		"""
		#TODO users == dict
		#TODO jobs == list
		#TODO POZAMIENIAC TO NA PRIVATE ZMIENNE CO NIE?
		"""
		self.cpu_limit = cpus
		self.cpu_used = 0
		self.future_jobs = jobs
		self.running_jobs = []
		self.waiting_jobs = []
		self.users = users
		self.active_shares = 0
		self.settings = settings
		self.parts = parts

	def run(self):
		"""
		Proceed with the simulation.
		Returns a list of encountered events.
		"""
		self.results = []

		self.pq = PriorityQueue() # events priority queue
		# the first job submit is the simulation 'time zero'
		self.prev_event = self.now = self.future_jobs[0].submit

		# Note: cpu usage decay is always applied after each event.
		# There is also a dummy force_decay event inserted into
		# the queue to force the calculations in case the gap
		# between consecutive events would be too long.
		self.decay_factor = 1 - (0.693 / self.settings.decay)
		self.force_period = 60

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
			self.now, event, entity = self.pq.pop()

			# process the time skipped between events
			diff = self.now - self.prev_event
			if diff:
				self._process_virtual(diff)
				self._process_real(diff)

			# TODO lista -> <time, utility>
			# TODO printy eventow aka job end,
			# TODO i jednak camp start??? bo utility wtedy
#TODO KONIEC KAMP = KONIEC OSTATNIEJ PRACY
#TODO A NIE KONIEC CAMP W VIRT

			# do work based on the event type
			if event == Events.new_job:
				self.new_job_event(entity)
			elif event == Events.job_end:
				self.job_end_event(entity)
			elif event == Events.campaign_end:
				self.camp_end_event(entity)
			elif event == Events.force_decay:
				pass
			else:
				raise Exception('unknown event')

			if event != Events.force_decay:
				# don't update on force_decay since nothing has changed
				self._update_camp_estimates()
			if not self.pq.empty():
				# the simulation has ended if the queue is empty
				self._force_next_decay()
#TODO ESTIMATE_END_EVENT
			# update event timer
			self.prev_event = self.now
		# return simulation results
		return self.results

	def _share_value(self, user):
		"""
		Calculate the user share of the available resources.
		"""
		share = float(u.shares) / self.active_shares
		return share * self.cpu_used

	def _process_virtual(self, period):
		"""
		Distribute virtual time to active users.
		"""
		for u in self.users.iteritems():
			if u.active:
				u.virtual_work(period * self._share_value(u))

	def _process_real(self, period):
		"""
		Account the real work done by the jobs.
		Apply the rolling decay to each user usage.
		"""
		real_decay = self.decay_factor ** period
		for u in self.users.iteritems():
			u.real_work(period, real_decay)

	def _update_camp_estimates(self):
		"""
		Update estimated campaign end times in the virtual schedule.
		Only the first campaign is considered from each user,
		since the subsequent campaigns are guaranteed to end later.
		"""
		for u in self.users.iteritems():
			if u.active:
				first_camp = u.active_camps[0]
				est = first_camp.time_left / self._share_value(u)
				est = self.now + int(math.ceil(est)) # must be int
				self.pq.add(
					est,
					Events.campaign_end,
					first_camp
				)

	def _force_next_decay(self):
		"""
		Add the next decay event.
		"""
		self.pq.add(
			self.now + self.force_period,
			Events.force_decay,
			'Dummy event'
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
				job.start_execution()#TODO TIME)
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
		pass

		#TODO backfilling
		# if still left free and not empty waiting -> self._backfill()

	def new_job_event(self, job):
		"""
		Add the job to a campaign and do a scheduling pass.
		Update the user activity status.
		"""
		if not job.user.active:
			# user will be active after this job submission
			self.active_shares += job.user.shares

		#TODO NAJPIERW ESTIMATE CZY NAJPIERW FIND CAMP??
		#TODO ESTIMATE MUSI BYC NA PEWNO PRZED .ADD_JOB
		job.estimate = self.estimator.initial_estimate(job)
		camp = self.selector.find_campaign(job)
#TODO DODAC TEZ DO USER>ACTIVE_JOBS
		if camp is None:
			camp = job.user.create_campaign(self.now)
			#TODO print <camp start event> aka utility

		camp.add_job(job)
		camp.sort_jobs(key=self._job_camp_key)

		self.waiting_jobs.append(job)
		self._schedule()

	def job_end_event(self, job):
		"""
		Free the resources and do a scheduling pass.
		"""
		job.execution_ended(self.now)
		# the job predicted run time could be higher than the
		# real run time, so we need to redistribute any extra
		# virtual time created by the mentioned difference
		job.user.virtual_work(0)

		# remove the job from the processors
		self.running_jobs.remove(job)
		self.cpu_used -= job.proc
		self._schedule()

	def camp_end_event(self, camp):
		"""
		Remove the campaign. Update the user activity status.
		"""
		assert camp.user.active_camps[0] == camp
		assert camp.time_left == 0

		camp.user.active_camps.pop(0)
		camp.user.completed_camps.append(camp)

		if not camp.user.active:
			# user became inactive
			self.active_shares -= camp.user.shares
