# -*- coding: utf-8 -*-
import heapq
import itertools
import math


class Events(object):
	"""
	The values of the events are **VERY IMPORTANT**.
	They are used in a priority queue to break ties.
	"""
	new_job = 1
	job_end = 2
	estimate_end = 3
	campaign_end = 4
	force_decay = 5


class PriorityQueue(object):
	"""
	A priority queue of <time, event, entity>, ordered by time.
	Ties are ordered by the `Events` value.
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
			# mark an existing event as removed
			self._entries[key][-1] = self._REMOVED
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
		return not self._pq

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
	virtual campaigns and effective CPU usage.
	"""

	def __init__(self, jobs, users, cpus, settings, parts):
		"""
		Args:
		  jobs: a list of submitted `Jobs`.
		  users: a dictionary of `Users`.
		  cpus: the number of CPUs in the cluster.
		  settings: algorithmic settings
		  parts: *instances* of all the system parts
		"""
		assert jobs and users and (cpus > 0)
		self._cpu_limit = cpus
		self._cpu_used = 0
		self._future_jobs = jobs
		self._running_jobs = []
		self._waiting_jobs = []
		self._users = users
		self._active_shares = 0
		self._settings = settings
		self._parts = parts

	def run(self):
		"""
		Proceed with the simulation.
		Return a list of encountered events.
		"""
		self._results = []

		self._pq = PriorityQueue()
		# the first job submission is the simulation 'time zero'
		prev_event = self._future_jobs[0].submit

		# Note:
		#   The CPU usage decay is always applied after each event.
		#   There is also a dummy `force_decay` event inserted into
		#   the queue to force the calculations in case the gap
		#   between consecutive events would be too long.
		self._decay_factor = 1 - (0.693 / self._settings.decay)
		self._force_period = 60

		count = 0
		submits = len(self._future_jobs)

		while count < submits or not self._pq.empty():
			# no need to add more than one event of this type
			if count < submits:
				self._pq.add(
					self._future_jobs[count].submit,
					Events.new_job,
					self._future_jobs[count]
				)
				count += 1
			# the queue cannot be empty here
			self._now, event, entity = self._pq.pop()

			# process the time skipped between events
			diff = self._now - prev_event
			if diff:
				self._process_virtual(diff)
				self._process_real(diff)

			# TODO lista -> <time, utility>
			# TODO printy eventow aka job end,
			# TODO i jednak camp start??? bo utility wtedy

			# do work based on the event type
			if event == Events.new_job:
				self._new_job_event(entity)
			elif event == Events.job_end:
				self._job_end_event(entity)
			elif event == Events.estimate_end:
				self._estimate_end_event(entity)
			elif event == Events.campaign_end:
				self._camp_end_event(entity)
			elif event == Events.force_decay:
				pass
			else:
				raise Exception('unknown event')

			# `Events.new_job` and `Events.campaign_end` change the
			# number of total shares.
			# `Events.job_end` and `Events.estimate_end` change the
			# workload of a specific campaign.
			# Don't update on `Events.force_decay`, nothing changed.
			if event != Events.force_decay:
				self._update_camp_estimates()
			# The simulation has ended if the queue is empty.
			if not self._pq.empty():
				self._force_next_decay()
			else:
				assert count == submits
				assert not self._waiting_jobs
				assert not self._running_jobs
			# update event timer
			prev_event = self._now
		# return simulation results

#TODO KONIEC KAMP = KONIEC OSTATNIEJ PRACY A NIE KONIEC CAMP W VIRT
#TODO AKA camp.completed_jobs[-1].end_time
#TODO NIE TRZEBA TEGO ZAPAMIETYWAC TYLKO NA KONCU WYPISAC
#TODO TAK SAMO WYPISAC JAKIES STATY DLA USEROW NA KONCU
#TODO WYPISAC == DODAC DO RESULTS
#TODO CHECK CORRECNESS AKA
#USER -> assert not self.active_jobs
#USER -> assert not self.active_camps

#TODO POOPISYWAC ASSERTY WSZEDZIE

		return self._results

	def _share_value(self, user):
		"""
		Calculate the user share of the available resources.
		"""
		share = float(user.shares) / self._active_shares
		# this will guarantee that the campaigns will eventually end
		cpus = max(self._cpu_used, 1)
		return share * cpus

	def _process_virtual(self, period):
		"""
		Distribute the virtual time to active users.
		"""
		for u in self._users.itervalues():
			if u.active:
				u.virtual_work(period * self._share_value(u))

	def _process_real(self, period):
		"""
		Update the real work done by the jobs.
		Apply the rolling decay to each user usage.
		"""
		real_decay = self._decay_factor ** period
		for u in self._users.itervalues():
			u.real_work(period, real_decay)

	def _update_camp_estimates(self):
		"""
		Update estimated campaign end times in the virtual schedule.
		Only the first campaign is considered from each user,
		since the subsequent campaigns are guaranteed to end later.
		"""
		for u in self._users.itervalues():
			if u.active:
				first_camp = u.active_camps[0]
				est = first_camp.time_left / self._share_value(u)
				est = self._now + int(math.ceil(est))  # must be int
				self._pq.add(
					est,
					Events.campaign_end,
					first_camp
				)

	def _force_next_decay(self):
		"""
		Add/update the next decay event.
		"""
		self._pq.add(
			self._now + self._force_period,
			Events.force_decay,
			'Dummy event'
		)

	def _schedule(self):
		"""
		Try to execute the highest priority jobs from
		the `_waiting_jobs` list.
		"""
		#sort the jobs using the defined ordering
		self._waiting_jobs.sort(
			key=self._parts.scheduler.job_priority_key)

		while self._waiting_jobs:
			free = self._cpu_limit - self._cpu_used
			if self._waiting_jobs[0].proc <= free:
				# execute the job
				job = self._waiting_jobs.pop(0)
				self._running_jobs.append(job)
				job.start_execution(self._now)
				self._cpu_used += job.proc
				# add events
				self._pq.add(
					self._now + job.run_time,
					Events.job_end,
					job
				)
				self._pq.add(
					self._now + job.estimate,
					Events.estimate_end,
					job
				)
			else:
				# only the top priority job can be scheduled
				break
#TODO DODAC CLASSE CLUSTER
# CLUSTER->CAN_RUN
# CLUSTER->ADD_RESERVATIONS
# CLUSTER->CLEAR_RESERCATIONS
# CLUSTER->RUN JOB
# CLUSTER->JOB FINISHED??
# CLUSTER == lista CPUS Z (start+time_limit, job)
# I SORTOWAC PO END TIMACH ZEBY WIEDZIEC KIEDY SIE CPU ZWOLNIA
# CLUSTER->TEST_JOB_RUNNABLE -> czy free_cpu <= job.prc LUB max(node.free) <= job.proc

		#TODO backfilling
		# if still left free and not empty waiting -> self._backfill()

	def _new_job_event(self, job):
		"""
		Add the job to a campaign and do a scheduling pass.
		Update the owner activity status.
		"""
		assert job.proc <= self._cpu_limit
		if not job.user.active:
			# user will be active after this job submission
			self._active_shares += job.user.shares

		job.estimate = self._parts.estimator.initial_estimate(job)
		camp = self._parts.selector.find_campaign(job)

		if camp is None:
			camp = job.user.create_campaign(self._now)
			#TODO print <camp start event> aka utility

		camp.add_job(job)
		job.user.add_job(job)

		self._waiting_jobs.append(job)
		self._schedule()

	def _job_end_event(self, job):
		"""
		Free the resources and do a scheduling pass.
		"""
		job.execution_ended(self._now)
		# The job predicted run time could be higher than the
		# real run time, so we need to redistribute any extra
		# virtual time created by the mentioned difference.
		job.user.virtual_work(0)

		# remove the job from the processors
		self._running_jobs.remove(job)
		self._cpu_used -= job.proc
		self._schedule()

	def _estimate_end_event(self, job):
		"""
		"""
		#TODO ASSERT
		#TODO ESTIMATE_END_EVENT -> ASSERT W TYM EVENCIE ZE CAMP JEST W ACTIVE A NIE COMPLETED
		pass

	def _camp_end_event(self, camp):
		"""
		Remove the campaign. Update the owner activity status.
		"""
		assert camp.user.active_camps[0] == camp
		assert camp.time_left == 0

		camp.user.active_camps.pop(0)
		camp.user.completed_camps.append(camp)

		if not camp.user.active:
			# user became inactive
			self._active_shares -= camp.user.shares
