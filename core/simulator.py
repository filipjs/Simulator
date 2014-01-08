# -*- coding: utf-8 -*-
import functools
import heapq
import itertools
import math
import time
import cluster_managers
from util import debug_print, delta


# set up debug level for this module
DEBUG_FLAG = True
DEBUG_FLAG = False
debug_print = functools.partial(debug_print, DEBUG_FLAG, __name__)


class Events(object):
	"""
	The values of the events are **VERY IMPORTANT**.
	Changing them will break the code.
	"""
	new_job = 1
	job_end = 2
	estimate_end = 3
	bf_run = 4
	campaign_end = 5
	force_decay = 6


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
		Raise KeyError if the queue is empty.
		"""
		if not self.empty():
			time, event, _, entity = heapq.heappop(self._pq)
			key = (event, entity)
			del self._entries[key]
			return time, event, entity
		raise KeyError('pop from an empty priority queue')

	def peek(self):
		"""
		Peek at the next upcoming event.
		Raise KeyError if the queue is empty.
		"""
		if not self.empty():
			time, event, _, entity = self._pq[0]
			return time, event, entity
		raise KeyError('peek from an empty priority queue')

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


class Container(object):
	"""
	A simple class that acts as a dictionary.
	"""
	pass


class Simulator(object):
	"""
	Defines the flow of the simulation.
	Simultaneously maintains statistics about
	virtual campaigns and effective CPU usage.
	"""

	def __init__(self, block, users, nodes, settings, parts):
		"""
		Args:
		  block: a `Block` instance with the submitted `Jobs`.
		  users: a dictionary of `Users`.
		  nodes: the configuration of nodes in the cluster.
		  settings: algorithmic settings
		  parts: *instances* of all the system parts
		"""
		assert block and users and nodes, 'invalid arguments'
		self._future_jobs = block
		self._waiting_jobs = []
		self._users = users
		self._settings = settings
		self._parts = parts
		self._core_period = (block.core_start, block.core_end)
		self._stats = Container()
		self._stats.cpu_used = 0
		self._stats.cpu_limit = sum(nodes.itervalues())
		self._stats.active_shares = 0
		self._stats.total_usage = 0
		# create an appropriate cluster manager
		if len(nodes) == 1:
			self._manager = cluster_managers.SingletonManager(nodes, settings)
		else:
			self._manager = cluster_managers.SlurmManager(nodes, settings)

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
		self._force_period = 60 * 5

		# initialize the scheduler
		self._parts.scheduler.set_stats(self._stats)

		sub_iter = sub_count = 0
		sub_total = len(self._future_jobs)
		end_iter = 0

		schedule = backfill = False
		instant_bf = (self._settings.bf_depth and
			      not self._settings.bf_interval)

		diag = Container()
		diag.forced = 0
		diag.sched_pass = diag.sched_jobs = 0
		diag.bf_pass = diag.bf_jobs = 0
		diag.prev_util = (prev_event, 0)  # <time, utility>
		diag.avg_util = 0
		diag.sim_time = time.time()

		while sub_iter < sub_total or not self._pq.empty():
			# We only need to keep two `new_job` events in the
			# queue at the same time (one to process, one to peek).
			while sub_iter < sub_total and sub_count < 2:
				self._pq.add(
					self._future_jobs[sub_iter].submit,
					Events.new_job,
					self._future_jobs[sub_iter]
				)
				sub_iter += 1
				sub_count += 1
			# the queue cannot be empty here
			self._now, event, entity = self._pq.pop()

			if event != Events.force_decay:
				debug_print('Time', delta(self._now), 'event', event)

			# Process the time skipped between events
			# before changing the state of the system.
			diff = self._now - prev_event
			if diff:
				self._virt_first_stage(diff)
				self._real_first_stage(diff)
			# The default flow is to redistribute the virtual
			# time and compute new campaign ends (and maybe do
			# a scheduling / backfilling pass in the between).
			virt_second = True
			campaigns = True

			if event == Events.new_job:
				# check if the job is runnable
				if self._manager.sanity_test(entity):
					self._new_job_event(entity)
					schedule = True
				else:
					#print 'WARNING: job', job.ID, 'can never run'
					end_iter += 1
				sub_count -= 1
			elif event == Events.job_end:
				self._job_end_event(entity)
				end_iter += 1
				schedule = True
			elif event == Events.estimate_end:
				self._estimate_end_event(entity)
			elif event == Events.bf_run:
				backfill = True
			elif event == Events.campaign_end:
				# We need to redistribute beforehand
				# so the campaign can actually end.
				self._virt_second_stage()
				virt_second = False  # already done
				campaigns = self._camp_end_event(entity)
			elif event == Events.force_decay:
				diag.forced += 1
				virt_second = False  # no need to do it now
				campaigns = False  # no change to campaign ends
			else:
				raise Exception('unknown event')

			# update event timer
			prev_event = self._now

			if not self._pq.empty():
				# We need to process the events that happen at
				# the same time *AND* change the campaign workloads
				# before we can continue further.
				next_time, next_event, _ = self._pq.peek()
				if (next_time == self._now and
				    next_event < Events.bf_run):
					continue

			if virt_second:
				self._virt_second_stage()

			if schedule:
				diag.sched_pass += 1
				diag.sched_jobs += self._schedule(bf_mode=False)
				self._avg_util(diag) # after sched
				schedule = False
				if instant_bf: backfill = True
				self._store_utility()  # add results

			if backfill:
				diag.bf_pass += 1
				exec_jobs = self._schedule(bf_mode=True)
				diag.bf_jobs += exec_jobs
				self._avg_util(diag)  # after sched
				backfill = False
				self._store_utility()  # add results

			if campaigns:
				self._update_camp_estimates()

			# add periodically occurring events
			if event < Events.bf_run:
				self._next_backfill(self._now)
			elif event == Events.bf_run and exec_jobs:
				self._next_backfill(self._now + 1)

			if end_iter < sub_total:
				# There are still jobs in the simulation
				# so we need an accurate usage.
				assert not self._pq.empty(), 'infinite loop'
				self._next_force_decay()

		# finalize diagnostic stats
		true_end = min(diag.prev_util[0], self._core_period[1])
		del diag.prev_util
		diag.avg_util /= (true_end - self._core_period[0])

		diag.sim_time = time.time() - diag.sim_time

		diag.sched_jobs /= float(sub_total)
		diag.bf_jobs /= float(sub_total)

		# add the rest of the results
		for u in self._users.itervalues():
			for i, c in enumerate(u.completed_camps):
				assert i == c.ID, 'invalid campaign ordering'
				assert not c.time_left, 'workload left'
				self._store_camp_ended(c)
			self._store_user_stats(u)
		self._store_system(diag)

		# cleanup
		self._parts.scheduler.clear_stats()

		return self._results, diag


	def _virt_first_stage(self, period):
		"""
		In the first virtual stage we just distribute
		the virtual time for the period to active users.
		"""
		for u in self._users.itervalues():
			if u.active:
				u.add_virtual(period * self._share_cpu_value(u))

	def _virt_second_stage(self):
		"""
		In the second virtual stage active users redistribute
		the accumulated virtual time.
		"""
		for u in self._users.itervalues():
			if u.active:
				u.virtual_work()

	def _real_first_stage(self, period):
		"""
		Update the real work done by the jobs in the period
		and apply the rolling decay.

		This is currently the only real stage.
		"""
		# calculate the decay factor for the period
		real_decay = self._decay_factor ** period
		# update global statistics
		self._stats.total_usage += self._stats.cpu_used * period
		self._stats.total_usage *= real_decay
		# and update users usage
		for u in self._users.itervalues():
			u.real_work(period, real_decay)

	def _share_cpu_value(self, user):
		"""
		Calculate the share of the available resources
		for the *active* user.
		"""
		assert user.active, 'inactive user'
		share = float(user.shares) / self._stats.active_shares
		# this will guarantee that the campaigns will eventually end
		cpus = max(self._stats.cpu_used, 1)
		return share * cpus

	def _queue_camp_end(self, camp):
		"""
		Create the `campaign_end` event and insert it to the queue.
		"""
		est = camp.time_left / self._share_cpu_value(camp.user)
		est = self._now + math.ceil(est)
		self._pq.add(
			int(est),  # must be int
			Events.campaign_end,
			camp
		)

	def _update_camp_estimates(self):
		"""
		Update estimated campaign end times in the virtual schedule.
		Only the first campaign is considered from each user,
		since the subsequent campaigns are guaranteed to end later.
		"""
		for u in self._users.itervalues():
			if u.active:
				self._queue_camp_end(u.active_camps[0])

	def _next_backfill(self, start):
		"""
		Add the next backfill event from `start`.
		"""
		if (not self._settings.bf_depth or
		    not self._settings.bf_interval):
			return  # backfilling 'thread' turned off
		next = float(start) / self._settings.bf_interval
		next = math.ceil(next) * self._settings.bf_interval
		self._pq.add(
			int(next),  # must be int
			Events.bf_run,
			'Bf event'
		)

	def _next_force_decay(self):
		"""
		Add/update the next decay event.
		"""
		self._pq.add(
			self._now + self._force_period,
			Events.force_decay,
			'Force event'
		)

	@property
	def _utility(self):
		"""
		Cluster usage.
		"""
		return float(self._stats.cpu_used) / self._stats.cpu_limit

	@property
	def _cpu_free(self):
		"""
		Free CPUs.
		"""
		return self._stats.cpu_limit - self._stats.cpu_used

	def _execute(self, job):
		"""
		Start the job execution.
		"""
		job.start_execution(self._now)
		# update stats
		self._stats.cpu_used += job.proc
		assert self._cpu_free >= 0, 'invalid cpu count'
		# add events
		self._pq.add(
			self._now + job.run_time,
			Events.job_end,
			job
		)
		if job.estimate < job.run_time:
			self._pq.add(
				self._now + job.estimate,
				Events.estimate_end,
				job
			)

	def _schedule(self, bf_mode):
		"""
		Try to execute the highest priority jobs.

		If not in `bf_mode` stop on the first failure.

		Return the number of started jobs.
		"""

		if not self._cpu_free or not self._waiting_jobs:
			return 0  # nothing to do

		#sort the jobs using the ordering defined by the scheduler
		self._waiting_jobs.sort(
			key=self._parts.scheduler.job_priority_key,
			reverse=True
		)

		if bf_mode:
			self._manager.prepare_backfill(self._now)
			try_func = self._manager.try_backfill

			assert self._settings.bf_depth, 'invalid bf_depth'
			work = min(len(self._waiting_jobs), self._settings.bf_depth)
		else:
			#TODO FIXME
			#try_func = self._manager.try_schedule
			self._manager.prepare_backfill(self._now)
			try_func = self._manager.try_backfill

			work = len(self._waiting_jobs)

		# last job has the highest priority
		prio_iter = len(self._waiting_jobs) - 1
		started = 0

		while self._cpu_free and work:
			job = self._waiting_jobs[prio_iter]

			if try_func(job):
				j2 = self._waiting_jobs.pop(prio_iter)
				assert job == j2, 'scheduled wrong job'

				self._execute(job)
				started += 1
				debug_print('Bf_mode', bf_mode, 'started', job)
			elif not bf_mode:
				break

			prio_iter -= 1
			work -= 1
		self._manager.clear_reservations() #TODO REMOVE
		return started

	def _new_job_event(self, job):
		"""
		Add the job to a campaign. Update the owner activity status.
		"""
		user = job.user

		if not user.active:
			# user is now active after this job submission
			self._stats.active_shares += user.shares

		job.estimate = self._parts.estimator.initial_estimate(job)
		camp = self._parts.selector.find_campaign(job)

		if camp is None:
			camp = user.create_campaign(self._now)
			self._store_camp_created(camp)  # add results

		camp.add_job(job)
		user.add_job(job)
		# enqueue the job
		self._waiting_jobs.append(job)

	def _job_end_event(self, job):
		"""
		Free the resources.
		"""
		assert job.estimate >= job.run_time, 'invalid estimate'
		job.execution_ended(self._now)
		self._manager.job_ended(job)
		self._stats.cpu_used -= job.proc
		assert self._stats.cpu_used >= 0, 'invalid cpu count'
		self._store_job_ended(job)  # add results

	def _estimate_end_event(self, job):
		"""
		Get a new estimate for the job.
		"""
		assert job.estimate < job.run_time, 'invalid estimate'
		user = job.user

		if not user.active:
			# user became inactive due to inaccurate estimates
			user.false_inactivity += (self._now - user.last_active)
			self._stats.active_shares += user.shares

		new_est = self._parts.estimator.next_estimate(job)
		job.next_estimate(new_est)

		# add the next event if we will need it
		if job.estimate < job.run_time:
			self._pq.add(
				job.start_time + job.estimate,
				Events.estimate_end,
				job
			)

	def _camp_end_event(self, camp):
		"""
		Remove the campaigns that ended in the virtual schedule.
		Update the owner activity status.

		Return if the `_update_camp_estimates` call is needed.
		"""
		assert not camp.time_left, 'campaign still active'
		user = camp.user

		if not user.active_camps or camp != user.active_camps[0]:
			# In the `_update_camp_estimates` we aren't removing
			# the old `campaign_end` events from the queue,
			# so it is possible that this event is out of order.
			debug_print('Skipping campaign_end event', camp)
			return

		while user.active_camps and not user.active_camps[0].time_left:
			# remove all of the campaigns that end now in one go
			ended = user.active_camps.pop(0)
			user.completed_camps.append(ended)

		if not user.active:
			# user became inactive
			user.last_active = self._now
			self._stats.active_shares -= user.shares
			# fix possible rounding errors
			self._stats.active_shares = max(self._stats.active_shares, 0)
			# we need new estimates, because shares changed
			return True
		else:
			self._queue_camp_end(user.active_camps[0])
			# shares still the same
			return False

	def _avg_util(self, diag):
		"""
		Keep track of the system average utility.
		"""
		core_st, core_end = self._core_period
		prev, ut = diag.prev_util

		if self._now >= core_st and prev < core_end:
			prev = max(prev, core_st)
			now = min(self._now, core_end)
			diag.avg_util += (now - prev) * ut
		diag.prev_util = (self._now, self._utility)

	def _store_msg(self, event_time, msg):
		"""
		Add the message to the results.
		"""
		if (event_time < self._core_period[1] and
		    event_time >= self._core_period[0]):
			prefix = 'CORE '
		else:
			prefix = 'MARGIN '
		self._results.append(prefix + msg)

	def _store_utility(self):
		"""
		Event message:
		  UTILITY time value
		"""
		msg = 'UTILITY {} {:.4f}'.format(self._now, self._utility)
		self._store_msg(self._now, msg)

	def _store_camp_created(self, camp):
		"""
		Event message:
		  CAMPAIGN START camp_id user_id time utility
		"""
		msg = 'CAMPAIGN START {} {} {} {:.4f}'.format(
			camp.ID, camp.user.ID, camp.created, self._utility)
		self._store_msg(camp.created, msg)

	def _store_camp_ended(self, camp):
		"""
		Event message:
		  CAMPAIGN END camp_id user_id real_end_time workload job_count
		"""
		msg = 'CAMPAIGN END {} {} {} {} {}'.format(
			camp.ID, camp.user.ID, camp.completed_jobs[-1].end_time,
			camp.workload, len(camp.completed_jobs))
		self._store_msg(camp.created, msg)

	def _store_job_ended(self, job):
		"""
		Event message:
		  JOB job_id camp_id user_id submit start end
		      final_estimate time_limit processor_count
		"""
		msg = 'JOB {} {} {} {} {} {} {} {} {}'.format(
			job.ID, job.camp.ID, job.user.ID,
			job.submit, job.start_time, job.end_time,
			job.estimate, job.time_limit, job.proc)
		self._store_msg(job.submit, msg)

	def _store_user_stats(self, user):
		"""
		Event message:
		  USER user_id camp_count lost_virtual_time
		       false_inactivity_period
		"""
		msg = 'USER {} {} {} {}'.format(
			user.ID, len(user.completed_camps),
			user.lost_virtual, user.false_inactivity)
		self._results.append('CORE ' + msg)

	def _store_system(self, diag):
		"""
		Event message:
		  SYSTEM sched_iterations sched_jobs bf_iterations bf_jobs
		         average_core_utility simulation_time forced_events
		"""
		msg = 'SYSTEM {sched_pass} {sched_jobs} {bf_pass} {bf_jobs}' \
		      '{avg_util:.4f} {sim_time:.2f} {forced}'.format(**diag.__dict__)
		self._results.append('CORE ' + msg)
