# -*- coding: utf-8 -*-
import functools
import heapq
import itertools
import math
import cluster_managers
from util import debug_print, delta


# set up debug level for this module
DEBUG_FLAG = False #__debug__
debug_print = functools.partial(debug_print, DEBUG_FLAG, __name__)


class Events(object):
	"""
	The values of the events are **VERY IMPORTANT**.
	Changing them will break the code.
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


class Simulator(object):
	"""
	Defines the flow of the simulation.
	Simultaneously maintains statistics about
	virtual campaigns and effective CPU usage.
	"""

	def __init__(self, jobs, users, nodes, margins, settings, parts):
		"""
		Args:
		  jobs: a list of submitted `Jobs`.
		  users: a dictionary of `Users`.
		  nodes: the configuration of nodes in the cluster.
		  settings: algorithmic settings
		  parts: *instances* of all the system parts
		"""
		assert jobs and users and nodes, 'invalid arguments'
		self._future_jobs = jobs
		self._waiting_jobs = []
		self._users = users
		self._settings = settings
		self._parts = parts
		self._margins = margins
		self._cpu_used = 0
		self._cpu_limit = sum(nodes.itervalues())
		self._active_shares = 0
		self._total_usage = 0
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

		sub_iter = 0
		sub_total = len(self._future_jobs)
		sub_count = 0
		end_iter = 0

		virt_second = False
		schedule = False
		campaigns = False

		forced_count = 0 #TODO DELETE
		sn, sb = 0, 0 #TODO DELETE
		last_bf = 0

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
				if sub_iter % 1000 == 0: #TODO DELETE
					print "WORKING NOW: JOB STRT", sub_iter, forced_count,
					print 'without', sn, 'with', sb
			# the queue cannot be empty here
			self._now, event, entity = self._pq.pop()

			if event != Events.force_decay:
				debug_print('Time', delta(self._now), 'event', event)

			# Process the time skipped between events
			# before changing the state of the system.
			diff = self._now - prev_event
			if diff:
				virt_second = self._virt_first_stage(diff, event)
				self._real_first_stage(diff, event)

			if event == Events.new_job:
				# check if the job is runnable
				if self._manager.sanity_test(entity):
					self._new_job_event(entity)
				else:
					#TODO PRINT MA BYC
					#print 'WARNING: job', job.ID, 'can never run'
					end_iter += 1
				schedule = campaigns = True
				sub_count -= 1
			elif event == Events.job_end:
				self._job_end_event(entity)
				end_iter += 1
				schedule = campaigns = True
			elif event == Events.estimate_end:
				self._estimate_end_event(entity)
				campaigns = True
			elif event == Events.campaign_end:
				campaigns = self._camp_end_event(entity)
			elif event == Events.force_decay:
				pass
				forced_count += 1 #TODO DELETE
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
				    next_event < Events.campaign_end):
					continue

			if virt_second:
				self._virt_second_stage()
				virt_second = False

			if schedule:
				if self._now > last_bf + 300:
					self._schedule(self._settings.bf_depth)
					sb += 1
					last_bf = (self._now / 300) * 300
				else:
					self._schedule(0)
					sn += 1
				schedule = False
				self._print_utility()  # add results

			if campaigns:
				self._update_camp_estimates()
				campaigns = False

			if end_iter < sub_total:
				# There are still jobs in the simulation
				# so we need an accurate usage.
				self._force_next_decay()

		# add more results
		assert not self._waiting_jobs, 'waiting jobs left'
		assert not self._cpu_used, 'running jobs left'
		for u in self._users.itervalues():
			for i, c in enumerate(u.completed_camps):
				assert i == c.ID, 'invalid campaign ordering'
				assert not c.time_left, 'workload left'
				assert not c.active_jobs, 'active jobs'
				self._print_camp_ended(c)
			assert u._camp_count == len(u.completed_camps), \
			  'missing camps'
			assert not u.active_camps, 'active campaigns'
			assert not u.active_jobs, 'active jobs'
			self._print_user_stats(u)
		# return everything
		print "FORCED COUNT", forced_count
		return self._results

	def _virt_first_stage(self, period, event):
		"""
		In the first virtual stage we just distribute
		the virtual time for the period to active users.

		Return if `_virtual_second_stage` is needed.
		"""
		for u in self._users.itervalues():
			if u.active:
				u.add_virtual(period * self._share_cpu_value(u))

		if event < Events.campaign_end:
			return True
		elif event == Events.campaign_end:
			# need it right away
			self._virt_second_stage()
		return False

	def _virt_second_stage(self):
		"""
		In the second virtual stage active users redistribute
		the accumulated virtual time.
		"""
		for u in self._users.itervalues():
			if u.active:
				u.virtual_work()

	def _real_first_stage(self, period, event):
		"""
		Update the real work done by the jobs in the period
		and apply the rolling decay.

		This is currently the only real stage.
		"""
		# calculate the decay factor for the period
		real_decay = self._decay_factor ** period
		# update global statistics
		self._total_usage += self._cpu_used * period
		self._total_usage *= real_decay
		# and update users usage
		for u in self._users.itervalues():
			u.real_work(period, real_decay)

	def _share_cpu_value(self, user):
		"""
		Calculate the share of the available resources
		for the *active* user.
		"""
		assert user.active, 'inactive user'
		share = float(user.shares) / self._active_shares
		# this will guarantee that the campaigns will eventually end
		cpus = max(self._cpu_used, 1)
		return share * cpus

	def _queue_camp_end(self, camp):
		"""
		Create the `campaign_end` event and insert it to the queue.
		"""
		est = camp.time_left / self._share_cpu_value(camp.user)
		est = self._now + int(math.ceil(est))  # must be int
		self._pq.add(
			est,
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

	def _force_next_decay(self):
		"""
		Add/update the next decay event.
		"""
		self._pq.add(
			self._now + self._force_period,
			Events.force_decay,
			'Dummy event'
		)

	@property
	def _utility(self):
		"""
		Cluster usage.
		"""
		ut = float(self._cpu_used) / self._cpu_limit
		return round(ut, 3)

	@property
	def _cpu_free(self):
		"""
		A simple auto-updating property.
		"""
		return self._cpu_limit - self._cpu_used

	def _schedule(self, bf_limit):
		"""
		Try to execute the highest priority jobs from
		the `_waiting_jobs` list.
		"""

		if not self._cpu_free or not self._waiting_jobs:
			# nothing to do
			return

		#sort the jobs using the ordering defined by the scheduler
		self._parts.scheduler.update_stats({
			'cpu_used': self._cpu_used,
			'active_shares': self._active_shares,
			'total_usage': self._total_usage
		})
		self._waiting_jobs.sort(
			key=self._parts.scheduler.job_priority_key,
			reverse=True
		)

		self._manager.prepare(self._now)

		bf_mode = False
		bf_checked = 0

		# last job has the highest priority
		prio_iter = len(self._waiting_jobs) - 1

		while self._cpu_free and prio_iter >= 0:
			job = self._waiting_jobs[prio_iter]

			run = self._manager.try_schedule(job)
			if run:
				# remove from queue
				j2 = self._waiting_jobs.pop(prio_iter)
				assert job == j2, 'scheduled wrong job'

				# update stats
				job.start_execution(self._now)
				self._cpu_used += job.proc
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
				debug_print('Started', job, 'bf == ', bf_mode)
			else:
				bf_mode = True
				debug_print('Reserved', job)

			# go to next job by priority
			prio_iter -= 1
			# stop if the backfilling checked enough jobs
			if bf_mode:
				bf_checked += 1
				if bf_checked > bf_limit:
					break
		# cleanup
		self._manager.clear_reservations()

	def _new_job_event(self, job):
		"""
		Add the job to a campaign. Update the owner activity status.
		"""
		user = job.user

		if not user.active:
			# user is now active after this job submission
			self._active_shares += user.shares

		job.estimate = self._parts.estimator.initial_estimate(job)
		camp = self._parts.selector.find_campaign(job)

		if camp is None:
			camp = user.create_campaign(self._now)
			self._print_camp_created(camp)  # add results

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
		self._cpu_used -= job.proc
		assert self._cpu_used >= 0, 'invalid cpu count'
		self._print_job_ended(job)  # add results

	def _estimate_end_event(self, job):
		"""
		Get a new estimate for the job.
		"""
		assert job.estimate < job.run_time, 'invalid estimate'
		user = job.user

		if not user.active:
			# user became inactive due to inaccurate estimates
			user.false_inactivity += (self._now - user.last_active)
			self._active_shares += user.shares

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
			self._active_shares -= user.shares
			# fix rounding errors
			self._active_shares = max(self._active_shares, 0)
			# we need new estimates, because shares changed
			return True
		else:
			self._queue_camp_end(user.active_camps[0])
			# shares still the same
			return False

	def _margin_symbol(self, event_time):
		"""
		Return the status of the event based on the event time.
		"""
		if (event_time < self._margins[0] or
		    event_time > self._margins[1]):
			return 'MARGIN '
		return 'CORE '

	def _print_utility(self):
		"""
		UTILITY time value
		"""
		msg = 'UTILITY {} {}'.format(self._now, self._utility)
		self._results.append(
			self._margin_symbol(self._now) + msg)

	def _print_camp_created(self, camp):
		"""
		CAMPAIGN START camp_id user_id time utility
		"""
		msg = 'CAMPAIGN START {} {} {} {}'.format(
			camp.ID, camp.user.ID, camp.created, self._utility)
		self._results.append(
			self._margin_symbol(camp.created) + msg)

	def _print_camp_ended(self, camp):
		"""
		CAMPAIGN END camp_id user_id real_end_time workload job_count
		"""
		msg = 'CAMPAIGN END {} {} {} {} {}'.format(
			camp.ID, camp.user.ID, camp.completed_jobs[-1].end_time,
			camp.workload, len(camp.completed_jobs))
		self._results.append(
			self._margin_symbol(camp.created) + msg)

	def _print_job_ended(self, job):
		"""
		JOB job_id camp_id user_id submit start end
		    final_estimate time_limit processor_count
		"""
		msg = 'JOB {} {} {} {} {} {} {} {} {}'.format(
			job.ID, job.camp.ID, job.user.ID,
			job.submit, job.start_time, job.end_time,
			job.estimate, job.time_limit, job.proc)
		self._results.append(
			self._margin_symbol(job.submit) + msg)

	def _print_user_stats(self, user):
		"""
		USER user_id camp_count lost_virtual_time
		     false_inactivity_period
		"""
		msg = 'USER {} {} {} {}'.format(
			user.ID, len(user.completed_camps),
			user.lost_virtual, user.false_inactivity)
		self._results.append('CORE ' + msg)
