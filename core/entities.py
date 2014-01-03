# -*- coding: utf-8 -*-


class ReadOnlyAttr(object):
	"""
	A data descriptor that permits only one setter invocation
	on an unassigned attribute.

	Setting a value `None` changes the state back to unassigned.
	"""
	def __init__(self):
		self._data = {}

	def __get__(self, obj, objtype):
		assert obj is not None, 'only usable at the instance level'
		return self._data.get(obj, None)

	def __set__(self, obj, value):
		if value is None or obj not in self._data:
			self._data[obj] = value
		elif self._data[obj] is None:
			self._data[obj] = value
		else:
			raise AttributeError('assignment to a read-only attribute')


class Job(object):
	"""
	A single job with the relevant properties.

	Attributes:
	  ID: job ID, globally unique.
	  user: `User` instance.
	  proc: number of required CPUs.
	  submit: job submission time.
	  run_time: job execution time.
	  estimate: job run time estimate set by the scheduler.
	  time_limit: job time limit (>= run time) set by the owner.
	  start_time: execution start time.
	  end_time: execution end time.
	  started: job state.
	  completed: job state.
	  camp: `Campaign` instance to which the job belongs to.

	Correct usage scheme for each simulation:
	  1) reset
	  2) add to campaign
	  3) start execution
	  4) execution ended
	"""

	time_limit = ReadOnlyAttr()
	nodes = ReadOnlyAttr()
	pn_cpus = ReadOnlyAttr()
	camp = ReadOnlyAttr()

	def __init__(self, stats, user):
		"""
		Required entries in stats:
		  id, proc, submit, run_time
		"""
		self._stats = stats
		self._user = user

	def reset(self):
		self.camp = None
		self._start = None
		self._completed = False
		self.estimate = None

	@property
	def ID(self):
		return self._stats['id']

	@property
	def user(self):
		return self._user

	@property
	def proc(self):
		return self._stats['proc']

	@property
	def submit(self):
		return self._stats['submit']

	@property
	def run_time(self):
		return self._stats['run_time']

	@property
	def start_time(self):
		assert self.started, 'job not started'
		return self._start

	@property
	def end_time(self):
		assert self.completed, 'job not completed'
		return self.start_time + self.run_time

	@property
	def started(self):
		return self._start is not None

	@property
	def completed(self):
		return self._completed

	def start_execution(self, t):
		assert not self.started, 'job already started'
		assert not self.completed, 'job already completed'
		self._start = t
		# notify further
		self.camp.job_started(self)
		self.user.job_started(self)

	def execution_ended(self, t):
		assert not self.completed, 'job already completed'
		assert self.start_time + self.run_time == t, 'invalid run time'
		self._completed = True
		# notify further
		self.camp.job_ended(self)
		self.user.job_ended(self)

	def next_estimate(self, value):
		# notify further
		self.camp.job_next_estimate(self, value)
		self.user.job_next_estimate(self, value)
		self.estimate = value

class Campaign(object):
	"""
	A single user campaign.

	Attributes:
	  ID: campaign ID, unique for each user, even between different simulations.
	  user: `User` instance.
	  created: time the campaign was created.
	  workload: predicted total execution time needed to finish the jobs.
	  time_left: virtual time needed to fulfill the campaign workload.
	  offset: virtual time needed to finish earlier created campaigns.
	  active: campaign state in the virtual schedule.
	  active_jobs: not finished jobs (pending or running).
	  completed_jobs: jobs that finished execution, ordered by end time.

	"""
	def __init__(self, id, user, time):
		self._id = id
		self._user = user
		self._created = time
		self._remaining = 0
		self._completed = 0
		self._virtual = 0
		self._offset = 0
		self.active_jobs = []
		self.completed_jobs = []

	@property
	def ID(self):
		return self._id

	@property
	def user(self):
		return self._user

	@property
	def created(self):
		return self._created

	@property
	def workload(self):
		return self._remaining + self._completed

	@property
	def time_left(self):
		# self.virtual is a float and we want an int
		return self.workload - int(self._virtual)

	@property
	def offset(self):
		return self._offset

	@property
	def active(self):
		return self.time_left > 0

	def add_job(self, job):
		# until the job ends we can only use the estimate
		self._remaining += job.estimate * job.proc
		self.active_jobs.append(job)
		job.camp = self # backward link

	def job_started(self, job):
		pass # nothing happens at this time

	def job_ended(self, job):
		# update the run time to the real value
		self._remaining -= job.estimate * job.proc
		self._completed += job.run_time * job.proc
		self.active_jobs.remove(job)
		self.completed_jobs.append(job)

	def job_next_estimate(self, job, new_value):
		# update the workload
		self._remaining -= job.estimate * job.proc
		self._remaining += new_value * job.proc


class User(object):
	"""
	A single user account with campaign lists and usage stats.

	Attributes:
	  ID: user ID, globally unique.
	  shares: assigned share of the resources.
	  active: state in the virtual schedule.
	  cpu_clock_used: total usage, already accounting the decay.
	  active_jobs: not finished jobs (pending or running).
	  completed_jobs: jobs that finished execution, ordered by end time.
	  active_camps: active campaigns (see `Campaign`), ordered by creation time.
	  completed_camps: completed campaigns, ordered by the virtual end time.

	"""

	shares = ReadOnlyAttr()

	def __init__(self, uid):
		self._id = uid
		self._global_count = 0

	def reset(self):
		self._lost_virtual = 0
		self._virt_pool = 0
		self.last_active = None
		self.false_inactivity = 0
		self._cpu_clock_used = 0
		self._occupied_cpus = 0
		self.active_jobs = []
		self.completed_jobs = []
		self.active_camps = []
		self.completed_camps = []

	@property
	def ID(self):
		return self._id

	@property
	def active(self):
		return bool(self.active_camps)

	@property
	def cpu_clock_used(self):
		return round(self._cpu_clock_used, 3)

	def set_virtual(self, value):
		"""
		Set the `value` long period as the virtual pool,
		which will be processed in next `virtual_work` call.
		"""
		assert not self._virt_pool, 'virtual pool not redistributed'
		self._virt_pool = value

	def virtual_work(self):
		"""
		Redistribute the accumulated virtual pool.
		"""
		total = reduce(lambda x, y: x + y._virtual,
			       self.active_camps, self._virt_pool)
		offset = 0
		for camp in self.active_camps:
			virt = min(camp.workload, total)
			total -= virt
			camp._virtual = virt
			camp._offset = offset
			offset += camp.time_left
		# overflow from total is lost
		self._virt_pool = 0
		self._lost_virtual += total

	def real_work(self, value, real_decay):
		"""
		Process the `value` long period in the real schedule.
		Apply the decay factor `real_decay`.
		"""
		self._cpu_clock_used += self._occupied_cpus * value
		self._cpu_clock_used *= real_decay

	def add_job(self, job):
		self.active_jobs.append(job)

	def job_started(self, job):
		# we need to keep track of the number of processors
		self._occupied_cpus += job.proc

	def job_ended(self, job):
		# update the processor count
		self._occupied_cpus -= job.proc
		self.active_jobs.remove(job)
		self.completed_jobs.append(job)
		# The job predicted run time could be higher than the
		# real run time, so we need redistribute the difference.
		diff = (job.estimate - job.run_time) * job.proc
		job.camp._virtual -= diff
		self._virt_pool += diff

	def job_next_estimate(self, job, new_value):
		if not job.camp in self.active_camps:
			# This campaign has to be made active again.
			# `camp.ID` corresponds to the location in the list.
			loc = job.camp.ID
			self.completed_camps, rest = (
				self.completed_camps[:loc],
				self.completed_camps[loc:]
			)
			self.active_camps = rest + self.active_camps
			assert job.camp == self.active_camps[0], \
			  'invalid campaign ordering'

	def create_campaign(self, time):
		new_camp = Campaign(self._global_count, self, time)
		self._global_count += 1
		self.active_camps.append(new_camp)
		return new_camp
