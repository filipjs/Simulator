# -*- coding: utf-8 -*-


class ReadOnlyAttr(object):
	"""
	A data descriptor that permits only a maximum of
	one setter invocation (from None to value).
	"""
	def __init__(self, value=None):
		self._val = value

	def __get__(self, obj, objtype):
		return self._val
	def __set__(self, obj, val):
		if self._val is not None:
			raise AttributeError("read-only attribute")
		self._val = val


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
	def __init__(self, stats, user):
		"""
		Required entries in stats:
		  id, proc, submit, run_time
		"""
		self._stats = stats
		self._user = user
		self.time_limit = ReadOnlyAttr()

	def reset(self):
		self._camp = ReadOnlyAttr()
		self._start = ReadOnlyAttr()
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
		return self._start

	@property
	def end_time(self):
		return self.start_time + self.run_time

	@property
	def started(self):
		return self._start is not None

	@property
	def completed(self):
		return self._completed

	def start_execution(self, t):
		self._start = t
		# notify further
		self._camp.job_started(self)
		self._user.job_started(self)

	def execution_ended(self, t):
		assert not self._completed
		assert self.end_time == t
		self._completed = True
		# notify further
		self._camp.job_ended(self)
		self._user.job_ended(self)

	def __str__(self):
		return '{} {} {} {} {} {} {} {} {}'.format(
			self.ID, self.user.ID, self.camp.ID,
			self.proc, self.submit, self.start_time,
			self.run_time, self.time_limit, self.estimate
		)


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

	def job_new_estimate(self, job, old_value):
		# update the workload
		self._remaining -= old_value * job.proc
		self._remaining += job.estimate * job.proc


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
	def __init__(self, uid):
		self._id = uid
		self._global_count = 0
		self.shares = ReadOnlyAttr()

	def reset(self):
		assert not self.active_jobs
		assert not self.active_camps
		self._lost_virtual = 0
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

	def virtual_work(self, value):
		"""
		Process the `value` long period in the virtual schedule.
		"""
		total = reduce(lambda x, y: x + y._virtual,
			       self.active_camps, value)
		offset = 0
		for camp in self.active_camps:
			virt = min(camp.workload, total)
			total -= virt
			camp._virtual = virt
			camp._offset = offset
			offset += camp.time_left
		# overflow from total is lost
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

	def create_campaign(self, time):
		new_camp = Campaign(self._global_count, self, time)
		self._global_count += 1
		self.active_camps.append(new_camp)
		return new_camp
