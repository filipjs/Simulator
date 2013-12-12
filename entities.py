# -*- coding: utf-8 -*-


class Job(object):
	"""
	A single job with the relevant properties.
	Correct usage scheme:
		reset -> camp.setter -> start_execution -> execution_ended
	"""
	def __init__(self, stats, user):
		self._stats = stats
		self._user = user
		self.estimate = None

	def reset(self):
		self._camp = None
		self._camp_index = None
		self._start = None
		self._completed = False

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
	def completed(self):
		return self._completed
	@property
	def camp(self):
		return self._camp
	@camp.setter
	def camp(self, v):
		assert self._camp is None
		self._camp = v

	def start_execution(self, t):
		assert self.start_time is None
		self._start = t
		# notify further
		self._camp.job_started(self)
		self._user.job_started(self)

	def execution_ended(self, t):
		assert self.end_time == t
		self._completed = True
		# notify further
		self._camp.job_ended(self)
		self._user.job_ended(self)

	def __str__(self):
		return "{} {} {} {} {} {} {} {}".format(self.ID,
			self.user.ID, self.camp.ID, self.proc,
			self.submit, self.start_time,
			self.run_time, self.estimate)


class Campaign(object):
	"""
	A user campaign with the appropriate jobs.
	A campaign is active if it is running in the virtual schedule.
	"""
	def __init__(self, id, user, time_stamp):
		self._id = id
		self._user = user
		self._created = time_stamp
		self._remaining = 0
		self._completed = 0
		self.virtual = 0
		self.offset = 0
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
		# self.virtual is a float
		# and want an integer value returned
		return self.workload - int(self.virtual)
	@property
	def active(self):
		return self.time_left > 0

	def add_job(self, job):
		# until the job ends we can only use the estimate
		self._remaining += job.estimate * job.proc
		self.active_jobs.append(job)
		job.camp = self # backward link

	def job_started(self, job):
		# nothing happens at this time
		pass

	def job_ended(self, job):
		# update the run time to the real value
		self._remaining -= job.estimate * job.proc
		self._completed += job.run_time * job.proc
		self.active_jobs.remove(job)
		self.completed_jobs.append(job)

	def sort_jobs(self, job_cmp):
		self.active_jobs.sort(key=job_key)
		for i, job in enumerate(self.active_jobs):
			# update the position in the campaign list
			job.camp_index = i


class User(object):
	"""
	User account with campaign lists and usage stats.
	Active_camps are ordered by creation time.
	Completed_camps are ordered by virtual end time.
	Completed_jobs are ordered by execution end time.
	"""
	def __init__(self, uid):
		self._id = uid
		self.camp_count = 0
		self.ost_shares = None
		self.fair_shares = None

	def reset(self):
		assert not self.active_camps
		assert not self._occupied_cpus
		self._lost_virtual = 0
		self._cpu_clock_used = 0
		self._occupied_cpus = 0
		self.active_camps = []
		self.completed_camps = []
		self.completed_jobs = []

	@property
	def ID(self):
		return self._id
	@property
	def active(self):
		return bool(self.active_camps)

	def virtual_work(self, value):
		total = reduce(lambda x, y: x + y.virtual, self.active_camps, value)
		offset = 0
		for camp in self.active_camps:
			virt = min(camp.workload, total)
			camp.virtual = virt
			total -= virt
			camp.offset = offset
			offset += camp.time_left
		# overflow from total is lost
		self._lost_virtual += total

	def real_work(self, value):
		self._cpu_clock_used += self._occupied_cpus * value

	def job_started(self, job):
		# we only need to know the number of processors
		self._occupied_cpus += job.proc

	def job_ended(self, job):
		# update processor count
		self._occupied_cpus -= job.proc
		self.completed_jobs.append(job)
