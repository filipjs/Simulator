# -*- coding: utf-8 -*-
import functools


class Job(object):
	"""
	A single job with the all relevant properties.
	Correct usage scheme:
		reset -> camp.setter -> start_execution -> execution_ended
	"""
	def __init__(self, stats):
		self._stats = stats
		self.estimate = None
	def reset(self):
		self._camp = None
		self._start = None
		self._completed = False
	@property
	def ID(self):
		return self._stats['id']
	@property
	def user(self):
		return self._stats['user']
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
		assert not self._completed
		self._start = t
	def execution_ended(self, t):
		assert self.end_time == t
		self._completed = True
	def __str__(self):
		return "{} {} {} {} {} {} {} {}".format(self.ID,
			self.userID, self.camp.ID, self.proc,
			self.submit, self.start_time,
			self.run_time, self.estimate)


class Campaign(object):
	"""
	A user campaign with the appropriate jobs.
	A campaign is 'active' if it is still running in the virtual schedule.
	"""
	def __init__(self, id, user, time_stamp):
		self._id = id
		self._user = user
  		self._created = time_stamp
		self._remaining = 0
		self._completed = 0
		self.virtual = 0
		self.offset = 0
		self._active_jobs = []
		self._completed_jobs = []
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
		return self.workload - self.virtual
	@property
	def active(self):
		return self.time_left > 0
	def add_job(self, job):
		self._remaining += job.estimate * job.proc
		self._active_jobs.append(job)
		job.camp = self # forward link
	def job_ended(self, job):
		self._remaining -= job.estimate * job.proc
		self._completed += job.run_time * job.proc
		self._active_jobs.remove(job)
		self._completed_jobs.append(job)
	def sort_jobs(self, job_cmp):
		self._active_jobs = sorted(
			self._active_jobs,
			key=functools.cmp_to_key(job_cmp)
		)
		for i, job in enumerate(self._active_jobs):
			job.camp_index = i # position in the list


class User(object):
	"""
	User account with campaign list and usage stats.
	Campaigns are sorted by creation time.
	Ended_jobs are sorted by execution end time.
	"""
	def __init__(self, uid):
		self._id = uid
		self._active_camps = []
		self._completed_camps = []
		self.shares = None
	def reset(self):
		assert not self._active_camps
		self.lost_virtual = 0
		self.raw_usage = 0
		self.fair_share = 0
		self.completed_jobs = []
	@property
	def ID(self):
		return self._id
	@property
	def active(self):
		return bool(self._active_camps)

	def virtual_work(self, value):
		total = reduce(lambda x, y: x + y.virtual, self._active_camps, value)
		offset = 0
		for camp in self._active_camps:
			virt = min(camp.workload, total)
			camp.virtual = virt
			total -= virt
			camp.offset = offset
			offset += camp.time_left
		# overflow from 'total' is lost
		self.lost_virtual += total

	def add_job(self, job, threshold):
		last_camp = self._camps and self._camps[-1]
		if not last_camp or job.submit > last_camp.created + threshold:
			self._camp_counter += 1
			last_camp = Campaign(self, job.submit)
			self._camps.append(last_camp)
		last_camp.add_job(job)
	def job_ended(self, job):
		#TODO MEH?
		self.ended_jobs.append(job)
		job.camp.job_ended(job)


def parse_workload(swf_file, serial):
	"""
	Parse a workload file and return a list of "Jobs":
		- swf_file - path to workload file
		- serial - change parallel jobs to multiple serial ones
	"""
	jobs = []
	for line in open(swf_file):
		if line[0] != ';': # skip comments
			stats = map(int, line.split())
			if stats[SWF.run_time] <= 0 or stats[SWF.proc] <= 0:
				continue
			if serial:
				count = stats[SWF.proc]
				stats[SWF.proc] = 1
			else:
				count = 1
			for i in range(count):
				jobs.append( Job(stats) )
	return jobs
