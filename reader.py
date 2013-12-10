# -*- coding: utf-8 -*-
import functools

class SWF(object):
	"""
	Field numbers in a workload (.swf) file.
	"""
	job_id = 0
	submit = 1
	wait_time = 2
	run_time = 3
	proc = 4
	user_id = 11
	partition = 15

class Job(object):
	"""
	A single job with the all relevant properties.
	Correct usage scheme:
		reset -> setter.camp -> start_execution -> execution_ended
	"""
	def __init__(self, stats):
		self._stats = stats
	def reset(self):
		self._camp = None
		self._start = None
		self._completed = False
	@property
	def ID(self):
		return self._stats[SWF.job_id]
	@property
	def userID(self):
		return self._stats[SWF.user_id]
	@property
	def proc(self):
		return self._stats[SWF.proc]
	@property
	def submit(self):
		return self._stats[SWF.submit]
	@property
	def run_time(self):
		return self._stats[SWF.run_time]
	@property
	def start_time(self):
		return self._start
	@property
	def end_time(self):
		return self._start + self.run_time
	@property
	def completed(self):
		return self._completed
	@property
	def camp(self):
		return self._camp
	@setter.camp
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
		return "{} {} {} {} {} {} {}".format(self.ID,
			self.userID, self.camp.ID, self.submit,
			self.start_time, self.run_time, self.proc)

class Campaign(object):
	"""
	A user campaign with the appropriate jobs.
	A campaign is 'active' if it is still running in the virtual schedule.
	A campaign is 'completed' if it ended in the virtual AND the real schedule.
	"""
	def __init__(self, user, time_stamp):
		self._user = user
		self._id = user.campID
  		self._created = time_stamp
		self._remaining = 0
		self._completed = 0
		self.virtual = 0
		self.offset = 0
		self._jobs = []
	@property
	def ID(self):
		return self._id
	@property
	def created(self):
		return self._created
	@property
	def user(self):
		return self._user
	@property
	def workload(self):
		return self._remaining + self._completed
	@property
	def active(self):
		return self.workload - self.virtual > 0
	@property
	def completed(self):
		return not self.active and not self._jobs
	def add_job(self, job):
		self._remaining += job.estimate * job.proc
		self._jobs.append(job)
		job.camp = self # forward link
	def job_ended(self, job):
		self._remaining -= job.estimate * job.proc
		self._completed += job.runtime * job.proc
		self._jobs.remove(job)
	def sort_jobs(self, job_cmp):
		self._jobs = sorted(self._jobs, key=functools.cmp_to_key(job_cmp))
		#TODO teraz do kazdej pracy dodac pole 'kolejnosc w kampani' zeby
		# nie musiec robic za kazdym razem _jobs.index(job)

class User(object):
	"""
	User account with campaign list and fair-share usage.
	Campaigns are sorted by creation time.
	Ended_jobs are sorted by execution end time.
	"""
	def __init__(self, uid):
		self._id = uid
		self._camp_counter = 0
		self._camps = []
	def reset(self):
		assert not self._camps
		self.virtual = 0
		self.raw_usage = 0
		self.fair_share = 0
		self.ended_jobs = []
	@property
	def ID(self):
		return self._id
	@property
	def campID(self):
		return self._camp_counter
	@property
	def active(self):
		return self._camps and self._camps[-1].active
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
