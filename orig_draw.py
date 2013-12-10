#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import math
import argparse
import numpy as np
import matplotlib.pyplot as plt

# field numbers in slurmctld log files
JOB_ID = 0
USER_ID = 1
CAMP_ID = 2
SUBMIT = 3
WAIT = 4
RUNTIME = 5
PROC = 6


class Job(object):
	def __init__(self, stats, context):
		self.j = stats
		self.ctx = context
		if 'fifo' not in self.ctx['slurm_config'] and self.j[WAIT] > 0:
			self.j[WAIT] -= 1
			self.j[SUBMIT] += 1
		#TODO
		#if 'fifo' not in self.ctx['slurm_config'] and self.j[WAIT] > 0:
			#self.j[WAIT] -= 1
			#self.j[SUBMIT] += 1
	@property
	def user(self):
		return self.j[USER_ID]
	@property
	def campID(self):
		return self.j[CAMP_ID]
	@property
	def ID(self):
		return self.j[JOB_ID]
	@property
	def proc(self):
		return self.j[PROC]
	@property
	def start(self):
		return self.j[SUBMIT]
	@property
	def runtime(self):
		return self.j[RUNTIME]
	@property
	def end(self):
		return self.j[SUBMIT] + self.j[WAIT] + self.j[RUNTIME]
	@property
	def stretch(self):
		v = float(self.j[WAIT] + self.j[RUNTIME]) / self.j[RUNTIME]
		return round(v, 2)
	def active_cpus(self, t):
		#check if job was running at time 't'
		if self.start + self.j[WAIT] <= t < self.end:
			return self.proc
		return 0
	def __repr__(self):
		return u"wait {}, runtime {}".format(self.j[WAIT], self.j[RUNTIME])

class Campaign(object):
	def __init__(self, jobs, context):
		self.jobs = jobs
		self.ctx = context
	@property
	def user(self):
		return self.jobs[0].user
	@property
	def ID(self):
		return self.jobs[0].j[CAMP_ID]
	@property
	def start(self):
		return min(map(lambda j: j.start, self.jobs))
	@property
	def runtime(self):
		return sum(map(lambda j: j.runtime, self.jobs))
	@property
	def end(self):
		return max(map(lambda j: j.end, self.jobs))
	@property
	def stretch(self):
		avg = float(self.runtime) / self.ctx['cpus']
		longest = max(map(lambda j: j.runtime, self.jobs))
		lb = max(avg, longest)
		return round(float(self.end - self.start)/lb, 2)
	@property
	def x_key(self):
		return "({} {} {})".format(self.user, self.ID, self.runtime)
	@property
	def key_desc(self):
		return "[User, ID, Runtime]"

class User(object):
	def __init__(self, camps, context):
		self.camps = camps
		self.ctx = context
	@property
	def ID(self):
		return self.camps[0].user
	@property
	def runtime(self):
		return sum(map(lambda c: c.runtime, self.camps))
	@property
	def stretch(self):
		total = self.runtime
		weighted = sum(map(lambda c: (c.stretch*c.runtime)/total, self.camps))
		return round(weighted, 2)
	@property
	def x_key(self):
		return "({} {} {})".format(self.ID, len(self.camps), self.runtime)
	@property
	def key_desc(self):
		return "[ID, Campaigns, Runtime]"


def _get_color(i):
	c = ['r', 'g', 'b', 'c', 'm', 'y', 'k']
	return c[i]

def cdf(data, key, **kwargs):
	""" Stretch CDF """
	plt.xlabel('stretch')
	plt.axis([1, 10, 0, 1])

	for i, d in enumerate(data):
		values = {}
		for ele in d[key]:
			values[ele.stretch] = values.pop(ele.stretch, 0) + 1

		values = sorted(values.items(), key=lambda x: x[0])
		x, y = [], []

		act, total = 0.0, len(d[key])
		for p, k in values:
			act += k
			x.append(p)
			y.append(act/total)
		plt.plot(x, y, color=_get_color(i),
				label=d['context']['title'])

def _label_values(bars, max_y):
	for i, bar in enumerate(bars):
		count = len(bars)

		if i % count == 0:
			direction = 'right'
		elif i % count == count - 1:
			direction = 'left'
		else:
			direction = 'center'

		for rect in bar:
			height = rect.get_height()
			if height >= max_y:
				plt.text(rect.get_x(), max_y, '%d'%int(height),
							va='bottom', ha=direction)

def weighted(data, key, **kwargs):
	"""// Campaigns Weighted Average """

	max_y = 10
	width = 0.55

	plot_count = len(data)
	step = int( math.ceil(plot_count * width) )

	bars = []

	for i, d in enumerate(data):
		ele_count = len(d[key])

		x = np.arange(0, ele_count * step, step)
		x_desc = map(lambda e: e.ID, d[key])
		y = map(lambda e: e.stretch, d[key])

		ax = plt.bar(
				x + i*width, y,
				width=width-0.15,
				color=_get_color(i),
				label=d['context']['title'],
				linewidth=0
			)
		bars.append(ax)

	plt.xticks(x + width, x_desc)
	plt.xlabel(key)

	plt.axis([0, ele_count * step + 1, 0, max_y])
	_label_values(bars, max_y)

def _runtime_func(job):
	real = int(job.runtime * job.ctx['time_scale'])

	small = 60 * 10			# 10 mins
	big = 60 * 60 * 4		# 4h
	huge = 60 * 60 * 24		# 1 day

	if real >= huge:
		return (real/huge + 1) * 1440
	elif real >= big:
		return (real/big + 1) * 240
	else:
		return (real/small + 1) * 10

def _math_stats(l):
	def average(s):
		return sum(s) * 1.0 / len(s)
	d = {}
	d['avg'] = average(l)
 	d['std_dev'] = math.sqrt(average(map(lambda x: (x - d['avg'])**2, l)))
 	#print d, l
	return d

def std(data, key, f):
	"""// Jobs Standard Deviation """

	max_y = 10
	width = 0.55

	plot_count = len(data)
	step = int( math.ceil(plot_count * width) )

	bars = []

	for i, d in enumerate(data):
		points = {}
		for job in d['jobs']:
			k = f(job)
			v = points.pop(k, [])
			v.append(job.stretch)
			points[k] = v

		points = sorted(points.items(), key=lambda x: x[0])
		ele_count = len(points)

		x = np.arange(0, ele_count * step, step)
		x_desc, y, yerr = [], [], []

		for (k, v) in points:
			x_desc.append(k)
			y.append(_math_stats(v)['avg'])
			yerr.append(_math_stats(v)['std_dev'])

		ax = plt.bar(
			x + i*width, y,
			#yerr=yerr,
			width=width-0.15,
			color=_get_color(i),
			ecolor=_get_color(i),
			label=d['context']['title'],
			linewidth=0
		)
		bars.append(ax)

	plt.xticks(x + width, x_desc)
	plt.xlabel(key)

	plt.axis([0, ele_count * step + 1, 0, max_y])
	_label_values(bars, max_y)

def utility(data, key, base):
	""" Utility """

	cpus = data[0]['context']['cpus']

	x_end = 20000
	shift = 6000

	for i, d in enumerate(data):
		values = d['jobs']

		#end = max(map(lambda j: j.end, values))
		#x_end = max(end, x_end)

		x, y = [], []

		for t in xrange(0 + shift, x_end, 1):
			act_jobs = sum(map(lambda j: j.active_cpus(t), values))

			x.append(t)
			y.append(act_jobs/float(cpus))

		plt.plot(x, y, color=_get_color(i),
				label=d['context']['title'])

	plt.xlabel('time')
	plt.axis([0 + shift, x_end, 0.8, 1])

def area(data, key, base):
	"""// Jobs Total Runtime """
	v = base or data[0]
	values = v['users']

	scale = v['context']['time_scale']
	y_desc = 60 * 60	# 1 hour

	x = np.arange(len(values))
	x_desc = map(lambda u: u.ID, values)
	y = map(lambda u: u.runtime*scale/y_desc, values)

	sim = v['context']['sim_time']*scale/y_desc

	plt.bar(x, y, color='r', label=str(int(sim)) + 'h simulation')

	plt.xticks(x, x_desc)
	plt.xlabel('user')
	plt.ylabel('hours')

def heatmap(d):
	""" Heatmap """

	cpus = d['context']['cpus']
	freq = 10.0
	max_y = 10

	v = {}
	for i in range(1, 10+1):
		v[i/10.0] = {j/freq:0 for j in range(1, max_y * int(freq) + 1)}

	for c in d['campaigns']:
		act_jobs = sum(map(lambda j: j.active_cpus(c.start), d['jobs']))

		ut = act_jobs/float(cpus)
		ut = math.ceil(ut * 10.0) / 10.0

		s = math.ceil(c.stretch * freq) / freq

		v[ut][min(max_y, s)] += 1

	v = sorted(v.items(), key=lambda x: x[0])		# sort by utility

	heat = []
	for i, j in v:
		j = sorted(j.items(), key=lambda x: x[0])	# sort by stretch
		heat.extend(map(lambda x: x[1], j))			# add values

	heat = np.array(heat)
	heat.shape = (10, max_y * int(freq))

	plt.pcolor(heat.T, label='b', cmap=plt.cm.hot_r)
	plt.colorbar()

	x_ax = np.arange(10)
	plt.xticks(x_ax, x_ax/10.0)
	plt.xlabel('utility')

	y_ax = np.arange(0, max_y * int(freq), int(freq))
	plt.yticks(y_ax, y_ax/freq)
	plt.ylabel('stretch')


def _find_camp(stats, jobs):
	for i, job in enumerate(jobs):
		if job.ID == stats[JOB_ID] and job.user == stats[USER_ID]:
			return job.campID
	else:
		#return None # TODO DELETE
		print "ERROR: Job ID not found:", stats
		sys.exit(1)

def parse(d, time_filter=0, base=None):
	from ast import literal_eval

	for line in open(os.path.join(d, "sim.trace")):
		if line.startswith('{'):
			context = literal_eval(line)
			break
	else:
		print "ERROR: No context found"
		sys.exit(1)

	if base:
		for key, val in base['context'].items():
			if key not in ["title", "move_jobs", "slurm_config", "user_id"]: # this can be different
				if context[key] != val and key != "swf_file": # TODO DEL SWF_FILE, ZROBIC ZEBY TEN PARAMETR TYLKO NAZWE PLLIKU MIAL AN IE CALA SCEIZKE!!!!
					print "ERROR: Different context between simulations"
					print key, context[key], val #TODO DEL
					sys.exit(1)

	jobs_log = []
	for line in open(os.path.join(d, "slurmctld.log")):
		if "OStrich Log" in line:
			job_stats = line.split(":")[-1].split()
			job_stats = map(int, job_stats[:-1])

			if job_stats[RUNTIME] <= time_filter:	# filter out very short jobs
				continue
			job_stats[USER_ID] -= context['user_id']

			# use base to create campaigns in 'fifo-type' simulations
			if job_stats[CAMP_ID] == -1 and base:
				job_stats[CAMP_ID] = _find_camp(job_stats, base['jobs'])
			if job_stats[CAMP_ID] is not None: # TODO DEL
				jobs_log.append(job_stats)

	first_submit = min(map(lambda j: j[SUBMIT], jobs_log))
	for job_stats in jobs_log:
		job_stats[SUBMIT] -= first_submit

	jobs = sorted(jobs_log, key=lambda x: x[JOB_ID])
	jobs = [Job(job_stats, context) for job_stats in jobs]

	campaigns = {}
	for job in jobs:
		key = (job.user, job.campID)
		c = campaigns.pop(key, [])
		c.append(job)
		campaigns[key] = c

	campaigns = sorted(campaigns.items(), key=lambda x: x[0])
	campaigns = [Campaign(job_list, context) for key, job_list in campaigns]

	users = {}
	for c in campaigns:
		u = users.pop(c.user, [])
		u.append(c)
		users[c.user] = u

	users = sorted(users.items(), key=lambda x: x[0])
	users = [User(camp_list, context) for key, camp_list in users]

	return {'context': context,
			'jobs': jobs,
			'campaigns': campaigns,
			'users': users}

def run_draw(args):

	time_filter = 1

	if args.base_camp is None:
		base = None
	else:
		base = parse(args.base_camp)

	data = [parse(d, time_filter, base) for d in args.dirs]

#	print "Campaign number", len(data[0]['campaigns']),
#	print "Short", len(filter(lambda x: x.runtime <= 10, data[0]['campaigns'])),
#	print "Single", len(filter(lambda x: len(x.jobs) == 1, data[0]['campaigns']))
#	print "Short and single(2)", len(filter(lambda x: x.runtime <= 2 and len(x.jobs) == 1, data[0]['campaigns'])),
#	print "Short and single(5)", len(filter(lambda x: x.runtime <= 5 and len(x.jobs) == 1, data[0]['campaigns'])),
#	print "Short and single(10)", len(filter(lambda x: x.runtime <= 10 and len(x.jobs) == 1, data[0]['campaigns'])),
#	print "Short and single(60)", len(filter(lambda x: x.runtime <= 60 and len(x.jobs) == 1, data[0]['campaigns']))
#	sas = filter(lambda x: x.runtime <= 60 and len(x.jobs) == 1, data[0]['campaigns'])
#	for c in sorted(sas, key=lambda x: x.stretch, reverse=True):
#		print "(", c.stretch, c.runtime, ")",
#	print
#	print
#	sys.exit(1)

	# create selected graphs
	graphs = [
		(cdf, "jobs", {}),
		(cdf, "campaigns", {}),
		#(weighted, "users", {}),
		#(utility, "total", {'base': base}),
		#(std, "runtime", {'f': _runtime_func}),
		#(std, "user", {'f': lambda j: j.user}),
		#(area, "user", {'base': base}),
	]

	out = args.dirs[0]

	for i, (g, key, kwargs) in enumerate(graphs):
		fig = plt.figure(i, figsize=(16, 9))				# size is in inches
		g(data, key, **kwargs)								# add plots
		plt.legend(loc=4)										# add legend
		plt.title(key.capitalize() + " " + g.__doc__,
					y=1.05, fontsize=20)					# add title
		fname = key.capitalize() + "_" + g.__name__
		fig.savefig(os.path.join(out, fname),
			format='pdf')									# save to file

	if 0:
		done = len(graphs)
		for i, d in enumerate(data):
			fig = plt.figure(done + i, figsize=(16, 9))
			heatmap(d)
			plt.title(d['context']['title'] + " Heatmap",
						y=1.05, fontsize=20)
			fname = "Heatmap_" + str(i)
			fig.savefig(os.path.join(out, fname), format='pdf')


	if 0:
		for c in data[0]['campaigns']:
			if c.stretch >= 20:
				print c.stretch, c.x_key, len(c.jobs)


if __name__=="__main__":

	parser = argparse.ArgumentParser(description="Draw graphs from logs")
	parser.add_argument('dirs', nargs='+',
						help="List of directories, each with log and trace file")
	parser.add_argument('--base_camp',
						help="Directory containing job-to-campaign mapping")
	run_draw(parser.parse_args())
