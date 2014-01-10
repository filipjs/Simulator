#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import argparse
import math
import matplotlib.pyplot as plt
import numpy as np
import os
import sys


class Job(object):
	def __init__(self, core, *args):
		self.core = core
		self.ID = args[0]
		self.camp = args[1]
		self.user = args[2]
		self.submit = args[3]
		self.start = args[4]
		self.end = args[5]
		self.estimate = args[6]
		self.time_limit = args[7]
		self.proc = args[8]
		# derivative properties
		self.run_time = self.end - self.start
		self.wait_time = self.start - self.submit
		# calculate stretch
		self.stretch = self._get_stretch()

	def _get_stretch(self):
		v = float(self.run_time + self.wait_time) / self.run_time
		return round(v, 2)

	def __repr__(self):
		return u"wait {}, runtime {}".format(self.wait_time, self.run_time)


class Campaign(object):
	def __init__(self, core, *args):
		self.core = core
		self.ID = args[0]
		self.user = args[1]
		self.start = args[2]
		self.utility = args[3]
		self.system_proc = args[4]
		self.end = None
		self.workload = None
		self.jobs = []

	def finalize(self, *args):
		assert self.ID == args[0], 'wrong campaign'
		assert self.user == args[1], 'wrong user'
		self.end = args[2]
		self.workload = args[3]
		assert len(self.jobs) == args[4], 'missing jobs'
		self.longest_job = max(map(lambda j: j.run_time, self.jobs))
		# calculate stretch
		self.stretch = self._get_stretch()

	def _get_stretch(self):
		avg = float(self.workload) / self.system_proc
		lower_bound = max(avg, self.longest_job)
		length = float(self.end - self.start)
		return round(length / lower_bound, 2)

	#@property
	#def x_key(self):
		#return "({} {} {})".format(self.user, self.ID, self.runtime)

	#@property
	#def key_desc(self):
		#return "[User, ID, Runtime]"


class User(object):
	def __init__(self, core, ID):
		self.core = core
		self.ID = ID
		self.jobs = []
		self.camps = []

	def finalize(self, *args):
		assert self.ID == args[0], 'wrong user'
		#assert len(self.jobs) == args[1], 'missing jobs'
		#assert len(self.camps) == args[2], 'missing camps'
		self.lost_virt = args[3]
		self.false_inact = args[4]
		# calculate stretch
		self.stretch = self._get_stretch()

	def _get_stretch(self):
		total = sum(map(lambda c: c.stretch, self.camps))
		avg = total / len(self.camps)
		return round(avg, 2)

	#@property
	#def x_key(self):
		#return "({} {} {})".format(self.ID, len(self.camps), self.runtime)
	#@property
	#def key_desc(self):
		#return "[ID, Campaigns, Runtime]"


def _get_color(i):
	c = ['r', 'g', 'b', 'c', 'm', 'y', 'k']
	#c = ['r', '0.0', '0.4']
	return c[i]

def cdf(data, key, **kwargs):
	""" Stretch CDF """
	plt.xlabel('stretch')
	plt.ylabel('fraction of ' + key)

	plt.xscale('log', subsx=[])
	plt.axis([1, 800, 0.3, 1])
	plt.xticks([1, 10, 100, 500], [1, 10, 100, 500])

	for i in range(3):
		values = {}
		act, total = 0.0, 0

		for sim in data:
			d = sim[i]

			for ele in d[key]:
				values[ele.stretch] = values.pop(ele.stretch, 0) + 1

			total += len(d[key])

		values = sorted(values.items(), key=lambda x: x[0])
		x, y = [], []

		for p, k in values:
			act += k
			x.append(p)
			y.append(act/total)

		plt.plot(x, y, color=_get_color(i),
				label=_get_label(i), lw=2)

def job_runtime(data, key, **kwargs):
	""" Stretch CDF """
	plt.xlabel('job runtime')
	plt.ylabel('average stretch')

	plt.xscale('log', subsx=[])
	plt.axis([2, 10000, 0, 60])
	plt.xticks([2, 20, 120, 120*24], ['1min', '10min', '1hour', '1day'])

	for i in range(1):
		values = {}

		for sim in data:
			d = sim[i]

			for ele in d[key]:
				kk = ele.runtime / 2
				j = values.pop(kk, [])
				j.append(ele.stretch)
				values[kk] = j

		x, y = [], []

		for k, v in sorted(values.items(), key=lambda x: x[0]):
			x.append(k*2)
			y.append(round(sum(v) / float(len(v)), 2))

		plt.plot(x, y, color=_get_color(i), lw=2)
				#label=_get_label(i), lw=2)

def diff_heat(data, key, **kwargs):
	""" Heatmap """

	max_y = 10

	v = {}
	for i in range(11, 20+1): # utility co 0.05
		ut = (i*5) / 100.
		v[ut] = {j/10.:0 for j in range(1, max_y * 10 + 1)} # stretch <1, max_y> co 0.1

	for i in range(2):

		if i == 0:
			m = -1	# ostr
		else:
			m = +1	# fair

		for sss, sim in enumerate(data):
			print "camp", sss

			d = sim[i]

			cpus = d['context']['cpus']

			for c in d['campaigns']:
				#act_jobs = sum(map(lambda j: j.active_cpus(c.start), d['jobs']))
				#ut = act_jobs/float(cpus)

				ut = c.utility
				ut = max(ut, 0.51)
				ut = math.ceil(ut * 20.) / 20.

				s = math.ceil(c.stretch * 10.) / 10.

				v[ut][min(max_y, s)] += m

	v = sorted(v.items(), key=lambda x: x[0])		# sort by utility

	heat = []
	for i, j in v:
		j = sorted(j.items(), key=lambda x: x[0])	# sort by stretch
		heat.extend(map(lambda x: x[1], j))			# add values

	heat = np.array(heat)
	heat.shape = (10, max_y * 10)

	#plt.pcolor(heat.T, label='b', cmap=plt.cm.hot_r)
	plt.pcolor(heat.T, label='b', cmap='RdBu', vmin=-300, vmax=300)
	plt.colorbar(shrink=0.7)

	x_ax = np.arange(0, 11)
	plt.xticks(x_ax, x_ax/20. + 0.5)
	plt.xlabel('cluster utility at the time of campaign submission')

	y_ax = np.arange(0, (max_y + 1) * 10, 10)
	plt.yticks(y_ax, map(str, y_ax/10.)[:-1] + [str(max_y) + " or more"])
	plt.ylabel('campaign stretch')


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

	#plt.axis([60, 140, 0, 25])
	plt.axis([0, 137, 0, 60])
	#plt.axis([0, 140, 0, 60])
	#plt.xticks([])
	plt.xlabel('users')
	plt.ylabel('stretch')

	for i in range(3):
		users = {}
		for sim in data:
			d = sim[i]

			for c in d['campaigns']:
				u = users.pop(c.user, [])
				u.append(c)
				users[c.user] = u

		users = [User(camp_list, None) for camp_list in users.values()]

		ustr = map(lambda u: u.stretch, users)
		y = sorted(ustr)
		#y = ustr
		x = range(len(y))

		plt.plot(x, y, color=_get_color(i),
			label=_get_label(i), lw=2)

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
	v = base or data[0]

	cpus = v['context']['cpus']
	values = v['jobs']

	st = min(map(lambda j: j.start, values))
	end = max(map(lambda j: j.end, values))

	x, y = [], []

	for t in xrange(st, end, 10):
		act_jobs = sum(map(lambda j: j.active_cpus(t), values))

		x.append(t - st)
		y.append(len(act_jobs)/float(cpus))

	plt.plot(x, y, 'r-', label=str(cpus) + ' cpus')

	plt.xlabel('time')
	plt.axis([0, end, 0, 1])

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

def heatmap(data):
	""" Heatmap """

	freq = 10.0
	max_y = 10

	v = {}
	for i in range(1, 10+1):
		v[i/10.0] = {j/freq:0 for j in range(1, max_y * int(freq) + 1)}

	for d in data:
		cpus = d['context']['cpus']
		for c in d['campaigns']:
			act_jobs = sum(map(lambda j: j.active_cpus(c.start), d['jobs']))

			ut = len(act_jobs)/float(cpus)
			ut = max(ut, 0.01)
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

	plt.pcolor(heat.T, label='b', cmap=plt.cm.hot_r, vmax=2200)
	plt.colorbar()

	x_ax = np.arange(10)
	plt.xticks(x_ax, x_ax/10.0)
	plt.xlabel('cluster utility at the time of campaign submission')

	y_ax = np.arange(0, (max_y + 1) * int(freq), int(freq))
	plt.yticks(y_ax, map(str, y_ax/freq)[:-1] + ["10 or more"])
	plt.ylabel('campaign stretch')


def _find_camp(stats, jobs):
	for i, job in enumerate(jobs):
		if job.ID == stats[JOB_ID] and job.user == stats[USER_ID]:
			return job.campID
	else:
		return None # TODO DELETE
		print "ERROR: Job ID not found:", stats
		sys.exit(1)

def parse(filename):

	def to_val(line):
		val = []
		for word in line:
			try:
				v = int(word)
			except ValueError:
				try:
					v = float(word)
				except ValueError:
					v = word  # not a number
			val.append(v)
		return val

	jobs = []
	camps = []
	users = {}
	utility = [(-1, -1)]  # guard at the start

	f = open(filename)
	# check file validity
	first = f.readline()
	assert first[0] == '{', 'missing context'

	block_camps = {}

	for line in f:
		tokens = line.split()
		prefix, entity, rest = tokens[0], tokens[1], to_val(tokens[2:])

		assert prefix in ['CORE', 'MARGIN'], 'invalid prefix'
		core = (prefix == 'CORE')

		if entity == 'JOB':
			job = Job(core, *rest)
			users[job.user].jobs.append(job)
			block_camps[(job.camp, job.user)].jobs.append(job)
			jobs.append(job)
		elif entity == 'CAMPAIGN':
			event, rest = rest[0], rest[1:]
			if event == 'START':
				c = Campaign(core, *rest)
				u = users.get(c.user, User(core, c.user))
				u.camps.append(c)
				users[u.ID] = u
				block_camps[(c.ID, c.user)] = c
			else:
				assert event == 'END'
				block_camps[(rest[0], rest[1])].finalize(*rest)
		elif entity == 'USER':
			if rest[0] in users:
				users[rest[0]].finalize(*rest)
			# else -> user without jobs in this block
		elif entity == 'UTILITY':
			if utility[-1][0] != rest[0]:
				utility.append([rest[0], rest[1]])
			else:
				utility[-1][1] = rest[1]
		elif entity == 'BLOCK':
			camps.extend(block_camps.itervalues())
			block_camps = {}
		else:
			assert entity in ['DIAG']
	# add last block
	camps.extend(block_camps.itervalues())
	# remove guard
	assert utility.pop(0) == (-1, -1), 'missing guard'
	f.close()

	return {'jobs': jobs,
		'campaigns': camps,
		'users': users,
		'utility': utility}


def run_draw(args):

	if args.output is not None:
		# before we start, check the output directory
		if not os.path.isdir(args.output):
			raise Exception('invalid output directory %s' % args.output)
	else:
		import itertools
		title = args.logs[0].split('-')[0]
		for i in itertools.count():
			suffix = '' if i == 0 else '(%s)' % i
			out = title + suffix
			if not os.path.exists(out):
				os.mkdir(out)
				args.output = out
				break

	data = {}

	for filename in args.logs:
		sched = filename.split('-')[1]
		data[sched] = parse(filename)

	# create selected graphs
	graphs = [
		#(cdf, "jobs", {}),
		#(cdf, "campaigns", {}),
		#(weighted, "users", {}),
		#(job_runtime, "jobs", {}),
		#(utility, "total", {'base': base}),
		#(std, "runtime", {'f': _runtime_func}),
		#(std, "user", {'f': lambda j: j.user}),
		#(area, "user", {'base': base}),
		#(diff_heat, "campaigns", {}),
	]

	out = "."

	for i, (g, key, kwargs) in enumerate(graphs):
		fig = plt.figure(i, figsize=(10, 7))				# size is in inches
		g(data, key, **kwargs)								# add plots
		#plt.legend(loc=2)									# add legend
		#plt.title(key.capitalize() + " " + g.__doc__,
					#y=1.05, fontsize=20)					# add title
		fname = key.capitalize() + "_" + g.__name__ + ".pdf"
		fig.savefig(os.path.join(out, fname),
			format='pdf')									# save to file

	if 0:
		done = len(graphs)
		for i in range(3):
			fig = plt.figure(done + i, figsize=(11, 7))
			d = [sim[i] for sim in data]
			heatmap(d)
			fname = "Heatmap_" + str(i)
			fig.savefig(os.path.join(out, fname), format='pdf')

	if 0:
		for c in data[0]['campaigns']:
			if c.stretch >= 20:
				print c.stretch, c.x_key, len(c.jobs)


if __name__=="__main__":

	parser = argparse.ArgumentParser(description='Draw graphs from logs')
	parser.add_argument('logs', nargs='+', help='List of log files to plot')
	parser.add_argument('--output', help='Directory to store the plots in')

	run_draw(parser.parse_args())
