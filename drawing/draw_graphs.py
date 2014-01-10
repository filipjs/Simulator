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

	def __repr__(self):
		return '{} {} {}'.format(len(self.jobs), self.workload, self.stretch)


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

	def __repr__(self):
		return '{} {}'.format(self.camps, self.stretch)


def _get_color(i):
	c = ['r', 'g', 'b', 'c', 'm', 'y', 'k']
	#c = ['r', '0.0', '0.4']
	return c[i]


def cdf(simulations, key, **kwargs):
	""" Stretch CDF """

	plt.xlabel('stretch')
	plt.ylabel('fraction of ' + key)

	plt.xscale('log', subsx=[])
	plt.axis([1, 250, 0.3, 1])
	plt.xticks([1, 10, 100], [1, 10, 100])

	for i, (sim, data) in enumerate(simulations.iteritems()):
		values = {}

		for ele in data[key]:
			values[ele.stretch] = values.pop(ele.stretch, 0) + 1

		values = sorted(values.items(), key=lambda x: x[0])
		x, y = [], []
		act = 0.0
		total = len(data[key])

		for stretch, count in values:
			act += count
			x.append(stretch)
			y.append(act/total)

		plt.plot(x, y, color=_get_color(i), label=sim)


def job_runtime(data, key, **kwargs):
	""" Stretch CDF """
	#TODO
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

def diff_heat(simulations, key, **kwargs):
	""" Heatmap """

	if len(simulations) != 2:
		print 'Difference heatmap only from two plots'
		return

	max_y = 10

	v = {}
	for i in range(11, 20+1): # utility incremented by 0.05
		ut = (i*5) / 100.
		# stretch <1, max_y> incremented by 0.1
		v[ut] = {j/10.:0 for j in range(1, max_y * 10 + 1)}

	for i, (sim, data) in enumerate(simulations.iteritems()):

		if i == 0:
			m = -1	# ostr
		else:
			m = +1	# fair

		for c in data['campaigns']:
			ut = c.utility
			ut = max(ut, 0.51)
			ut = math.ceil(ut * 20.) / 20.

			s = math.ceil(c.stretch * 10.) / 10.

			v[ut][min(max_y, s)] += m

	v = sorted(v.items(), key=lambda x: x[0])  # sort by utility

	heat = []
	for i, j in v:
		j = sorted(j.items(), key=lambda x: x[0])  # sort by stretch
		heat.extend(map(lambda x: x[1], j))  # add values

	heat = np.array(heat)
	heat.shape = (10, max_y * 10)

	#plt.pcolor(heat.T, label='b', cmap=plt.cm.hot_r)
	plt.pcolor(heat.T, label='b', cmap='RdBu', vmin=-300, vmax=300)
	plt.colorbar(shrink=0.7)

	x_ax = np.arange(0, 11)
	plt.xticks(x_ax, x_ax/20. + 0.5)
	plt.xlabel('cluster utilization at the time of campaign submission')

	y_ax = np.arange(0, (max_y + 1) * 10, 10)
	plt.yticks(y_ax, map(str, y_ax/10.)[:-1] + [str(max_y) + " or more"])
	plt.ylabel('campaign stretch')


def average(simulations, key, **kwargs):
	"""// Campaigns Average Stretch """

	plt.xlabel('users')
	plt.ylabel('stretch')

	for i, (sim, data) in enumerate(simulations.iteritems()):

		y = map(lambda u: u.stretch, data['users'].itervalues())
		y.sort()
		x = range(len(y))

		plt.plot(x, y, color=_get_color(i), label=sim)
	plt.axis([0, len(y), 0, 100])


def utility(simulations, key, **kwargs):
	""" Utilization """

	max_st = 0
	min_end = float('inf')

	for i, (sim, data) in enumerate(simulations.iteritems()):
		sparse = data['utility'][::500]
		x, y = zip(*sparse)
		print len(x)

		max_st = max(max_st, x[0])
		min_end = min(min_end, x[-1])

		plt.plot(x, y, color=_get_color(i), label=sim)

	plt.xlabel('time')
	plt.xticks([])
	plt.axis([max_st, min_end, 0, 1])


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

	if args.output is None:
		out = args.logs[0].split('-')[0]
	else:
		out = args.output

	if not os.path.exists(out):
		os.mkdir(out)  # doesn't exist
	elif os.listdir(out) and not args.override:
		# not empty and cannot override
		raise Exception('directory already exists %s' % out)

	simulations = {}

	for filename in args.logs:
		sim = '{0[0]} {0[1]}'.format(
			os.path.basename(filename).split('-'))
		simulations[sim] = parse(filename)

	# create selected graphs
	graphs = [
		(cdf, "jobs", {}),
		(cdf, "campaigns", {}),
		#(average, "users", {}),
		#(job_runtime, "jobs", {}),
		#(utility, "total", {}),
		(diff_heat, "campaigns", {}),
	]

	for i, (g, key, kwargs) in enumerate(graphs):
		fig = plt.figure(i, figsize=(10, 7))  # size is in inches

		g(simulations, key, **kwargs)  # add plots
		plt.legend(loc=2)  # add legend

		title = '{} {}'.format(key.capitalize(), g.__doc__)
		plt.title(title, y=1.05, fontsize=20)  # add title

		fname = '{}_{}.pdf'.format(key.capitalize(), g.__name__)
		fig.savefig(os.path.join(out, fname), format='pdf')  # save to file

	if 0:
		for c in data[0]['campaigns']:
			if c.stretch >= 20:
				print c.stretch, c.x_key, len(c.jobs)


if __name__=="__main__":

	parser = argparse.ArgumentParser(description='Draw graphs from logs')
	parser.add_argument('logs', nargs='+', help='List of log files to plot')
	parser.add_argument('--output', help='Directory to store the plots in')
	parser.add_argument('--override', action='store_true',
			    help='Override the output directory')

	run_draw(parser.parse_args())
