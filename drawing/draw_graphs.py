#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import argparse
import collections
import itertools
import math
import matplotlib.pyplot as plt
import numpy as np
import os
import sys


class Job(object):
	short_len = 10

	def __init__(self, *args):
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

	def calc_stretch(self):
		v = float(self.run_time + self.wait_time) / max(Job.short_len, self.run_time)
		v = max(1, v)
		self.stretch = round(v, 2)

	def __repr__(self):
		return u"wait {}, runtime {}".format(self.wait_time, self.run_time)


class Campaign(object):
	short_len = 10

	def __init__(self, *args):
		self.ID = args[0]
		self.user = args[1]
		self.start = args[2]
		self.utility = args[3]
		self.end = None
		self.workload = None
		self.jobs = []

	def finalize(self, system_proc, *args):
		assert self.ID == args[0], 'wrong campaign'
		assert self.user == args[1], 'wrong user'
		self.end = args[2]
		self.workload = args[3]
		assert len(self.jobs) == args[4], 'missing jobs'
		self._system_proc = system_proc

	def calc_stretch(self):
		avg = float(self.workload) / self._system_proc
		longest_job = max(map(lambda j: j.run_time, self.jobs))
		lower_bound = max(avg, longest_job)
		lower_bound = max(Campaign.short_len, lower_bound)
		length = float(self.end - self.start)
		self.stretch = round(length / lower_bound, 2)
		self.stretch = max(1, self.stretch)

	def __repr__(self):
		return '{} {} {}'.format(len(self.jobs), self.workload, self.stretch)


class User(object):
	def __init__(self, ID):
		self.ID = ID
		self.jobs = []
		self.camps = []

	def finalize(self, *args):
		assert self.ID == args[0], 'wrong user'
		self.lost_virt = args[3]
		self.false_inact = args[4]

	def _get_avg(self, container):
		total = sum(map(lambda x: x.stretch, container))
		avg = total / len(container)
		return round(avg, 2)

	def calc_stretch(self):
		self.stretch = self.camp_stretch = self._get_avg(self.camps)
		self.job_stretch = self._get_avg(self.jobs)

	def __repr__(self):
		return '{} {}'.format(self.camps, self.stretch)


def _get_color(i):
	c = ['r', 'g', 'b', 'c', 'm', 'y', 'k']
	#c = ['r', '0.0', '0.4']
	return c[i]


def cdf(simulations, key):
	""" Stretch CDF """

	plt.xlabel('stretch')
	plt.ylabel('fraction of ' + key)

	plt.xscale('log', subsx=[])
	plt.axis([1, 300, 0.25, 1])
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


def heatmap(simulations, key):
	""" Heatmap """

	if len(simulations) > 2:
		print 'Heatmap only from a maximum of two plots'
		return

	colorbar_range = 300
	max_y = 10

	v = {}
	for i in range(11, 20+1): # utility incremented by 0.05
		ut = (i*5) / 100.
		# stretch <1, max_y> incremented by 0.1
		v[ut] = {j/10.:0 for j in range(10, max_y * 10)}

	for i, (sim, data) in enumerate(simulations.iteritems()):

		if i == 0:
			m = +1
		else:
			m = -1

		for c in data['campaigns']:
			ut = c.utility
			ut = max(ut, 0.51)
			ut = math.ceil(ut * 20.) / 20.

			s = math.ceil(c.stretch * 10.) / 10.

			v[ut][min(max_y - 0.1, s)] += m

	v = sorted(v.items(), key=lambda x: x[0])  # sort by utility

	heat = []
	for i, j in v:
		j = sorted(j.items(), key=lambda x: x[0])  # sort by stretch
		heat.extend(map(lambda x: x[1], j))	 # add values

	heat = np.array(heat)
	heat.shape = (10, (max_y - 1) * 10)

	#plt.pcolor(heat.T, label='b', cmap=plt.cm.hot_r)
	plt.pcolor(heat.T, label='b', cmap='RdBu',
		   vmin=-colorbar_range, vmax=colorbar_range)
	plt.colorbar(shrink=0.7)

	x_ax = np.arange(0, 11)
	plt.xticks(x_ax, x_ax/20. + 0.5)
	plt.xlabel('cluster utilization at the time of campaign submission')

	y_ax = np.arange(0, max_y * 10, 10)
	plt.yticks(y_ax, map(str, y_ax/10. + 1)[:-1] + [str(max_y) + " or more"])
	plt.ylabel('campaign stretch')


def average_per_user(simulations, key):
	""" Average Stretch per User """

	plt.xlabel('users')
	plt.ylabel(key + ' stretch')

	if key == 'jobs':
		value = 'job_stretch'
	else:
		value = 'camp_stretch'

	for i, (sim, data) in enumerate(simulations.iteritems()):

		y = map(lambda u: getattr(u, value), data['users'].itervalues())
		y.sort()
		x = range(len(y))

		plt.plot(x, y, color=_get_color(i), label=sim)
	plt.axis([0, len(y), 0, 100])


def utility(simulations, key):
	""" Utilization """

	if len(simulations) > 2:
		print 'Utility graph only from a maximum of two plots'
		return

	def timeline(ut, step):
		padded = itertools.chain(ut, [(step-1, 0)])
		total, needed = 0.0, step
		for (period, value) in padded:
			while period > 0:
				avail = min(period, needed)
				period -= avail
				needed -= avail
				total += avail * value

				if not needed:
					yield total / step
					total, needed = 0.0, step

	count = 0

	for data in simulations.itervalues():
		total = sum(map(lambda u: u[0], data['utility']))
		count = max(count, total)

	graph_points = 200.
	period = int(math.ceil( count / graph_points ))

	plots = []

	for data in simulations.itervalues():
		y = list(timeline(data['utility'], period))
		plots.append(y)

	if len(plots) == 1:
		plt.ylabel('utilization')
		y = plots[0]
		plt.axis([0, len(y), 0, 1])
	else:
		plt.ylabel('difference in utilization')
		plot = itertools.izip_longest(plots[0], plots[1], fillvalue=0)
		y = map(lambda (a, b): a - b, plot)
		plt.axis([0, len(y), -0.5, 0.5])
		plt.axhline(color='k', ls='--')

	x = range(len(y))
	plt.plot(x, y, 'r-')

	plt.xlabel('time')
	plt.xticks([])


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
	utility = []

	f = open(filename)
	# check file validity
	for line in f:
		if line[0] == '#':
			continue  # skip comments
		else:
			assert line[0] == '{', 'missing context'
			break

	block_camps = {}
	block_cpus = None

	for line in f:
		tokens = line.split()
		event, rest = tokens[0], to_val(tokens[1:])

		if event in ['CORE', 'MARG']:
			true_event = rest.pop(0)
			core = (event == 'CORE')

			if true_event == 'JOB':
				job = Job(*rest)
				job.core = core
				jobs.append(job)
				block_camps[(job.camp, job.user)].jobs.append(job)
				users[job.user].jobs.append(job)
			else:
				assert true_event == 'CAMP'
				event_type = rest.pop(0)

				if event_type == 'START':
					c = Campaign(*rest)
					c.core = core
					block_camps[(c.ID, c.user)] = c
					u = users.get(c.user, User(c.user))
					u.camps.append(c)
					users[u.ID] = u
				else:
					assert event_type == 'END'
					cid, uid = rest[0], rest[1]
					block_camps[(cid, uid)].finalize(block_cpus, *rest)
		elif event == 'USER':
			uid = rest[0]
			if uid in users:
				users[rest[0]].finalize(*rest)
			else:
				pass # user without core jobs in this block
		elif event == 'UTIL':
			utility.append([rest[0], rest[1]])
		elif event == 'BLOCK':
			event_type = rest.pop(0)

			if event_type == 'START':
				block_cpus = rest[0]
			else:
				assert event_type == 'END'
				camps.extend(block_camps.itervalues())
				block_camps = {}
				block_cpus = None
		elif event == 'SIMULATION':
			pass
		else:
			print event
			assert False, 'unknown event'
	f.close()

	def only_core(e):
		return e.core

	jobs = filter(only_core, jobs)
	camps = filter(only_core, camps)
	for uid, u in users.items():
		u.jobs = filter(only_core, u.jobs)
		u.camps = filter(only_core, u.camps)
		if not len(u.camps):
			del users[uid]

	for j in jobs:
		j.calc_stretch()
	for c in camps:
		c.calc_stretch()
	for u in users.itervalues():
		u.calc_stretch()

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

	simulations = collections.OrderedDict()

	for filename in args.logs:
		key = os.path.basename(filename)
		simulations[key] = parse(filename)

	# create selected graphs
	graphs = [
		(cdf, "jobs", 4),
		(cdf, "campaigns", 4),
		(average_per_user, "jobs", 2),
		(average_per_user, "campaigns", 2),
		(utility, "total", None),
		(heatmap, "campaigns", None),
	]

	for i, (g, key, legend) in enumerate(graphs):
		fig = plt.figure(i, figsize=(16, 9))  # size is in inches

		g(simulations, key)	 # add plots
		if legend:
			plt.legend(loc=legend)	# add legend

		title = '{} {}'.format(key.capitalize(), g.__doc__)
		plt.title(title, y=1.05, fontsize=20)  # add title

		fname = '{}_{}.pdf'.format(key.capitalize(), g.__name__)
		fig.savefig(os.path.join(out, fname), format='pdf')	 # save to file


if __name__=="__main__":

	parser = argparse.ArgumentParser(description='Draw graphs from logs')
	parser.add_argument('logs', nargs='+', help='List of log files to plot')
	parser.add_argument('--output', help='Directory to store the plots in')

	run_draw(parser.parse_args())
