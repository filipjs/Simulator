#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import argparse
import functools
import glob
import importlib
import multiprocessing
import os
import sys
import time
from core import parsers, simulator
from parts import settings


PROFILE_FLAG = True
PROFILE_FLAG = False


##
## Action ``run``.
##


class Block(object):
	"""
	A block uses a set of indexes to extract the appropriate
	job slice from the full list of jobs.

	Note: all indexes are inclusive:
	  left - (index of) the first job of the left margin
	  first - (index of) the first job of the core
	  last - (index of) the last job of the core
	  right - (index of) the last job of the right margin
	"""
	def __init__(self, jobs, inx, block_time, num):
		"""
		Args:
		  jobs: full list of jobs.
		  inx: dictionary with indexes.
		  block_time: core length.
		  num: block number
		"""
		self._jobs = jobs[inx['left']:inx['right']+1]
		self.core_count = inx['last'] - inx['first'] + 1
		self.margin_count = len(self._jobs) - self.core_count
		self.core_start = jobs[inx['first']].submit
		self.core_end = self.core_start + block_time
		self.number = num

	def __len__(self):
		return len(self._jobs)

	def __getitem__(self, key):
		return self._jobs[key]

	def __iter__(self):
		return iter(self._jobs)


def divide_jobs(jobs, first_job, block_time, block_margin):
	"""
	Divide the jobs into potentially many smaller blocks.
	Each block can have extra jobs to fill up and empty the cluster.

	Args:
	  jobs: list of all the jobs.
	  first_job: ID of the first job to start with or ``zero``.
	  block_time: length of each block in seconds or ``zero``.
	  block_margin: extra length added to the blocks on both sides.

	Returns:
	  a list of consecutive blocks as `Block` instances.
	"""
	if first_job:
		for i, j in enumerate(jobs):
			if j.ID == first_job:
				break
		else:
			print 'ERROR: job ID', first_job, 'not found'
			sys.exit(1)
	else:
		i = 0

	# 'i' now points to first job of the first block
	blocks = []

	while i < len(jobs):
		inx = {'first': i}
		st = jobs[i].submit
		end = st + block_time

		while i >= 0 and st - jobs[i].submit <= block_margin:
			i -= 1

		inx['left'] = i + 1

		if not block_time:
			inx['last'] = inx['right'] = len(jobs) - 1
			block_time = float('inf')
		else:
			i = inx['first']
			while i < len(jobs) and jobs[i].submit < end:
				i += 1
			inx['last'] = i - 1
			while i < len(jobs) and jobs[i].submit < end + block_margin:
				i += 1
			inx['right'] = i - 1

		blocks.append(
			Block(jobs, inx, block_time, len(blocks))
		)
		i = inx['last'] + 1  # margins from consecutive blocks can overlap

	return blocks


def cpu_percentile(block, percentile):
	"""
	Return the number of CPUs equal to the p-th `percentile`
	of the system resource usage over the block simulation period.

	Note:
	  We are assuming that the system has no CPU limit.
	"""
	events = {}  # pairs <time-stamp, change in the number of CPUs>
	last_event = block[-1].submit

	for j in block:
		events[j.submit] = events.get(j.submit, 0) + j.proc
		end = min(j.submit + j.run_time, last_event)
		events[end] = events.get(end, 0) - j.proc

	events = sorted(events.iteritems())  # sort by time-stamp
	prev_event, cpus = events[0][0], 0
	util = {}  # pairs <number of CPUs, total period with that CPU count>

	for time_stamp, diff in events:
		period = time_stamp - prev_event
		prev_event = time_stamp
		util[cpus] = util.get(cpus, 0) + period
		cpus += diff

	total = sum(util.itervalues())  # total simulation period
	find = int(percentile / 100.0 * total)

	# sort by CPU usage and find the percentile
	util = sorted(util.iteritems())
	elements = 0
	for cpus, period in util:
		elements += period
		if elements >= find:
			return cpus
	else:
		print 'ERROR: Invalid percentile:', percentile
		sys.exit(1)


def make_classes(name, conf, modules=[]):
	"""
	Return an instance of the class `name` from the ``parts`` package.
	Each class must take *exactly one* init argument `conf`.

	Args:
	  name: name of the class OR a list of names.
	  conf: argument passed to the class constructor.
	  modules: a cache with loaded modules from the `parts` package.

	"""
	if not modules:
		for f in glob.glob('parts/*.py'):
			m = f.replace('/', '.')[:-3]
			m = importlib.import_module(m)
			modules.append(m)

	if isinstance(name, list):
		return [make_classes(n, conf) for n in name]

	for m in modules:
		if hasattr(m, name):
			cl = getattr(m, name)
			return cl(conf)  # create instance
	else:
		raise Exception('class not found: ' + name)


def print_stats(diag):
	"""
	"""
	# change some stats to percentages
	diag.bf_jobs *= 100
	diag.sched_jobs *= 100
	diag.avg_util *= 100

	diag_msg = """  Backfilled jobs {bf_jobs:.2f}% sched jobs {sched_jobs:.2f}%
  Avg utilization {avg_util:.2f}% backfill loops {bf_pass} sched loops {sched_pass}
  Simulation time {sim_time:.2f} decay events {forced}"""
	print diag_msg.format(**diag.__dict__)


def simulate_block(block, nodes, alg_conf, part_conf):
	"""
	"""

	# extract the users and reset all instances
	users = {}
	for j in block:
		j.reset()
		users[j.user.ID] = j.user
	for u in users.itervalues():
		u.reset()

	my_simulator = simulator.Simulator(block, users, nodes,
					   alg_conf, part_conf)

	if not PROFILE_FLAG:
		return my_simulator.run()
	else:
		import cProfile
		cProfile.runctx('print my_simulator.run()[1]',
				globals(), locals(),
				sort='cumulative')


def run(workload, args):
	"""
	Run the simulation described in `args` on the `workload`.

	Run one simulation for each supplied ``scheduler``.
	"""

	# encapsulate different settings
	sim_conf = settings.Settings(settings.sim_templates, **args)
	alg_conf = settings.Settings(settings.alg_templates, **args)
	part_conf = settings.Settings(settings.part_templates, **args)

	# before we start, check the output directory
	if not os.path.isdir(sim_conf.output):
		print 'ERROR: invalid output directory', sim_conf.output
		sys.exit(1)

	# now we need to load and instantiate the classes from `part_conf`
	for key, value in part_conf.__dict__.items():
		setattr(part_conf, key, make_classes(value, alg_conf))

	# parse the workload
	parser = parsers.get_parser(workload)

	jobs, users = parser.parse_workload(workload, sim_conf.serial)
	jobs.sort(key=lambda j: j.submit)  # order by submit time

	# set the missing job attributes
	for j in jobs:
		j.time_limit = part_conf.submitter.time_limit(j)
		part_conf.submitter.modify_configuration(j)
		j.validate_configuration()

	# set the missing user attributes
	shares = {}
	for uid, u in users.iteritems():
		shares[uid] = part_conf.share.user_share(u)
	# shares must be normalized
	total_shares = sum(shares.itervalues())
	for uid, u in users.iteritems():
		u.shares = float(shares[uid]) / total_shares

	# divide into blocks
	blocks = divide_jobs(jobs, sim_conf.job_id, sim_conf.block_time,
			     sim_conf.block_margin)
	if sim_conf.one_block:
		blocks = blocks[:1]

	if not PROFILE_FLAG:
		# Prepare the worker pool. Leave one CPU free,
		# so the operating system can stay responsive.
		my_pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)

		async_results = {sched: [None] * len(blocks)
				 for sched in part_conf.schedulers}

	block_msg = 'Block {:2} (first id {}): {} scheduler {} jobs' \
		    ' (inc. {} margin jobs) {} CPUs'
	if not sim_conf.cpu_count:
		block_msg += ' ({}-th percentile)'.format(sim_conf.cpu_percent)

	global_start = time.time()

	print '-' * 50
	print 'Simulation STARTED. Block count', len(blocks)

	# start with blocks with highest job count
	for bl in sorted(blocks, key=lambda x: len(x), reverse=True):
		# calculate the CPU number
		if sim_conf.cpu_count:
			cpus = sim_conf.cpu_count
		else:
			cpus = cpu_percentile(bl, sim_conf.cpu_percent)
		# setup the node configuration
		if sim_conf.cpu_per_node:
			full_count = cpus / sim_conf.cpu_per_node
			nodes = {i: sim_conf.cpu_per_node
				 for i in xrange(full_count)}
			rest = cpus % sim_conf.cpu_per_node
			if rest:
				nodes[len(nodes)] = rest
		else:
			nodes = {0: cpus}

		for sched in part_conf.schedulers:
			# set the current scheduler
			part_conf.scheduler = sched

			params = (bl, nodes, alg_conf, part_conf)
			msg = block_msg.format(bl.number, bl[0].ID, sched,
					       len(bl), bl.margin_count, cpus)

			if not PROFILE_FLAG:
				async_r = my_pool.apply_async(simulate_block, params)
				async_results[sched][bl.number] = (async_r, msg)
			else:
				print msg
				simulate_block(*params)

	if PROFILE_FLAG:
		return

	# wait for the results and than save them
	for sched, sim_results in async_results.iteritems():
		time_stamp = time.localtime(global_start)
		filename = '{}-{}-{}'.format(
			sim_conf.title,
			sched,
			time.strftime('%b-%d-%H:%M', time_stamp)
		)
		filename = os.path.join(sim_conf.output, filename)

		f = open(filename, 'w')
		f.write(str(args) + '\n')  # full parameters

		for async_r, msg in sim_results:
			print msg
			# ctr-c doesn't seem to work without timeout
			r, diag = async_r.get(timeout=60*60*24*365)
			print_stats(diag)
			# save partial results to file
			f.write('NEXT BLOCK\n')
			f.writelines( '%s\n' % line for line in r )

		f.close()
		print 'Saving results COMPLETED.', filename

	print 'Simulation COMPLETED. Total run time',
	print round(time.time() - global_start, 2)
	print '-' * 50


##
## Action ``config``.
##


def config(args):
	"""
	Create a configuration file based on the `Template` lists.
	Generating takes default values.
	Recreating takes values from a file with the simulation results.
	"""
	if args['generate']:
		values = {}
	else:
	        from ast import literal_eval
		with open(args['recreate']) as f:
			line = f.readline()
			if line[0] == '{':
				values = literal_eval(line)
			else:
				print 'ERROR: no context in file:', args['recreate']
				sys.exit(1)

	def str_value(value):
		"""
		Change the value to a printable version.
		"""
		if isinstance(value, list):
			return ' '.join(map(str, value))
		return str(value)

	def print_template(temp):
		"""
		Print a `Template`.
		"""
		value = values.get(temp.name, temp.default)
		unit = temp.time_unit or ''

		print '--{:15}{}'.format(temp.name, str_value(value))
		print '# {}: (default) {} {}'.format(
		           temp.desc, str_value(temp.default), unit)
		if temp.loc is not None:
			print '# Used by `{}` class.'.format(temp.loc)

	print '\n##\n## General simulation parameters\n##\n'
	map(print_template, settings.sim_templates)
	print '\n##\n## Algorithm specific parameters\n##\n'
	map(print_template, settings.alg_templates)
	print '\n##\n## Part selection parameters\n##\n'
	map(print_template, settings.part_templates)


##
## Argument parsing
##


run_opts = '[SIM_OPTS][ALG_OPTS][PART_OPTS] workload_file'

global_desc = """INSTRUCTIONS
----------------------------------------------------------------
To run a cluster simulation:
    `%(prog)s run {}`
To read the options from a config file:
    `%(prog)s run @myconfig workload_file`

You can generate a template of the configuration:
    `%(prog)s config --generate`.
You can also recreate a config from a simulation:
    `%(prog)s config --recreate sim_file`
----------------------------------------------------------------
""".format(run_opts)


def arguments_from_templates(parser, templates):
	"""
	Add arguments to the parser based on the `Template` list.
	"""

	def str2bool(v):
		if v.lower() not in ['true', 'false']:
			raise argparse.ArgumentTypeError('choose from True, False')
		return v.lower() == 'true'

	def positive(atype, v):
		v = atype(v)
		if v < 0:
			raise argparse.ArgumentTypeError('negative value')
		return v

	for temp in templates:
		assert temp.default is not None, 'invalid default value'

		opts = {'metavar': temp.time_unit,
			'default': temp.default,
			'help': temp.desc}

		ftypes = {bool: str2bool,
			  int: functools.partial(positive, int),
			  float: functools.partial(positive, float),
			  str: str}

		if isinstance(temp.default, list):
			# multiple values
			opts['type'] = ftypes[type(temp.default[0])]
			opts['nargs'] = '*'
		else:
			opts['type'] = ftypes[type(temp.default)]

		parser.add_argument('--' + temp.name, **opts)


class MyHelpFormatter(argparse.HelpFormatter):
	"""
	`HelpFormatter` with custom graphical parameters.
	"""

	def __init__(self, prog, **kwargs):
		kwargs['max_help_position'] = 40
		kwargs['indent_increment'] = 4
		kwargs['width'] = 100
		argparse.HelpFormatter.__init__(self, prog, **kwargs)


class MyArgumentParser(argparse.ArgumentParser):
	"""
	`ArgumentParser` with custom file parsing.
	"""

	def convert_arg_line_to_args(self, arg_line):
		"""
		Parse the file skipping the comments and using
		any whitespace as an argument delimiter.
		"""
		if arg_line.startswith('#'):
			return
		for arg in arg_line.split():
			if not arg.strip():
				continue
			yield arg


if __name__=="__main__":

	parser = MyArgumentParser(description=global_desc,
				  formatter_class=argparse.RawDescriptionHelpFormatter)
	subparsers = parser.add_subparsers(dest='command', help='Select a command')

	# run simulation parser
	run_parser = subparsers.add_parser('run', help='Run a simulation',
					   usage='%(prog)s {}'.format(run_opts),
					   fromfile_prefix_chars='@',
					   formatter_class=MyHelpFormatter)
	run_parser.add_argument('workload', help='The workload file')

	sim_group = run_parser.add_argument_group('General simulation parameters')
	arguments_from_templates(sim_group, settings.sim_templates)

	alg_group = run_parser.add_argument_group('Algorithm specific parameters')
	arguments_from_templates(alg_group, settings.alg_templates)

	part_group = run_parser.add_argument_group('Part selection parameters')
	arguments_from_templates(part_group, settings.part_templates)

	# config parser
	config_parser = subparsers.add_parser('config', help='Create configuration')

	action_group = config_parser.add_mutually_exclusive_group(required=True)
	action_group.add_argument('--generate', action='store_true',
				  help='Generate a new configuration template.')
	action_group.add_argument('--recreate', metavar='SIM FILE',
				  help='Recreate the configuration from a simulation')

	args = vars(parser.parse_args())

	if args['command'] == 'run':
		run(args['workload'], args)
	elif args['command'] == 'config':
		config(args)
	else:
		print "Hmm...", args['command']
