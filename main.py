#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import functools
import glob
import importlib
import os
import sys
import time
from core import parsers, simulator
from parts import settings


##
## Action ``run``.
##


def divide_jobs(jobs, first_job, block_time, block_margin):
	"""
	Divide the jobs into potentially many smaller blocks.
	Each block can have extra jobs to fill up and empty the cluster.

	Args:
	  jobs: list of all the jobs.
	  first_job: ID of the first job to start with or ``zero``.
	  block_time: length of each block in seconds or ``zero``.
	  block_margin: extra length added to the block on both sides.

	Returns:
	  a list of consecutive blocks - a block is a dict of indexes
	    {left [margin start]:first [block job]:last [block job]:right [margin end]}
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
		b = {'first': i}
		st = jobs[i].submit

		while i >= 0 and st - jobs[i].submit <= block_margin:
			i -= 1

		b['left'] = i + 1
		i = b['first']

		while i < len(jobs) and (not block_time or jobs[i].submit - st <= block_time):
			i += 1

		b['last'] = i - 1

		while i < len(jobs) and jobs[i].submit - st <= block_time + block_margin:
			i += 1

		b['right'] = i - 1
		i = b['last']

		if not block_time or jobs[i].submit - st > block_time / 2:
			# block must be at least half the desired length
			blocks.append(b)
		# next block starts right after the previous one, excluding the margins
		i = i + 1
	return blocks


def cpu_percentile(jobs, percentile):
	"""
	Return the number of CPUs equal to the p-th `percentile`
	of the system utilization over the simulation period.

	Note:
	  We are assuming that the system has no CPU limit.
	"""
	last_event = jobs[-1].submit
	events = {}  # pairs <time, change in the number of CPUs>

	for j in jobs:
		events[j.submit] = events.get(j.submit, 0) + j.proc
		end = j.submit + j.run_time
		if end <= last_event:
			events[end] = events.get(end, 0) - j.proc

	events = sorted(events.iteritems())
	prev_event, cpus = events[0][0], 0
	util = {}  # pairs <number of CPUs, total period with that CPU count>

	for time, change in events:
		period = time - prev_event
		prev_event = time
		util[cpus] = util.get(cpus, 0) + period
		cpus += change

	total = sum(util.itervalues())  # total simulation period
	find = int(percentile / 100.0 * total)

	# sort by utilization and find the percentile
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
		package = 'parts'
		os.chdir(package)
		for f in glob.glob('*.py'):
			m = importlib.import_module(package + '.' + f[:-3])
			modules.append(m)
		os.chdir('..')

	if isinstance(name, list):
		return [make_classes(n, conf) for n in name]

	for m in modules:
		if hasattr(m, name):
			cl = getattr(m, name)
			return cl(conf)  # create instance
	else:
		raise Exception('class not found: ' + name)


def run(workload, args):
	"""
	Run the simulation described in `args` on the `workload`.

	Run one simulation for each supplied ``scheduler``.
	"""

	# encapsulate different settings
	sim_conf = settings.Settings(settings.sim_templates, **args)
	alg_conf = settings.Settings(settings.alg_templates, **args)
	part_conf = settings.Settings(settings.part_templates, **args)

	# now we need to load and instantiate the classes from `part_conf`
	for key, value in part_conf.__dict__.items():
		setattr(part_conf, key, make_classes(value, alg_conf))

	# parse the workload
	parser = parsers.get_parser(workload)

	jobs, users = parser.parse_workload(workload, sim_conf.serial)
	jobs.sort(key=lambda j: j.submit)  # order by submit time

	# set the missing values
	for j in jobs:
		j.time_limit = part_conf.submitter.time_limit(j)
		part_conf.submitter.modify_configuration(j)
		j.validate_configuration()

	for u in users.itervalues():
		u.shares = part_conf.share.user_share(u)

	# divide into blocks
	blocks = divide_jobs(jobs, sim_conf.job_id, sim_conf.block_time,
			     sim_conf.block_margin)

	for sched in part_conf.schedulers:
		print 'Simulating:', sched.__class__.__name__

		# set the current scheduler
		part_conf.scheduler = sched

		for b in blocks:
			print 'Block:', b

			# block includes both ends
			job_slice = jobs[b['left']:b['right']+1]

			# calculate the CPU number
			if sim_conf.cpu_count:
				cpus = sim_conf.cpu_count
			else:
				cpus = cpu_percentile(job_slice, sim_conf.cpu_percent)
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

			# reset the entities
			users_slice = {}
			for j in job_slice:
				j.reset()
				users_slice[j.user.ID] = j.user
			for u in users_slice.itervalues():
				u.reset()

			# run the simulation
			my_simulator = simulator.Simulator(job_slice, users_slice,
							   nodes, alg_conf, part_conf)
			results_slice = my_simulator.run()

			# TODO TUTAJ SAVE RESULTS

			if sim_conf.one_block:
				break

		print '-' * 30

#first_sub = jobs[b['first']].submit
#last_sub = jobs[b['last']].submit
#TODO get results -> drop margins (AKA EXTRA FLAGA Z PRZODU) -> save to file
#TODO DODAC TIME.CTIME DO FILENAME! default=time.ctime(),
#TODO ARGS SIE NIGDZIE NIE ZMIENIAJA? WYPISYWAC ARGS JAKO CONTEXT W KAZDYM PLIKU W 1 LINII

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
