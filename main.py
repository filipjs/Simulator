#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import importlib
import sys
import time
from core import parsers#, simulator
from parts import settings


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
			print "ERROR: job ID", first_job, "not found"
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


def load_class(module_name, class_name):
	"""
	Load a class the the module.
	"""
	m = importlib.import_module('parts.' + module_name)
	c = getattr(m, class_name)
	return c


def run(args):
	"""
	"""
	#TODO NA SAMYM POCZATKU PRINT ARGS, ZA MOMENT JE ZMIENIAMY??

	# encapsulate different settings
	sim_set = settings.Settings(settings.sim_templates, **args)
	alg_set = settings.Settings(settings.alg_templates, **args)

	# now transform parts settings
	# load common parts
	common_parts = ['estimator', 'submitter', 'selector', 'share']
	for part in common_parts:
		cl = load_class(part+'s', args[part])
		args[part] = cl(alg_set)

	# load schedulers : we will do one simulation per scheduler
	schedulers = []
	for cl_name in args['schedulers']:
		cl = load_class('schedulers', cl_name)
		schedulers.append(cl(alg_set))

	# finally create parts settings
	part_set = settings.Settings(settings.part_templates, **args)

	# parse the workload
	parser = parsers.get_parser(args['workload'])

	jobs, users = parser.parse_workload(sim_set.workload, sim_set.serial)
	jobs.sort(key=lambda j: j.submit)  # order by submit time

	# calculate missing values
	for j in jobs:
		j.time_time = part_set.submitter.time_limit(j)

	for u in users.itervalues():
		u.shares = part_set.share.user_share(u)

	# divide into blocks
	blocks = divide_jobs(jobs, sim_set.job_id, sim_set.block_time, sim_set.block_margin)

	for sched in schedulers:
		# set the current scheduler
		part_set.scheduler = sched

		for b in blocks:
			# block includes both ends
			job_slice = jobs[b['left']:b['right']+1]
			# calculate the CPU number
			cpus = args['cpus'] # TODO
			# reset entities
			users_slice = {}
			for j in job_slice:
				j.reset()
				slice_users[j.user.ID] = j.user
			for u in slice_users.itervalue():
				u.reset()
			# run the simulation
			simulator = simulator.Simulator(job_slice, users_slice,
							cpus, alg_set, part_set)
			results_slice = simulator.run()

			# TODO TUTAJ SAVE RESULTS
			if sim_args.one_block:
				break

#first_sub = jobs[b['first']].submit
#last_sub = jobs[b['last']].submit
#TODO get results -> drop margins (AKA EXTRA FLAGA Z PRZODU) -> save to file
#TODO DODAC TIME.CTIME DO FILENAME! default=time.ctime(),


def config(args):
	"""
	"""
	values = {}
	if args['generate']:
		values = {}
	else:
	        from ast import literal_eval
		with open(args['recreate']) as f:
			values = literal_eval(f.readline())

	def print_template(temp):
		value = values.get(temp.name, temp.default)
		unit = temp.time_unit or ''

		print '--{:15}{}'.format(temp.name, str(value))
		print '# {}: (default) {} {}'.format(temp.desc, temp.default, unit)
		if temp.loc is not None:
			print '# Used by `{}`'.format(temp.loc)

	print '##\n## General simulation parameters\n##\n'
	map(print_template, settings.sim_templates)
	print '\n##\n## Algorithm specific parameters\n##\n'
	map(print_template, settings.alg_templates)
	print '\n##\n## Part selection parameters\n##\n'
	map(print_template, settings.part_templates)


def arguments_from_templates(parser, templates):
	"""
	"""

	def str2bool(v):
		return v.lower() == 'true'

	for temp in templates:
		assert temp.default is not None

		opts = {'metavar': temp.time_unit,
			'default': temp.default,
			'help': temp.desc}

		if isinstance(temp.default, list):
			# multiple values
			opts['type'] = type(temp.default[0])
			opts['nargs'] = '*'
		elif isinstance(temp.default, bool):
			opts['type'] = str2bool
		else:
			opts['type'] = type(temp.default)

		parser.add_argument('--' + temp.name, **opts)


class MyHelpFormatter(argparse.HelpFormatter):
	"""
	"""

	def __init__(self, prog, **kwargs):
		kwargs['max_help_position'] = 40
		kwargs['indent_increment'] = 4
		kwargs['width'] = 100
		argparse.HelpFormatter.__init__(self, prog, **kwargs)


class MyArgumentParser(argparse.ArgumentParser):
	"""
	"""

	def convert_arg_line_to_args(self, arg_line):
		"""
		"""
		if arg_line.startswith('#'):
			return
		for arg in arg_line.split():
			if not arg.strip():
				continue
			yield arg


run_usage = '`%(prog)s run [SIM_OPTS][ALG_OPTS][PART_OPTS] workload_file`'

global_desc = """INSTRUCTIONS
----------------------------------------------------------------
To run a cluster simulation:
    {}
To read the options from a config file:
    `%(prog)s run @myconfig workload_file`

You can generate a template of the configuration:
    `%(prog)s config --generate`.
You can also recreate a config from a simulation:
    `%(prog)s config --recreate sim_file`
----------------------------------------------------------------
""".format(run_usage)


if __name__=="__main__":

	parser = MyArgumentParser(description=global_desc,
				  formatter_class=argparse.RawDescriptionHelpFormatter)
	subparsers = parser.add_subparsers(dest='command', help='Select a command')

	# run simulation parser
	run_parser = subparsers.add_parser('run', help='Run a simulation',
					   usage=run_usage,
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
		run(args)
	elif args['command'] == 'config':
		config(args)
	else:
		print "Hmm..."
