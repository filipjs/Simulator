#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import sys
import time
from core import parsers, simulation
from parts import settings


def divide_jobs(jobs, first_job, block_time, block_margin):
	"""
	Divide the jobs into potentially many smaller blocks.
	Each block can have extra jobs to fill up and empty the cluster.

	Args:
	  jobs: list of all the jobs.
	  first_job: ID of the first job to start with.
	  block_time: length of each block in seconds or ``zero``.
	  block_margin: extra length added to the block on both sides.

	Returns:
	  a list of consecutive blocks - a block is a dict of indexes
	    {left [margin start]:first [block job]:last [block job]:right [margin end]}
	"""
	for i, j in enumerate(jobs):
		if j.ID == first_job:
			break
	else:
		print "ERROR: job ID", first_job, "not found"
		sys.exit(1)

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


def run(args):
	"""
	"""
	#TODO ZMIENILY SIE PARAMS: TERAZ 0 == NONE
	print "RUNWWW", args
	return 0



	parser = parsers.get_parser(args['workload'])

	jobs, users = parser.parse_workload(args['workload'], args['serial'])
	jobs.sort(key=lambda j: j.submit)  # order by submit time

	for j in jobs:
		# TODO ADD TIME LIMIT
		pass

	for u in users.itervalues():
		#TODO ADD SHARES
		pass

	if not args['job_id']:
		args['job_id'] = jobs[0].ID

	# change hours to seconds
	block_time = args['block_time'] and args['block_time'] * 3600
	block_margin = args['block_margin'] * 3600

	blocks = divide_jobs(jobs, args['job_id'], block_time, block_margin)
#TODO all users -> do symulacji tylko tych ktorzy tam wystepuja + reset
#TODO DODAC TIME.CTIME DO TITLE! default=time.ctime(),
	for b in blocks:
		full_slice = jobs[b['left']:b['right']+1] # block includes both ends

		cpus = args['cpus'] # TODO

#TODO for each algo job.reset -> get results -> drop margins -> save to file?
		for j in full_slice:
			j.reset()

		#run_ostrich(job_slice, first_sub, last_sub, cpus)
		#run_fairshare(job_slice, first_sub, last_sub, cpus)

		first_sub = jobs[b['first']].submit
		last_sub = jobs[b['last']].submit

		if args['one_block']:
			break


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
