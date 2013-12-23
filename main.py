#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import sys
import time
import parts.settings
#from reader import SWFReader, ICMReader #TODO ZMIENIC IMPORT READOW NA TYLKO MODULE??



def divide_jobs(jobs, first_job, block_time, block_margin):
	"""
	Divide the jobs into potentially many smaller blocks.
	Each block can have extra jobs to fill up the cluster.
	IN:
	- jobs - list of all the jobs
	- first_job - ID of the first job to start with
	- block_time - length of each block in seconds or None
	- block_margin - extra length added to the block on both sides
	OUT:
	- a list of consecutive blocks - a block is a dict of indexes
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


def main(args):
	"""
	"""
	print args
	return 0
	#TODO select reader based on extenstion
#TODO DODAC TIME.CTIME DO TITLE! default=time.ctime(),
	reader = SWFReader()
	jobs, users = reader.parse_workload(args['workload'], args['serial'])
	jobs.sort(key=lambda j: j.submit) # order by submit time

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


def arguments_from_templates(parser, templates):
	"""
	"""
	for temp in templates:
		assert temp.default is not None

		opts = {'metavar': temp.time_unit,
			'default': temp.default,
			'help': temp.desc}

		if type(temp.default) == type([]):
			# multiple values
			opts['nargs'] = '*'
			opts['type'] = type(temp.default[0])
		elif type(temp.default) == type(True):
			# boolean value
			del opts['metavar']
			opts['action'] = 'store_true'
		else:
			# normal argument
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
		for arg in arg_line.split():
			if not arg.strip():
				continue
			yield arg
		#TODO SKIP COMMENTS


desc = """Run a cluster simulation:
    `main.py run [SIM_OPTS] [ALG_OPTS] [PART_OPTS] <workload_file>`
To read the options from a config file:
    `main.py run @myconfig <workload_file>`

You can generate a template of the configuration:
    `main.py config --generate <out_file>`.
You can also recreate a config from a simulation:
    `main.py config --simulation <sim_file> <out_file>`
"""


if __name__=="__main__":

	#TODO PROLOG, EPILOG, USAGE
	parser = MyArgumentParser(description=desc,
				  formatter_class=argparse.RawDescriptionHelpFormatter)

				  #fromfile_prefix_chars='@',
				  #formatter_class = MyHelpFormatter,

	parser.add_argument('workload', help='The workload file')

	sim_group = parser.add_argument_group('General simulation parameters')
	arguments_from_templates(sim_group, parts.settings.sim_templates)

	alg_group = parser.add_argument_group('Algorithm specific parameters')
	arguments_from_templates(alg_group, parts.settings.alg_templates)

	part_group = parser.add_argument_group('Part selection parameters')
	arguments_from_templates(part_group, parts.settings.part_templates)

	main(vars(parser.parse_args()))
