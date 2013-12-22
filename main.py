#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import sys
import time
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

	print args
	return 0
	#TODO select reader based on extenstion
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


def convert_arg_line_to_args(arg_line):
	for arg in arg_line.split():
		if not arg.strip():
			continue
		yield arg


if __name__=="__main__":
#TODO PROLOG, EPILOG
	parser = argparse.ArgumentParser(description="Simulate a cluster from a workload file",
					     fromfile_prefix_chars='@')
	parser.convert_arg_line_to_args = convert_arg_line_to_args

#TODO parents = [settings? parts?]
	#formatter_class=argparse.ArgumentDefaultsHelpFormatter)

	sim_group = parser.add_argument_group('Simulation', 'General simulation parameters')
	sim_group.add_argument('--title', default='',
			       help="Title of the simulation")
	sim_group.add_argument('--job_id', type=int, help="Start from the job with this ID")
	sim_group.add_argument('--block_time', metavar="HOURS", type=int,
			help="Divide simulation into 'block_time' long parts")
	sim_group.add_argument('--block_margin', metavar="HOURS", type=int, default=0,
			help="Extra simulation time to fill up the cluster")
	sim_group.add_argument('--one_block', action='store_true',
			help="Simulate only the first block")
	sim_group.add_argument('--serial', action='store_true',
			help="Change parallel jobs to serial")
	sim_group.add_argument('--cpu_count', type=int,
			help="Set a static number of CPUs")
	sim_group.add_argument('--cpu_percentile', metavar='P-th', type=int,
			help="Set the number of CPUs to the P-th percentile")
	sim_group.add_argument('parts', help="PARTS", nargs="*")
	sim_group.add_argument('workload', help="Workload file")
#TODO DODAC TIME.CTIME DO TITLE! default=time.ctime(),
	## automatically build the rest of the arguments
	#alg_group = parser.add_argument_group('Algorithm', 'Algorithm specific parameters')
	#for temp in Settings.templates:
		#alg_group.add_argument('--' + temp[0], type=type(temp[2]),
			#default=temp[2], metavar=temp[3], help=temp[1])
##TODO ARGUMENTY DO WYBIERANIA NAZWY KLAS POSZCZEGOLNYCH CZESCI SYMULACJI



	args = vars(parser.parse_args())
	# manual check of the --cpu_xx arguments [required and exclusive]
	#if not args['cpu_count'] and not args['cpu_percentile']:
		#parser.error("one of the arguments --cpu_count --cpu_percentile is required")
	#if args['cpu_count'] and args['cpu_percentile']:
		#parser.error("argument --cpu_count not allowed with argument --cpu_percentile")
	# run the simulation
	main(args)
