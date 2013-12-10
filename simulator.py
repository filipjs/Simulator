#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse

import from settings import get_settings

Settings = get_settings()


def divide_jobs(jobs, first_job, block_time, block_margin):

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

	jobs = parse_workload(args['swf_file'], args['serial'])
	# jobs from swf files are sorted by submit time

	for j in jobs:
		j.estimate = Settings.job_initial_estimate(j)

	if not args['job_id']:
		args['job_id'] = jobs[0].ID
#TODO GDZIES TUTAJ PRINT ARGS JAKO 'CONTEXT' + TITLE ARG + KLASA SETTIGS??
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
		#run_fcfs(job_slice, first_sub, last_sub, cpus)

		first_sub = jobs[b['first']].submit
		last_sub = jobs[b['last']].submit

		if args['one_block']:
			break


if __name__=="__main__":

	parser = argparse.ArgumentParser(description="Start simulation from .swf file")
	parser.add_argument('--job_id', type=int, help="Start from job with this ID")
	parser.add_argument('--block_time', metavar="HOURS", type=int,
			help="Divide simulation into 'block_time' long parts")
	parser.add_argument('--block_margin', metavar="HOURS", type=int, default=0,
			help="Extra simulation time to 'fill up' the cluster")
	parser.add_argument('--one_block', action='store_true',
			help="Simulate only one part")
	parser.add_argument('--serial', action='store_true',
			help="Change parallel jobs to serial")
	parser.add_argument('--cpus', type=int,
			help="Set the number of CPUs in the cluster")
	parser.add_argument('swf_file', help="Workload file")

	main(vars(parser.parse_args()))
