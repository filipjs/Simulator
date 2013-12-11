# -*- coding: utf-8 -*-

sim_margin = 12 * 60 * 60	# seconds


def job_initial_estimate(job):
	return job.runtime
