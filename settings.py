# -*- coding: utf-8 -*-

camp_threshold = 10 * 60	# seconds
sim_margin = 12 * 60 * 60	# seconds


def job_initial_estimate(job):
	return job.runtime


def campaign_job_cmp(x, y):
	if x.estimate == y.estimate:
		return x.submit - y.submit
	return x.estimate - y.estimate

