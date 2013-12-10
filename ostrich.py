#!/usr/bin/env python
# -*- coding: utf-8 -*-

from simulation import BaseSimulator # TODO zmiana lib pozniej
from entities import * #TODO remove *


class OStrichSimulator(BaseSimulator):
	def new_job_event(job, time, last_time):
		raise NotImplemented
	def job_end_event(job, time, last_time):
		raise NotImplemented
	def new_camp_event(camp, time, last_time):
		raise NotImplemented
	def camp_end_event(camp, time, last_time):
		raise NotImplemented


def run_simulation(jobs, block, cpus):

	if cpus is None:
		cpus = 300 #TODO
	used_cpus = 0

	users = {}

	future_jobs = jobs[block['left']:block['right']]
	waiting_jobs = []
	running_jobs = []

	for j in future_jobs:
		j.reset()

	act_time = future_jobs[0].submit

	while future_jobs and running_jobs:
		if not running_jobs:
			event = Events.new_job
		elif not future_jobs:
			event = Events.job_end
		elif future_jobs[0].submit < running_jobs[0].end:
			event = Events.new_job
		else:
			event = Events.job_end

		if event == Events.new_job:
			j = future_jobs.pop(0)
			diff = j.submit - act_time
			act_time = j.submit
			# dist time
			if j.user not in users:
				users[j.user] = User(j.user)
			users[j.user].add_job(j)
			waiting_jobs.append(j)
		else:
			j = running_jobs.pop(0)
			diff = j.end - act_time
			act_time = j.end
			# dist time
			# zwolnic cpu
			j.has_ended()
		# reszta wspolna? aka przeliczyc kampanie i priorytety i wrzucic z waiting nowe jak miejsce


		# TODO lista -> <time, utility>

