#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools
import heapq
from abc import ABCMeta
from entities import * #TODO remove *


class Events(object):
	new_job = 0
	job_end = 1
	new_campaign = 2
	campaign_end = 3


class PriorityQueue(object):
	"""
	A priority queue of <time, event, entity>, ordered by ascending time.
	Ties are ordered by event type.
	"""
	REMOVED = 'removed-event'

	def __init__(self):
		self.pq = []
		self.entries = {}
	def add(self, time, event, entity):
		"""
		Add an entity event to the queue.
		"""
		key = (event, entity) # must be an unique key
		if key in self.entries:
			self._remove_event(key)
		entry = [time, event, entity]
		self.entries[key] = entry
		heapq.heappush(self.pq, entry)
	def pop(self):
		"""
		Remove and return the next upcoming event.
		Return 'None' if queue is empty.
		"""
		if not self.empty():
			time, event, entity = heapq.heappop(self.pq)
			key = (event, entity)
			del self.entries[key]
			return time, event, entity
		raise KeyError('pop from an empty priority queue')
	def empty(self):
		"""
		Check if queue is empty.
		"""
		self._pop_removed()
		return bool(self.pq)
	def _remove_event(self, key):
		"""
		Mark an existing event as removed.
		"""
		entry = self.entries.pop(key)
		entry[-1] = self.REMOVED
	def _pop_removed(self):
		"""
		Process the queue to the first non-removed event.
		"""
		while self.pq and self.pq[0][-1] == self.REMOVED:
			heapq.heappop(self.pq)


class BaseSimulator(object):
	"""
	"""
	__metaclass__ = ABCMeta

	def __init__(self, jobs, users, cpus):
		self.cpu_limit = cpus
		self.cpu_used = 0
		self.future_jobs = jobs
		self.running_jobs = []
		self.waiting_jobs = []
		self.users = users
		self.total_shares = 0

	def run(self):
		"""
		"""
		self.results = []

		self.prev_events = {}
		self.pq = PriorityQueue()

		count = 0
		submits = len(self.future_jobs)

		while count < submits or not self.pq.empty():
			if count < submits:
				self.pq.add(
					self.future_jobs[count].submit,
					Events.new_job,
					self.future_jobs[count]
				)
				count += 1
			# the queue cannot be empty here
			time, event, entity = self.pq.pop()

			# if it is the first event then last_time = time
			last_time = self.prev_events.get(event, time)

			if event == Events.new_job:
				self._new_job_event(entity, time, last_time)
				#TODO run job if free cpu + backfill?
			elif event == Events.job_end:
				self.job_end_event(entity, time, last_time)
				#TODO run job if in queue + backfill
			elif event == Events.new_campaign:
				#TODO zmienic event na new active user?
				self.new_camp_event(entity, time, last_time)
			elif event == Events.campaign_end:
				#TODO zmienic event na remove active user?
				self.camp_end_event(entity, time, last_time)
			else:
				raise Exception('unknown event')

			# update events time
			self.prev_events[event] = time
		# return simulation results
		return self.results

	def _distribute_virtual(self, period):
		""" Distribute virtual time shares to active users.
		"""
		if self.total_shares:
			one_share = float(period)/self.total_shares
			one_share *= self.cpu_used
			for u in self.users:
				if u.active:
					u.virtual += one_share * u.shares
					#TODO zle -> od razu dodawac czas do pierwszej nie skonczonej kampani
					#TODO aka active_camps[0]?

	def new_job_event(job, time, last_time):
		"""
		"""
		self._distribute_virtual(time - last_time)

		user = self.users[job.userID]

		camp = self._add_new_job(user, job)
		camp.sort_jobs(self._camp_job_cmp)

		# !! niepotrzebe przeliczac priority wszystkich prac
		# !! wystarzcy ze praca wie jaki jest REMAINING WORK (nie ma potrzeby end time)
		# !! swojej kampani, creation time
		# !! i pozycja wewnatrz bo wtedy mozna sortowac
		# sort tuples <end time, creation time, camp position>
		# TODO zamiast camp position mozna po prostu uzyc 'runtime' bo wiemy ze tak sortujemy??
		# TODO ale jak wtedy podmienic funkcje sortujaca

		#1) < cpu limit & empty queue
		#2) < cpu limit & queue & backfill

		#(teraz juz po dodaniu pracy i ew. zwiekszeniu liczby cpu)
		#3) przeliczyc end time wszystkich(?) kampani
		#4) dodac pierwsze kampanie jako event <end camp> z nowym czasem


	@abstractmethod
	def job_end_event(job, time, last_time):
		raise NotImplemented
	@abstractmethod
	def new_camp_event(camp, time, last_time):
		raise NotImplemented
	@abstractmethod
	def camp_end_event(camp, time, last_time):
		raise NotImplemented

	@abstractmethod
	def _add_new_job(self, user, job):
		"""
		Add the job to an appropriate user campaign.
		Return: the campaign to which the job was added.
		"""
		raise NotImplemented
	@abstractmethod
	def _camp_job_cmp(self, x, y):
		""" Job comperator for the inner campaign sort.
		"""
		raise NotImplemented
