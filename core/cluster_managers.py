# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


"""
"""


class _NodeSpace(object):
	"""

	"""

	def __init__(self, begin, end, nodes, next):
		self.begin = begin
		self.end = end
		self.nodes = nodes
		self.next = next

	@property
	def size(self):
		return self.end - self.begin


class BaseClusterManager(object):
	"""

	"""

	__metaclass__ = ABCMeta

	@abstractmethod
	def runnable_test(self, job):
		"""

		"""
		raise NotImplemented

	@abstractmethod
	def try_schedule(self, job, now, bf_window):
		"""

		"""
		raise NotImplemented

	@abstractmethod
	def clear_reservations(self):
		"""

		"""
		raise NotImplemented


class SingletonNodes(BaseClusterManager):
	"""

	"""

	def __init__(self, count):
		self._cpu_limit = count
		self._node_space = [_NodeSpace(0, float('inf'), count, None)]
		self._first_space = 0
		self._space_count = 1
		self._reservations = []

	def runnable_test(self, job):
		"""

		"""
		return job.proc <= self._cpu_limit

	def try_schedule(self, job, now, bf_window):

		total_time = 0
		first = it = self._first_space
#TODO W END JOB ROBIC ZWIJANIE SPACES
#TODO AKA JEST SPACE END == JOB.END LUB JESLI NEXT SPACE.NODES == THIS.SPACE.NODES

		self._node_space[it].begin = now  # advance the first window
		assert self._node_space[it].size > 0, 'some finished job not removed'

		while True:
			space = self._node_space[it]

			if space.nodes >= job.proc:
				total_time += space.size
				if total_time >= job.time_limit:
					break
			else:
				total_time = 0
				first = space.next
			# next
			it = space.next

		if self._node_space[first].begin > now + bf_window:
			# reservation outside the backfilling window
			return False

		last = it
		it = first

		# At this point, we know that the job spans the spaces from
		# `first` to `last`. However the `last` one might need to be split.
		# Start by updating the available nodes in all but the last space.
		while it != last:
			self._node_space[it].nodes -= job.proc
			it = self._node_space[it].next

		last_space = self._node_space[last]
		extra_time = total_time - job.time_limit

		if extra_time > 0:
			# divide the `last` space appropriately
			# and create a new space that will follow it
			new_space = _NodeSpace(
				last_space.end - extra_time,
				last_space.end,
				last_space.proc,
				last_space.next
			)
			self._node_space.append(new_space)
			self._space_count += 1

			last_space.end = new_space.begin
			last_space.nodes -= job.proc
			last_space.next = self._space_count
		else:
			# perfect fit, no need to split
			last_space -= job.proc

		if first == self._first_space:
			# we can execute the job right away
			job.start_execution(now)
			return True
		else:
			# add to reservations
			self._reservations.append(job)
			return False
