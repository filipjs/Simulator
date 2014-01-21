# -*- coding: utf-8 -*-
import bisect
import logging
from abc import ABCMeta, abstractmethod
from util import delta


"""
#TODO OPIS
"""


class _NodeSpace(object):
	"""

	"""

	def __init__(self, begin, end, nodes, next):
		self.begin = begin
		self.end = end
		self.nodes = nodes  # node map
		self.next = next
		self.update()

	def update(self):
		self.length = self.end - self.begin

	def __repr__(self):
		s = '[{}, {}] \n\tnodes {}'
		return s.format(delta(self.begin), delta(self.end), self.nodes)


class BaseManager(object):
	"""

	"""

	__metaclass__ = ABCMeta

	def __init__(self, nodes, settings):
		self._settings = settings
		self._space_list = _NodeSpace(
					0,
					float('inf'),
					self._node_map(nodes),
					None,
				   )
		self._space_list.job_ends = 0 #TODO DO KONTRUKTORA??
		self._rsrv_count = 0
		# reservations ordered by begin time
		self._rsrv_begin = []
		# reservations ordered by end time
		self._rsrv_end = []
		# configuration
		self._node_count = len(nodes)
		self._max_cpu_per_node = nodes[0]
		self._cpu_limit = sum(nodes.itervalues())
		self._debug = logging.getLogger().isEnabledFor(logging.DEBUG)

	def _dump_space(self, intro, *args):
		"""
		Print the current state of node spaces.
		"""
		logging.debug(intro, *args)
		it = self._space_list
		while it is not None:
			logging.debug('%s', it)
			it = it.next
		logging.debug('Reservations')
		for i in xrange(self._rsrv_count):
			logging.debug('%s', self._rsrv_begin[i])

	def sanity_test(self, job):
		"""
		Return if the job is ever runnable in the current configuration.
		"""
		ret = True
		ret &= (job.nodes <= self._node_count)
		ret &= (job.pn_cpus <= self._max_cpu_per_node)
		ret &= (job.proc <= self._cpu_limit)
		return ret

	@abstractmethod
	def _node_map(self, nodes=None):
		"""
		Return the constructor for the `_BaseNodeMap` subclass
		this manager uses.
		"""
		raise NotImplemented

	@abstractmethod
	def _check_nodes(self, avail, job):
		"""
		Return if the job is runnable on `avail` nodes.
		"""
		raise NotImplemented

	@abstractmethod
	def _assign_resources(self, avail, job, reservation):
		"""
		Return a new node map with the best node selection
		for the job. Must be a subset of the `avail` map.

		Args:
		  avail: map of available nodes to use.
		  job: the job in question.
		  reservation: is this a reservation.

		"""
		raise NotImplemented

	def start_session(self, now):
		"""
		Prepare the manager for the upcoming scheduling or backfilling pass.
		"""
		self._space_list.begin = now
		self._space_list.update()
		assert self._space_list.length > 0, 'some finished jobs not removed'
		# drop previous reservations
		self._rsrv_count = 0
		self._rsrv_begin = []
		self._rsrv_end = []
		self._window = now + self._settings.bf_window

	def try_schedule(self, job):
		"""
		Try to schedule the job to be executed immediately.
		"""
		assert not self._rsrv_count, 'reservations are present'
		# Without reservations we only have to check the first
		# space to see if the job can be executed.
		first = self._space_list
		if not self._check_nodes(first.nodes, job):
			return False

		last = first

		while True:
			if (last.end - first.begin) >= job.time_limit:
				break
			last = last.next

		# The job spans the spaces from `first` to `last` (inclusive).
		# However might we have to split the last one.
		if (last.end - first.begin) > job.time_limit:
			# Divide the `last` space appropriately and
			# create a new space to occupy the hole.
			new_space = _NodeSpace(
					first.begin + job.time_limit,
					last.end,
					self.copy(last.nodes),
					last.next,
				    )
			# new space is following `last`
			new_space.job_ends = last.job_ends
			last.job_ends = 0
			last.end = new_space.begin
			last.next = new_space
			last.update()
		last.job_ends += 1
		# assign and remove used resources
		job.alloc = self._assign_resources(first.nodes, job, False)
		assert self._check_nodes(job.alloc, job), 'invalid resource map'

		it = first
		while True:
			it.nodes = self.remove(it.nodes, job.alloc)
			if it == last:
				break
			it = it.next

		if self._debug:
			self._dump_space('Added resources %s', job)
		return True

	def try_backfill(self, job):
		"""
		Try to schedule the job. Make a reservation otherwise.
		Return if the job can be executed immediately.
		"""

		it = self._space_list
		res_it = 0

		job_start = it.begin

		rem = self.remove
		cop = self.copy
		check_j = self._check_nodes

		while True:
			avail = cop(it.nodes)

			job_end = job_start + job.time_limit

			# Maybe we can stop, if the potential start is already
			# outside of the backfilling window.
			if job_start >= self._window:
				break

			for i in xrange(self._rsrv_count):
				res = self._rsrv_begin[i]  # ordered by begin time
				if res.begin >= job_end:
					break
				if res.end <= job_start:
					continue
				# the job and the reservation spaces intersect
				avail = rem(avail, res.nodes)

			if check_j(avail, job):
				if it == self._space_list:
					old = self._rsrv_count
					self._rsrv_count = 0
					r = self.try_schedule(job)
					assert r
					self._rsrv_count = old
					return r
				alloc = self._assign_resources(avail, job, True)
				new_res = _NodeSpace(
						job_start,
						job_end,
						alloc,
						None
					  )
				self._rsrv_count += 1
				# add to both list, preserving the order
				#TODO bisect.
				self._rsrv_begin.append(new_res)
				self._rsrv_begin.sort(key=lambda x: x.begin)
				self._rsrv_end.append(new_res)
				self._rsrv_end.sort(key=lambda x: x.end)
				break
			else:
				# We need more resources. Check what happens next:
				# 1) a job ends, or
				# 2) a reservation ends
				next_job_end = it.end
				if res_it < self._rsrv_count:
					next_res_end = self._rsrv_end[res_it].end
				else:
					next_res_end = float('inf')
				if next_job_end <= next_res_end:
					it = it.next
				if next_res_end <= next_job_end:
					res_it += 1
				job_start = min(next_job_end, next_res_end)
		return False

	def job_ended(self, job):
		"""
		Free the resources taken by the job.
		"""
		self._space_list.begin = job.end_time
		self._space_list.update()
		assert self._space_list.length >= 0, 'some finished jobs not removed'
		assert job.alloc is not None, 'missing job resources'

		last_space_end = job.start_time + job.time_limit
		it = self._space_list

		while it.end < last_space_end:
			it.nodes = self.add(it.nodes, job.alloc)
			it = it.next

		assert it.end == last_space_end, 'missing job last space'
		assert it.job_ends > 0, 'invalid last space'

		if it.job_ends == 1:
			# we can safely merge this space with the next one
			remove = it.next
			it.end = remove.end
			it.nodes = remove.nodes
			it.job_ends = remove.job_ends
			it.update()
			# move 'pointers' as the last step
			it.next = remove.next
			remove.next = None
		else:
			it.nodes = self.add(it.nodes, job.alloc)
			it.job_ends -= 1
		# finally clear
		job.alloc = None
		if self._debug:
			self._dump_space('Removed resources %s', job)

	@abstractmethod
	def intersect(self, x, y):
		"""
		"""
		raise NotImplemented

	@abstractmethod
	def add(self, x, y):
		"""
		"""
		raise NotImplemented

	@abstractmethod
	def remove(self, x, y):
		"""
		"""
		raise NotImplemented

	@abstractmethod
	def clear(self):
		"""
		"""
		raise NotImplemented

	@abstractmethod
	def size(self, x):
		"""
		"""
		raise NotImplemented

	@abstractmethod
	def copy(self, x):
		"""
		"""
		raise NotImplemented


class SingletonManager(BaseManager):
	"""
	"""

	def _node_map(self, nodes=None):
		if nodes:
			return nodes[0]
		else:
			return 0

	def _check_nodes(self, avail, job):
		return job.proc <= avail

	def _assign_resources(self, avail, job, reservation):
		assert job.proc <= avail, 'insufficient resources'
		return self._node_map({0:job.proc})

	def intersect(self, x, y):
		return min(x, y)

	def add(self, x, y):
		return x + y

	def remove(self, x, y):
		return x - y

	def clear(self):
		return 0

	def size(self, x):
		return x

	def copy(self, x):
		return x
