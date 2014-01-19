# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta, abstractmethod
from util import delta


"""
#TODO OPIS
"""


class _NodeSpace(object):
	"""

	"""

	def __init__(self, begin, end, avail, reserved, next, job_ends):
		self.begin = begin
		self.end = end
		self.avail = avail  # node map
		self.reserved = reserved  # node map
		self.next = next
		self.job_ends = job_ends
		self.rsrv_starts = 0
		self.update()

	def update(self):
		self.length = self.end - self.begin

	def __repr__(self):
		s = '[{}, {}] last {} first {}\n\tavail {}\n\trsrvd {}'
		return s.format(delta(self.begin), delta(self.end),
			self.job_ends, self.rsrv_starts,
			self.avail, self.reserved)


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
					self._node_map(),
					None,
					0,
				   )
		self._reservations = 0
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
		  reservation: is this a reservation flag.

		"""
		raise NotImplemented

	def start_session(self, now):
		"""
		Prepare the manager for the upcoming scheduling or backfilling pass.
		"""
		self._window = now + self._settings.bf_window
		self._space_list.begin = now
		self._space_list.update()
		assert self._space_list.length > 0, 'some finished jobs not removed'
		assert not self._reservations, 'reservations not removed'

	def _allocate_resources(self, job, first, last, avail, reservation):
		"""
		Allocate resources to the job and update the space list.

		Args:
		  job: job in question.
		  first: starting space for the job.
		  last: last space for the job.
		  avail: node map with available nodes for the job.
		  reservation: is this a reservation flag.

		"""
		assert self._check_nodes(avail, job), 'invalid avail map'
		# The job spans the spaces from `first` to `last` (inclusive).
		# However might we have to split the last one.
		if (last.end - first.begin) > job.time_limit:
			# Divide the `last` space appropriately and
			# create a new space to occupy the hole.
			new_space = _NodeSpace(
					first.begin + job.time_limit,
					last.end,
					self.copy(last.avail),
					self.copy(last.reserved),
					last.next,
					last.job_ends,
				    )
			# new space is following `last`
			last.end = new_space.begin
			last.next = new_space
			last.job_ends = 0
			last.update()

		# get the resources from the `avail` node map
		res = self._assign_resources(avail, job, reservation)
		assert self._check_nodes(res, job), 'invalid resource map'

		if not reservation:
			job.alloc = res
			last.job_ends += 1
		else:
			self._reservations += 1
			first.rsrv_starts += 1

		# remove the used up resources
		it = first
		while True:
			it.avail = self.remove(it.avail, res)
			if reservation:
				it.reserved = self.add(it.reserved, res)
			if it == last:
				break
			it = it.next

		if self._debug:
			self._dump_space('Added resources %s', job)

	def try_schedule(self, job):
		"""
		"""
		assert not self._reservations, 'reservations are present'
		# In a space list without reservations, spaces are
		# guaranteed to have more resources available than
		# the spaces before them.
		# This means we only have to check the first one
		# to see if the job can be executed.
		first = self._space_list
		if not self._check_nodes(first.avail, job):
			return False

		total_time = 0
		it = first

		while True:
			total_time += it.length
			if total_time >= job.time_limit:
				last = it
				break
			it = it.next

		self._allocate_resources(job, first, last, first.avail, False)
		return True

	def try_backfill(self, job):
		"""
		Make a reservation for the job.
		Return if the job can be executed immediately.
		"""
		total_time = 0
		it = first = self._space_list
		avail = None
		must_check = True

		cop = self.copy
		inter = self.intersect
		check_j = self._check_nodes

		while True:
			if must_check:
				if avail is None:
					avail = cop(it.avail)
					#avail = self.copy(it.avail)
				else:
					avail = inter(avail, it.avail)
					#avail = self.intersect(avail, it.avail)

			if not must_check or check_j(avail, job):
			#if not must_check or self._check_nodes(avail, job):
				total_time += it.length
				if total_time >= job.time_limit:
					last = it
					break
				# next space #TODO OPIS
				it = it.next
				must_check = it.rsrv_starts > 0
			else:
				total_time = 0
				#TODO OPIS
				it = first = first.next
				avail = None
				must_check = True
				# Maybe we can stop, if the potential start is already
				# outside of the backfilling window.
				if first.begin > self._window:
					return False

		# check if the job can be executed now
		can_run = (first == self._space_list)

		self._allocate_resources(job, first, last, avail, not can_run)
		return can_run

	def end_session(self):
		"""
		Clear the created reservations.
		"""
		before = self._reservations
		prev, it = None, self._space_list

		while it.next is not None:
			# update the count
			self._reservations -= it.rsrv_starts
			it.rsrv_starts = 0
			# now clean up
			if not it.job_ends:
				# we can safely remove this space
				remove, it = it, it.next
				it.begin = remove.begin
				it.update()
				prev.next = it
				remove.next = None
			else:
				it.avail = self.add(it.avail, it.reserved)
				it.reserved = self.clear()
				prev, it = it, it.next

		assert not self._reservations, 'reservations not cleared'
		if self._debug:
			self._dump_space('Cleared %s reservations', before)

	def job_ended(self, job):
		"""
		Free the resources taken by the job.
		"""
		assert not self._reservations, 'reservations are present'
		self._space_list.begin = job.end_time
		self._space_list.update()
		assert self._space_list.length >= 0, 'some finished jobs not removed'
		assert job.alloc is not None, 'missing job resources'

		last_space_end = job.start_time + job.time_limit
		it = self._space_list

		while it.end < last_space_end:
			it.avail = self.add(it.avail, job.alloc)
			it = it.next

		assert it.end == last_space_end, 'missing job last space'
		assert it.job_ends > 0, 'invalid last space'

		if it.job_ends == 1:
			# we can safely merge this space with the next one
			remove = it.next
			it.end = remove.end
			it.avail = remove.avail
			it.reserved = remove.reserved
			it.job_ends = remove.job_ends
			it.update()
			# move 'pointers' as the last step
			it.next = remove.next
			remove.next = None
		else:
			it.avail = self.add(it.avail, job.alloc)
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
