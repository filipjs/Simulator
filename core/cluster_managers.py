# -*- coding: utf-8 -*-
#import bisect #TODO POTRZEBNE?
import logging
from abc import ABCMeta, abstractmethod
from util import delta


"""
#TODO OPIS
"""


class _ListItem(object):
	"""
	"""

	def __init__(self, key, value, next):
		self.key = key
		self.value = value
		self.next = next


class SortedList(object):
	"""
	"""

	def __init__(self):
		self._first = None

	def head(self):
		return self._first

	def add(self, key, item):
		prev, it = None, self.head()
		while it is not None and it.key < key:
			prev, it = it, it.next
		new_item = _ListItem(key, item, it)
		if prev is None:
			self._first = new_item
		else:
			prev.next = new_item

	def pop(self):
		self._first = self._first.next

	def remove(self, item):
		prev, it = None, self.head()
		while it is not None and it.value != item:
			prev, it = it, it.next
		assert it is not None, 'item missing'
		if prev is None:
			self._first = self._first.next
		else:
			prev.next = it.next
		it.value = it.next = None

	def clear(self):
		self._first = None


class _Reservation(object):
	"""

	"""

	def __init__(self, begin, end, nodes):
		self.begin = begin
		self.end = end
		self.nodes = nodes  # node map

	def __repr__(self):
		s = '[{}, {}] \n\tnodes {}'
		return s.format(delta(self.begin), delta(self.end), self.nodes)


class BaseManager(object):
	"""

	"""

	__metaclass__ = ABCMeta

	def __init__(self, nodes, settings):
		self._settings = settings
		self._jobs = SortedList()
		self._reservations = SortedList()
		self._avail = self._node_map(nodes)
		# configuration
		self._node_count = len(nodes)
		self._max_cpu_per_node = nodes[0]
		self._cpu_limit = sum(nodes.itervalues())
		self._debug = logging.getLogger().isEnabledFor(logging.DEBUG)

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

	def _dump_space(self, intro, *args):
		"""
		Print the current state of node spaces.
		"""
		logging.debug(intro, *args)
		it = self._jobs.head()
		prev_st = self._now
		avail = self.copy(self._avail)
		while it is not None:
			logging.debug('[%s, %s] \n\tavail %s',
			delta(prev_st), delta(it.key), avail)
			prev_st = it.key
			avail = self.add(avail, it.value.alloc)
			it = it.next
		logging.debug('Reservations')
		it = self._reservations.head()
		while it is not None:
			logging.debug('%s', it.value)
			it = it.next

	def start_session(self, now):
		"""
		Prepare the manager for the upcoming scheduling or backfilling pass.
		"""
		# drop previous reservations
		self._reservations.clear()
		self._now = now
		self._window = now + self._settings.bf_window

	def _allocate_resources(self, avail, job):
		"""
		"""
		assert self._check_nodes(avail, job), 'invalid avail map'
		job.alloc = self._assign_resources(avail, job, False)
		assert self._check_nodes(job.alloc, job), 'invalid resource map'

		self._avail = self.remove(self._avail, job.alloc)
		self._jobs.add(self._now + job.time_limit, job)

		if self._debug:
			self._dump_space('Added resources %s', job)

	def try_schedule(self, job):
		"""
		Try to schedule the job to be executed immediately.
		"""
		assert self._reservations.head() is None, 'reservations are present'
		# Without reservations we only have to check
		# the currently available nodes.
		if not self._check_nodes(self._avail, job):
			return False
		self._allocate_resources(self._avail, job)
		return True

	def try_backfill(self, job):
		"""
		Try to schedule the job. Make a reservation otherwise.
		Return if the job can be executed immediately.
		"""

		avail = self.copy(self._avail)

		job_it = self._jobs.head()
		res_it = self._reservations.head()
		rsrv_ends = SortedList()

		job_start = self._now

		rem = self.remove
		cop = self.copy
		check_j = self._check_nodes

		while True:

			job_end = job_start + job.time_limit

			# Maybe we can stop, if the potential start is already
			# outside of the backfilling window.
			if job_start >= self._window:
				break

			while (res_it is not None and
			       res_it.key < job_end):
				avail = self.remove(avail, res_it.value.nodes, r=True)
				rsrv_ends.add(res_it.value.end, res_it.value)
				res_it = res_it.next

			if check_j(avail, job):
				if job_start == self._now:
					self._allocate_resources(avail, job)
					return True
				alloc = self._assign_resources(avail, job, True)
				new_r = _Reservation(job_start, job_end, alloc)
				self._reservations.add(job_start, new_r)
				if self._debug:
					self._dump_space('Added reservation %s', job)
				break
			else:
				# We need more resources. Check what happens first:
				# 1) a job ends, or
				# 2) a reservation ends
				assert job_it is not None or rsrv_ends.head() is not None, \
				'nothing happens'
				if job_it is not None:
					next_job_end = job_it.key
				else:
					next_job_end = float('inf')
				if rsrv_ends.head() is not None:
					next_res_end = rsrv_ends.head().key
				else:
					next_res_end = float('inf')

				if next_job_end <= next_res_end:
					while job_it is not None and job_it.key == next_job_end:
						avail = self.add(avail, job_it.value.alloc)
						job_it = job_it.next
				if next_res_end <= next_job_end:
					while rsrv_ends.head() is not None and rsrv_ends.head().key == next_res_end:
						avail = self.add(avail, rsrv_ends.head().value.nodes, r=True)
						rsrv_ends.pop()
				job_start = min(next_job_end, next_res_end)
		return False

	def job_ended(self, job):
		"""
		Free the resources taken by the job.
		"""
		self._now = job.end_time # TODO DLEETE??
		assert job.alloc is not None, 'missing job resources'
		self._jobs.remove(job)
		self._avail = self.add(self._avail, job.alloc)
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
			return [range(nodes[0]), []]
		else:
			return [[],[]]

	def _check_nodes(self, avail, job):
		return job.proc <= len(avail[0])

	def _assign_resources(self, avail, job, reservation):
		return [avail[0][-job.proc:], []]
		#return self._node_map({0:job.proc})

	def intersect(self, x, y):
		raise NotImplemented
		#return min(x, y)

	def add(self, x, y, r=False):
		assert not (set(x[0]) & set(y[0]))
		assert not y[1]
		x2 = [x[0][:], x[1][:]]
		if r:
			for i in y[0]:
				x2[1].remove(i)
		for i in y[0]:
			if i not in x2[1]:
				x2[0].append(i)
		return x2
		#return x + y

	def remove(self, x, y, r=False):
		assert not y[1]
		x2 = [x[0][:], x[1][:]]
		for i in y[0]:
			try:
				x2[0].remove(i)
			except:
				pass
		if r:
			x2[1].extend(y[0])
		return x2
		#return x - y

	def clear(self):
		raise NotImplemented
		#return 0

	def size(self, x):
		raise NotImplemented
		#return x

	def copy(self, x):
		assert not x[1]
		return [x[0][:], []]
		#return x
