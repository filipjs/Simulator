# -*- coding: utf-8 -*-
import copy
from abc import ABCMeta, abstractmethod, abstractproperty
from functools import partial
from util import debug_print


# set up debug level for this module
DEBUG_FLAG = __debug__
debug_print = partial(debug_print, flag=DEBUG_FLAG, name=__name__)


"""


"""


class _NodeSpace(object):
	"""

	"""

	def __init__(self, begin, end, avail, reserved, next):
		self.begin = begin
		self.end = end
		self.avail = avail  # node map
		self.reserved = reserved  # node map
		self.next = next
		self.job_last_space = 0

	@property
	def length(self):
		return self.end - self.begin


class _BaseNodeMap(object):
	"""

	"""

	__metaclass__ = ABCMeta

	@abstractmethod
	def intersect(self, other):
		"""
		"""
		raise NotImplemented

	@abstractmethod
	def add(self, other):
		"""
		"""
		raise NotImplemented

	@abstractmethod
	def remove(self, other):
		"""
		"""
		raise NotImplemented

	@abstractmethod
	def clear(self):
		"""
		"""
		raise NotImplemented

	@abstractproperty
	def size(self):
		"""
		"""
		raise NotImplemented


class BaseManager(object):
	"""

	"""

	__metaclass__ = ABCMeta

	def __init__(self, nodes, settings):
		assert settings.bf_window > 0, 'invalid bf_window'
		assert settings.bf_resolution > 0, 'invalid bf_resolution'
		self._settings = settings
		self._space_list = _NodeSpace(0, float('inf'),
			self._node_map(nodes), self._node_map(), None)
		# configuration
		self._node_count = len(nodes)
		self._max_cpu_per_node = nodes[0]
		self._cpu_limit = sum(nodes.itervalues())

	def sanity_test(self, job):
		"""
		Return if the job is ever runnable in the current configuration.
		"""
		ret = True
		ret &= (job.nodes <= self._node_count)
		ret &= (job.pn_cpus <= self._max_cpu_per_node)
		ret &= (job.proc <= self._cpu_limit)
		return ret

	@abstractproperty
	def _node_map(self):
		"""
		Return the constructor for the subclassed `_BaseNodeMap`
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
		Return the best node selection from `avail` for the job.

		Args:
		  avail: map of available nodes to use.
		  job: the job in question.
		  reservation: is this for reservation only flag.

		"""
		raise NotImplemented

	def prepare(self, now):
		"""
		Prepare the node manager for the next scheduling pass.
		"""
		self._reservations = 0
		self._now = now
		self._space_list.begin = now  # advance the first window
		assert self._space_list.length > 0, 'some finished jobs not removed'

	def try_schedule(self, job):
		"""
		Make a reservation for the job.
		Return if the job can be executed immediately.
		"""
		total_time = 0
		it = first = self._space_list
		avail = None

#TODO WINDOW ZROBIC ZE ZAOKRAGLAC TYLKO END TIMY I TIME LIMIT ALE W GORE!!!

		while True:
			if avail is None:
				avail = copy.deepcopy(it.avail)
			else:
				avail.intersect(it.avail)

			if self._check_nodes(avail, job):
				total_time += it.length
				if total_time >= job.time_limit:
					last = it
					break
				# next space
				it = it.next
			else:
				total_time = 0
				# We can only advance one element in case of failure,
				# because we are using set intersection on the nodes.
				it = first = first.next
				avail = None
				# Maybe we can stop, if the potential start is already
				# outside of the backfilling window.
				if first.begin > self._now + self._settings.bf_window:
					return False

		# check if the job can be executed now
		can_run = (first == self._space_list)

		# At this point, we know that the job spans the spaces from `first`
		# to `last` (inclusive). However we have to split the last one.
		if total_time > job.time_limit:
			# Divide the `last` space appropriately and
			# create a new space to occupy the hole.
			new_space = copy.deepcopy(last)
			new_space.begin = first.begin + job.time_limit

			last.end = new_space.begin
			last.next = new_space
			last.job_last_space = 0
		if can_run:
			last.job_last_space += 1

		# get the resources from the `avail` node map
		res = self._assign_resources(avail, job, not can_run)
		if can_run:
			job.res = res
		else:
			self._reservations += 1
		# update the available nodes in all spaces
		it = first
		while True:
			it.avail.remove(res)
			if not can_run:
				it.reserved.add(res)
			if it == last:
				break
			it = it.next
		return can_run

	def clear_reservations(self):
		"""
		Scheduling pass is over. Clear the created reservations.
		"""
		if not self._reservations:
			# nothing to clear
			return

		prev, it = None, self._space_list

		while it.next is not None:
			if not it.job_last_space:
				# we can safely remove this space
				it.next.begin = it.begin
				prev.next = it.next
				it = it.next
			else:
				it.avail.add(it.reserved)
				it.reserved.clear()
				prev, it = it, it.next

	def job_ended(self, job):
		"""
		Free the resources taken by the job.
		"""
		self._space_list.begin = job.end_time
		assert self._space_list.length >= 0, 'some finished jobs not removed'
		assert hasattr(job, 'res'), 'missing job resources'

		last_space_end = job.start_time + job.time_limit
		it = self._space_list

		while it.end < last_space_end:
			assert not it.reserved, 'reservations not removed'
			it.avail.add(job.res)
			it = it.next

		assert it.end == last_space_end, 'missing job last space'
		assert it.job_last_space > 0, 'invalid space'

		if it.job_last_space == 1:
			# we can safely merge this space with the next one
			it.end = it.next.end
			it.avail = it.next.avail
			it.reserved = it.next.reserved
			assert not it.reserved, 'reservations not removed'
			it.next = it.next.next
			it.job_last_space = it.next.job_last_space
			it.next.next = None  # garbage collection
		else:
			it.avail.add(job.res)
			it.job_last_space -= 1
		# finally clear
		del job.res


class _SingletonNodeMap(_BaseNodeMap):
	"""
	"""

	def __init__(self, nodes=None):
		if nodes:
			self._cpus = nodes[0]
		else:
			self._cpus = 0

	def intersect(self, other):
		self._cpus = min(self._cpus, other._cpus)

	def add(self, other):
		self._cpus += other._cpus

	def remove(self, other):
		self._cpus -= other._cpus

	def clear(self):
		self._cpus = 0

	def size(self):
		return self._cpus

	def __repr__(self):
		return "CPU count: {}".format(self._cpus)


class SingletonManager(BaseManager):
	"""
	"""

	def _node_map(self):
		return _SingletonNodeMap

	def _check_nodes(self, avail, job):
		return job.proc <= avail._cpus

	def _assign_resources(self, avail, job, reservation):
		return self._node_map({0:job.proc})
