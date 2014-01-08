# -*- coding: utf-8 -*-
import functools
from abc import ABCMeta, abstractmethod
from util import debug_print, delta


# set up debug level for this module
DEBUG_FLAG = False #__debug__
debug_print = functools.partial(debug_print, DEBUG_FLAG, __name__)


"""
#TODO OPIS
"""


class _NodeSpace(object):
	"""

	"""

	def __init__(self, begin, end, avail, reserved, next, last_space):
		self.begin = begin
		self.end = end
		self.avail = avail  # node map
		self.reserved = reserved  # node map
		self.next = next
		self.job_last_space = last_space
		self.reservation_start = False
		#self.update()
		self.length = self.end - self.begin

	#def update(self):
		#self.length = self.end - self.begin

	def __repr__(self):
		pad = ' ' * 22
		s = '[{}, {}] last {}\n{pad}avail {}\n{pad}rsrvd {}'
		return s.format(delta(self.begin), delta(self.end),
			self.job_last_space, self.avail, self.reserved,
			pad=pad)


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
					0
				   )
		# configuration
		self._node_count = len(nodes)
		self._max_cpu_per_node = nodes[0]
		self._cpu_limit = sum(nodes.itervalues())
		self._reservations = 0#TODO REMOVE????
	def _dump_space(self, *args):
		"""
		Print the current state of node spaces.
		"""
		debug_print(*args)
		it = self._space_list
		while it is not None:
			print '{:20}{}'.format(' ', it)
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

	def try_schedule(self, job):
		"""
		"""
		pass #TODO SIMPLE VERSIOB OF BACKFILL

	def prepare_backfill(self, now):
		"""
		Prepare the manager for the upcoming backfilling pass.
		"""
		#self.clear_reservations()#TODO CLEANUP ROBIC W PREPARE
		#TODO ODEJMOWAC W CLEAR + ASSERT + PRIV FUNC
		self._reservations = 0
		self._now = now
		self._space_list.begin = now  # advance the first window
		self._space_list.length = self._space_list.end - now
		#self._space_list.update()
		assert self._space_list.length > 0, 'some finished jobs not removed'

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
				must_check = it.reservation_start
			else:
				total_time = 0
				#TODO OPIS
				it = first = first.next
				avail = None
				must_check = True
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
			new_space = _NodeSpace(
					first.begin + job.time_limit,
					last.end,
					self.copy(last.avail),
					self.copy(last.reserved),
					last.next,
					last.job_last_space
				    )

			# new space is following `last`
			last.end = new_space.begin
			last.length = last.end - last.begin
			#last.update()
			last.next = new_space
			last.job_last_space = 0

		# get the resources from the `avail` node map
		res = self._assign_resources(avail, job, not can_run)
		if can_run:
			job.res = res
			last.job_last_space += 1
		else:
			self._reservations += 1
			first.reservation_start = True #TODO OPIS

		# update the available nodes in all spaces
		it = first
		while True:
			it.avail = self.remove(it.avail, res)
			if not can_run:
				it.reserved = self.add(it.reserved, res)
			if it == last:
				break
			it = it.next
		# debug info
		if DEBUG_FLAG:
			self._dump_space('Added resources', job)
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
				it.next.length = it.next.end - it.next.begin #TODO NOZ KURWA
				prev.next = it.next
				it = it.next
			else:
				it.avail = self.add(it.avail, it.reserved)
				it.reserved = self.clear()
				it.reservation_start = False
				prev, it = it, it.next
		# debug info
		if DEBUG_FLAG:
			self._dump_space('Cleared reservations')

	def job_ended(self, job):
		"""
		Free the resources taken by the job.
		"""
		self._space_list.begin = job.end_time
		self._space_list.length = self._space_list.end - job.end_time
		#self._space_list.update()
		assert self._space_list.length >= 0, 'some finished jobs not removed'
		assert hasattr(job, 'res'), 'missing job resources'

		last_space_end = job.start_time + job.time_limit
		it = self._space_list

		while it.end < last_space_end:
			assert not self.size(it.reserved), 'reservations not removed'
			it.avail = self.add(it.avail, job.res)
			it = it.next

		assert it.end == last_space_end, 'missing job last space'
		assert it.job_last_space > 0, 'invalid space'

		if it.job_last_space == 1:
			# we can safely merge this space with the next one
			it.end = it.next.end
			it.length = it.end - it.begin
			#it.update()
			it.avail = it.next.avail
			it.reserved = it.next.reserved
			assert not self.size(it.reserved), 'reservations not removed'
			it.job_last_space = it.next.job_last_space
			# move 'pointers' as the last step
			it.next = it.next.next
		else:
			it.avail = self.add(it.avail, job.res)
			it.job_last_space -= 1
		# finally clear
		del job.res
		# debug info
		if DEBUG_FLAG:
			self._dump_space('Removed resources', job)

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
