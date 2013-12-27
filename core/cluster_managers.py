# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


"""
"""


class _NodeSpace(object):
	"""

	"""

	def __init__(self, begin, end, nodes, reserved, next):
		self.begin = begin
		self.end = end
		self.nodes = nodes
		self.reserved = reserved
		self.next = next

	@property
	def size(self):
		return self.end - self.begin


class _BaseNodeBitmap(object):
	"""

	"""

	__metaclass__ = ABCMeta

	def __init__(self, another=None):
		if another is None:
			self._new_bitmap()
		else:
			self._copy_bitmap(another)

	@abstractmethod
	def _new_bitmap(self):
		pass
	@abstractmethod
	def _copy_bitmap(self, another):
		pass
	@abstractmethod
	def add_bitmap(self, another):
		pass
	@abstractmethod
	def remove_bitmap(self, another):
		pass
	@abstractmethod
	def add_reserved(self, another):
		pass
	@abstractmethod
	def remove_reserved(self, another):
		pass


class ClusterManager(object):
	"""

	"""

	def __init__(self):
		self._cpu_used = 0

	@property
	def cpu_used(self):
		return self._cpu_used

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
		BaseClusterManager.__init__(self)
		self._cpu_limit = count
		self._space_list = _NodeSpace(0, float('inf'), count, None)
		self._space_list.reserved = 0

	def runnable_test(self, job):
		"""

		"""
		return job.proc <= self._cpu_limit

	def try_schedule(self, job, now, bf_window):

		total_time = 0
		first = it = self._space_list

		it.begin = now  # advance the first window
		assert it.size > 0, 'some finished job not removed'

		while True:
			if it.nodes >= job.proc:
				total_time += it.size
				if total_time >= job.time_limit:
					last = it
					break
			else:
				total_time = 0
				first = it.next
			# next
			it = it.next

		# check if the job can be executed
		can_run = (first == self._space_list)

		if first.begin > now + bf_window:
			# reservation outside the backfilling window
			assert not can_run, 'invalid bf_window'
			return can_run

		# At this point, we know that the job spans the spaces from
		# `first` to `last`. However the `last` one might need to be split.
		# Start by updating the available nodes in all but the last space.
		it = first
		while it != last:
			it.nodes -= job.proc
			if not can_run:
				it.reserved += job.proc
			it = it.next

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
			last_space.end = new_space.begin
			last_space.next = new_space

		last_space.nodes -= job.proc
		if not can_run:
			last_space.reserved += job.proc

		if can_run:
			job.start_execution(now)
			self._cpu_used += job.proc
		return can_run

	def clear_reservations(self):
		"""

		"""
		it = self._space_list
		while it is not None:
			it.nodes += it.reserved
			it.reserved = 0
#TODO GDZIES ASSERTY ZE NIE MA NIC RESERVED!!
#TODO PRZED KAZDA FUNKCJA TRZEBA ROBIC ZWIJANIE AKA FIRST.BEGIN = NOW
#TODO I TERAZ MOZE SIE ZDAZYC ZE SIZE < 0 ALE TYLKO JESLI FIRST.NODES == NEXT.NODES??
#TODO AKA PRACA SKONCZYLA SIE PRZED TIME LIMIT??

#TODO ZROBIC MERGE SPACE JESLI NODES == NODES??
#TODO I WTEDY W JOB.END POTRZEBA 'UNMERGE'

	def job_ended(self, job, now):
		"""

		"""
		job.execution_ended(now)
		self._cpu_used -= job.proc
		# Node spaces are built based on `job.time_limit`,
		# but the actual `job.run_time` can be shorter.

		it = self._space_list
		while True:
			if it.end > job.start_time + job.time_limit:
				break
			it.nodes += job.proc
			it = it.next
