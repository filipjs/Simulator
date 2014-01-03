# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
import copy

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
	def union(self, another):
		pass
	@abstractmethod
	def intersect(self, another):
		pass
	@abstractmethod
	def remove_res(self, another):
		pass


class BaseNodeManager(object):
	"""

	"""

	__metaclass__ = ABCMeta

	#def __init__(self):
		#self._space_list = _NodeSpace(0, float('inf'), count, None)
		#self._space_list.reserved = 0

	def __init__(self, settings):
		assert settings.bf_window > 0, 'invalid bf_window'
		assert settings.bf_resolution > 0, 'invalid bf_resolution'
		self._settings = settings
		self._space_list = self._get_initial_space()

	@abstractmethod
	def sanity_test(self, job):
		"""
		Return if the job is ever runnable in the current configuration.
		"""
		raise NotImplemented

	@abstractmethod
	def _get_initial_space(self):
		"""
		Return the initial `_NodeSpace` element.
		"""
		raise NotImplemented

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

		# check if the job can be executed now
		can_run = (first == self._space_list)

		if not can_run and first.begin > self._now + self._settings.bf_window
			# reservation outside of the backfilling window
			return False

		# At this point, we know that the job spans the spaces from
		# `first` to `last`. However the `last` one might need to be split.
		extra_time = total_time - job.time_limit

#TODO JAK WINDOW TO JUZ TUTAJ I WTEDY ZAOOKRAGLAC LAST>END I NEW>BEGIN??

#/* If we decrease the resolution of our timing information, this can
#* decrease the number of records managed and increase performance */
#start_time = (start_time / backfill_resolution) * backfill_resolution;
#end_reserve = (end_reserve / backfill_resolution) * backfill_resolution;

		if extra_time > 0:
			# Divide the `last` space appropriately and
			# create a new space to occupy the rest.
			new_space = copy.deepcopy(last)
			new_space.begin = last.end - extra_time

			last.end = new_space.begin
			last.next = new_space


		# get the resources from the `avail` node map
		res = self._assign_resources(avail, job, not can_run)

		# update the nodes in all spaces
		it = first
		while it != last:
			it.avail.remove_res(res)
			if not can_run:
				it.reserved.union(res)
			it = it.next

		return can_run

	def clear_reservations(self):
		"""
		Scheduling pass is over. Clear the created reservations.
		"""
		it = self._space_list
		while it is not None:
			it.avail.union(it.reserved)
			it.reserved = self._node_map()
			it = it.next


class _SingletonNodeMap(_BaseNodeMap):
	"""
	"""
	pass

class SingletonNodeManager(BaseNodeManager):
	"""
	"""

	def __init__(self, nodes, settings):
		BaseNodeManager.__init__(self, settings)

		#self._cpu_used = 0
		#self._cpu_limit = cpus

	#def sanity_test(self, job):
		#return job.proc <= self._cpu_limit

	#def clear_reservations(self):
		#"""

		#"""
		#it = self._space_list
		#while it is not None:
			#it.nodes += it.reserved
			#it.reserved = 0
##TODO GDZIES ASSERTY ZE NIE MA NIC RESERVED!!
##TODO PRZED KAZDA FUNKCJA TRZEBA ROBIC ZWIJANIE AKA FIRST.BEGIN = NOW
##TODO I TERAZ MOZE SIE ZDAZYC ZE SIZE < 0 ALE TYLKO JESLI FIRST.NODES == NEXT.NODES??
##TODO AKA PRACA SKONCZYLA SIE PRZED TIME LIMIT??

##TODO ZROBIC MERGE SPACE JESLI NODES == NODES??
##TODO I WTEDY W JOB.END POTRZEBA 'UNMERGE'

	#def job_ended(self, job, now):
		#"""

		#"""
		#job.execution_ended(now)
		#self._cpu_used -= job.proc
		## Node spaces are built based on `job.time_limit`,
		## but the actual `job.run_time` can be shorter.

		#it = self._space_list
		#while True:
			#if it.end > job.start_time + job.time_limit:
				#break
			#it.nodes += job.proc
			#it = it.next
