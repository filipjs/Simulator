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

	def __init__(self, begin, end, nodes, next):
		self.begin = begin
		self.end = end
		self.length = end - begin  # needs to be updated manually
		self.nodes = nodes  # 'node map'
		self.next = next
		self.job_last_space = 0 #TODO jakos sie tego pozbyc????

	def __repr__(self):
		s = '[{}, {}] last {}\n{:22}nodes {}'
		return s.format(delta(self.begin), delta(self.end),
			self.job_last_space, '', self.nodes)


class BaseManager(object):
	"""

	"""

	__metaclass__ = ABCMeta

	def __init__(self, nodes, settings):
		self._settings = settings
		self._job_space = _NodeSpace(
					0,
					float('inf'),
					self._node_map(nodes),
					None,
				  )
		# configuration
		self._node_count = len(nodes)
		self._max_cpu_per_node = nodes[0]
		self._cpu_limit = sum(nodes.itervalues())

	def _dump_space(self, *args):
		"""
		Print the current state of node spaces.
		"""
		debug_print(*args)
		print '-' * 30
		it = self._job_space
		while it is not None:
			print '{:20}{}'.format('', it)
			it = it.next
		if self._rsrv_space is not None:
			print '-' * 10 + 'reserved' + '-' * 10
			it = self._rsrv_space
			while it is not None:
				print '{:20}{}'.format('', it)
				it = it.next
		print '-' * 30

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

	def prepare(self, now):
		"""
		Prepare the node manager for the next scheduling pass.
		"""
		# advance the first window
		self._job_space.begin = now
		self._job_space.length = self._job_space.end - now
		assert self._job_space.length > 0, 'some finished jobs not removed'
		# create a new reservation space
		self._reservations = 0
		self._rsrv_limit = now + self._settings.bf_window
		self._rsrv_space = _NodeSpace(
					now,
					self._rsrv_limit,
					self._node_map(),
					None,
				   )

#TODO WINDOW ZROBIC ZE ZAOKRAGLAC TYLKO END TIMY I TIME LIMIT ALE W GORE!!!
#TODO IF RESOLUTION == 0 -> RESULUTION = 1

	def try_schedule(self, job):
		"""
		#TODO LEPSZY OPIS
		Make a reservation for the job.
		Return if the job can be executed immediately.
		"""

		job_it = self._job_space
		first_res = last_res = self._rsrv_space
		failed = False
		while True:

			if failed: #TODO FIXME
				block_begin = last_res.end
			else:
				block_begin = job_it.begin

			# Maybe we can stop, if the potential start is already
			# outside of the backfilling window.
			if self._rsrv_limit <= block_begin:
			#if self._rsrv_limit <= job_it.begin:
				return False

			avail = self.copy(job_it.nodes)
			pred_end = block_begin + job.time_limit
			#pred_end = job_it.begin + job.time_limit

			if self._reservations:
				# Consume the reservations that end earlier
				# than the potential job start.
				while first_res.end <= job_it.begin:
					first_res = first_res.next

				res_it = first_res
				total_res = self._node_map()

				while res_it is not None:
					if pred_end <= res_it.begin:
						break
					else:
						total_res = self.union(total_res,
									res_it.nodes)
						last_res = res_it
						# on the last iteration pred_end <= last_res.end
					res_it = res_it.next

				avail = self.remove(avail, total_res)

			# check
			if self._check_nodes(avail, job):
				first = job_it
				# and find the last space
				while job_it.end < pred_end:
					job_it = job_it.next
				last = job_it
				break

			if job_it.next is not None:
				job_it = job_it.next #TODO FIXME SERIOSULY
			else:
				failed = True
		# Check if the job can be executed now or if it's only
		# a reservation. Get the resources from the `avail` nodes.
		can_run = (first == self._job_space)
		resources = self._assign_resources(avail, job, not can_run)

		if can_run:
			# At this point, we know that the job spans the spaces from `first`
			# to `last` (inclusive). However we have to split the last one.
			if last.end > pred_end:
				# Divide the `last` space appropriately and
				# create a new space to occupy the hole.
				new_space = _NodeSpace(
						pred_end,
						last.end,
						self.copy(last.nodes),
						last.next,
					    )
				new_space.job_last_space = last.job_last_space
				# new space is following `last`
				last.end = pred_end
				last.next = new_space
				last.job_last_space = 0
			last.job_last_space += 1

			# update the available nodes in all spaces
			job.res = resources
			job_it = first
			while True:
				job_it.nodes = self.remove(job_it.nodes, resources)
				if job_it == last:
					break
				job_it = job_it.next

		else:
			#TODO ZROBIC FIRST.BEGIN = PRED_START bo co sie tutaj dzieje..
			assert first_res.end > first.begin, 'not intersecting'
			assert first_res.begin < pred_end, 'not intersecting'
			assert first.begin >= first_res.begin, 'invalid first res begin'

			if first.begin == first_res.begin:
				pass #OK
			else:
				# first > first_res
				new_space = _NodeSpace(
						first.begin,
						first_res.end,
						self.copy(first_res.nodes),
						first_res.next,
					    )
				# new space is following `first_res`
				first_res.end = first.begin
				first_res.next = new_space

				if first_res == last_res:
					last_res = new_space
				first_res = new_space # this is the true start

			# on the last iteration pred_end <= last_res.end
			assert last_res.end > first.begin, 'not intersecting'
			assert last_res.begin < pred_end, 'not intersecting'
			#assert last_res.end >= pred_end, 'invalid last res end'

			if pred_end >= last_res.end:
				pass #OK
			else:
				# pred < last_res
								# first > first_res
				new_space = _NodeSpace(
						pred_end,
						last_res.end,
						self.copy(last_res.nodes),
						last_res.next,
					    )
				# new space is following `last_res`
				last_res.end = pred_end
				last_res.next = new_space

			self._reservations += 1
			res_it = first_res
			while True:
				res_it.nodes = self.add(res_it.nodes, resources)
				if res_it == last_res:
					break
				res_it = res_it.next
		# debug info
		if DEBUG_FLAG:
			self._dump_space('Added resources', job)
		return can_run

	def clear_reservations(self, m=[0]):
		"""
		Scheduling pass is over. Clear the created reservations.
		"""
		it = self._rsrv_space
		i = 0
		while it is not None:
			i += 1
			it = it.next
		if i > m[0]:
			print i
			m[0] = i
		self._rsrv_space = None
		return # TODO USELESS NOW?
		if not self._reservations:
			# nothing to clear
			return

		prev, it = None, self._space_list
		i = 0
		while it.next is not None:
			if not it.job_last_space:
				# we can safely remove this space
				it.next.begin = it.begin
				prev.next = it.next
				it = it.next
			else:
				it.avail = self.add(it.avail, it.reserved)
				it.reserved = self.clear()
				it.reservation_start = False
				prev, it = it, it.next
			i += 1
		# debug info
		if i > m[0]:
			print i
			m[0] = i
		if DEBUG_FLAG:
			self._dump_space('Cleared reservations')

	def job_ended(self, job):
		"""
		Free the resources taken by the job.
		"""
		self._job_space.begin = job.end_time
		self._job_space.length = self._job_space.end - job.end_time
		assert self._job_space.length >= 0, 'some finished jobs not removed'
		assert hasattr(job, 'res'), 'missing job resources'

		last_space_end = job.start_time + job.time_limit
		it = self._job_space

		while it.end < last_space_end:
			it.nodes = self.add(it.nodes, job.res)
			it = it.next

		assert it.end == last_space_end, 'missing job last space'
		assert it.job_last_space > 0, 'invalid space'

		if it.job_last_space == 1:
			# we can safely merge this space with the next one
			it.end = it.next.end
			it.nodes = it.next.nodes
			it.job_last_space = it.next.job_last_space
			# move 'pointers' as the last step
			it.next = it.next.next
		else:
			it.nodes = self.add(it.nodes, job.res)
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

	def union(self, x, y):
		return max(x, y)

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
