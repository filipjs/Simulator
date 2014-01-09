# -*- coding: utf-8 -*-
from simulator import GeneralSimulator


class VirtualSimulator(GeneralSimulator):
	"""
	A specialized version of the `GeneralSimulator`.
	Only keeps track about virtual campaigns.
	"""

	def _real_first_stage(self, period):
		pass

	def _next_force_decay(self):
		pass

	def _finalize(self):
		GeneralSimulator._finalize(self)
		assert not self._diag.forced, 'decay calculated'


class RealSimulator(GeneralSimulator):
	"""
	A specialized version of the `GeneralSimulator`.
	Only keeps track about real CPU usage.
	"""

	def _virt_first_stage(self, period):
		pass

	def _virt_second_stage(self):
		pass

	def _update_camp_estimates(self):
		pass

	def _finalize(self):
		"""
		We have to manually 'finish' the campaigns.
		"""
		GeneralSimulator._finalize(self)

		GeneralSimulator._virt_first_stage(self, float('inf'))
		GeneralSimulator._virt_second_stage(self)

		for u in self._users.itervalues():
			assert u.active_camps, 'inactive user'
			inactive = self._camp_end_event(u.active_camps[0])
			assert inactive, 'active user'
			assert not u.active_camps, 'active user'