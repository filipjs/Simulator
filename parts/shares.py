# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

"""
Shares:
	Assign the number of CPU shares to each user.
	One share translates to (100.0 / shares_sum) percent of all the CPUs.

Customizing:
	Create a new subclass of `BaseShare` and override the required methods.
	To add new settings to use in your subclass see :class: `Settings` documentation.
"""


class BaseShare(object):
	"""
	Shares base class. Subclasses are required to override:

	1) _get_share

	You can access the `Settings` using `self._settings`.
	"""

	__metaclass__ = ABCMeta

	def __init__(self, settings):
		"""
		Init the class with a `Settings` instance.
		"""
		self._settings = settings

	def user_share(self, user):
		"""
		Public wrapper method.
		Run and check the correctness of `_get_share`.
		"""
		share = self._get_share(user)
		assert share > 0
		return share

	@abstractmethod
	def _get_share(self, user):
		"""
		Return the number of shares to assign to the user.

		Note:
		  **DO NOT** set the `user.shares` yourself.
		"""
		raise NotImplemented


class EqualShare(BaseShare):
	"""
	Each user gets an equal number of shares.
	"""

	def _get_share(self, user):
		return 1


class CustomShare(BaseShare):
	"""
	Read the shares values from a file.
	Users `Settings.shares_file` to read from.
	"""

	def __init__(self, *args):
		"""
		Read the specified file only once in init.
		"""
		BaseShare.__init__(self, *args)

		self._shares = {}
		for line in open(self._settings.shares_file):
			if line:
				# non-empty line consists of a pair <user, share>
				user, share = map(int, line.split())
				self._shares[user] = share

	def _get_share(self, user):
		"""
		Return the value from the file or a default value.
		"""
		return self._shares.get(user.ID, 1)
