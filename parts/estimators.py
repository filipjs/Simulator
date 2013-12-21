#!/usr/bin/env python
# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class BaseEstimator(object):
	"""
	"""

	__metaclass__ = ABCMeta

	def __init__(self, settings):
		self._settings = settings

	def initial_estimate(self, job):
		"""
		"""
		est = self._get_initial(job)
		assert job.estimate is None
		return est

	def next_estimate(self, job):
		"""
		"""
		prev = job.estimate
		est = self._get_next(job, prev)
		assert job.estimate == prev
		assert est > prev
		return est

	@abstractmethod
	def _get_initial(self, job):
		"""
		"""
		raise NotImplemented

	@abstractmethod
	def _get_next(self, job, prev_estimate):
		"""
		"""
		raise NotImplemented


class SimpleEstimator(BaseEstimator):
	"""
	"""

	def __init__(self, *args):
		BaseEstimator.__init__(self, *args)

	def _get_initial(self, job):
		"""
		"""
		return job.time_limit

	def _get_next(self, job, prev_estimate):
		"""
		"""
		raise Exception("job exceeded its time limit")
