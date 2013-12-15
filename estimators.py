#!/usr/bin/env python
# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class BaseEstimator(object):
	"""
	"""

	__metaclass__ = ABCMeta

	def initial_estimate(self, job):
		"""
		"""
		raise NotImplemented

	def next_estimate(self, job, prev_estimate):
		"""
		"""
		raise NotImplemented


class SimpleEstimator(BaseEstimator):
	"""
	"""

	def initial_estimate(self, job):
		"""
		"""
		return job.time_limit

	def next_estimate(self, job, prev_estimate):
		"""
		"""
		raise Exception("job run longer than time limit")


