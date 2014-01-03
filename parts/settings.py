# -*- coding: utf-8 -*-

"""
Module gathering the settings from different parts of the system.

Module attributes:
  time_units: a dictionary mapping the permitted time units to seconds.
  alg_templates: a combined list of all the algorithm specific settings.
  part_templates: a list describing chosen system parts.
  sim_templates: a list of general simulation settings.

Customizing:
  To create new settings for your algorithm simply add an appropriate `Template`
  instance to the `alg_templates`.

Note:
  **DO NOT** modify other lists unless you really know what you are doing.
"""


class Template(object):
	"""
	A class that corresponds to a single setting.

	Attributes:
	  name: setting name.
	  desc: verbal description.
	  default: a sensible default value.
	  time_unit: required if the setting represents time.
	  loc: (optional) location where the setting is used.

	Time unit:
	  see `time_units`.
	"""

	def __init__(self, name, desc, default, time_unit=None, loc=None):
		assert default is not None
		self.name = name
		self.desc = desc
		self.default = default
		if time_unit is not None:
			self.time_unit = time_unit.upper()
			assert self.time_unit in time_units
		else:
			self.time_unit = time_unit
		self.loc = loc


time_units = {'SEC': 1, 'MINS': 60, 'HOURS': 60*60, 'DAYS': 60*60*24}


# Add your settings here.
alg_templates = [
	Template('threshold', 'The campaign selection threshold', 10, 'MINS',
		 loc='VirtualSelector'),
	Template('decay', 'The half-decay period of the CPU usage', 24, 'HOURS',
		 loc='FairshareScheduler'),
	Template('default_limit', 'Default job time limit', 7, 'DAYS',
		 loc='DefaultTimeSubmitter'),
	Template('share_file', 'File with user shares', 'shares.txt',
		 loc='CustomShare'),
	Template('bf_depth', 'The maximum number of jobs to backfill', 50),
	Template('bf_window', 'The amount of time to look into the future'
		 'when considering jobs for backfilling', 24, 'HOURS'),
	Template('bf_resolution', 'The resolution of the backfilling data', 1, 'SEC'),
]

# You can change the default classes here.
part_templates = [
	Template('estimator', 'The estimator class', 'NaiveEstimator'),
	Template('submitter', 'The submitter class', 'OracleSubmitter'),
	Template('selector', 'The selector class', 'VirtualSelector'),
	Template('schedulers', 'The scheduler classes',
		 ['OStrich', 'SlurmFairshare']),
	Template('share', 'The share assigner class', 'EqualShare')
]

sim_templates = [
	Template('title', 'The title of the simulation', 'title'),
	Template('job_id', 'Start from the job with this ID', 0),
	Template('block_time', 'Divide the simulation in `block_time`'
		 ' long parts', 0, 'DAYS'),
	Template('block_margin', 'Extra simulation time to fill up'
		 ' and empty the cluster', 0, 'HOURS'),
	Template('one_block', 'Simulate only the first block', False),
	Template('serial', 'Change parallel jobs to serial versions', False),
	Template('cpu_count', 'Set a static number of CPUs, takes precedence', 0),
	Template('cpu_percent', 'Set the number of CPUs to the P-th percentile', 70),
	Template('cpu_per_node', 'Divide the cluster into nodes with'
		 'said number of CPUs', 0),
]


class Settings(object):
	"""
	A simple namespace containing the settings from a given template list.

	Settings corresponding to time are automatically changed to seconds.
	"""

	def __init__(self, templates, **kwargs):
		"""
		Args:
		  templates: a list of `Template` instances.
		  **kwargs: values read from the command line.
		"""
		for temp in templates:
			value = kwargs[temp.name]
			# change the time if applicable
			if temp.time_unit in time_units:
				value *= time_units[temp.time_unit]
			# set the attribute
			setattr(self, temp.name, value)
