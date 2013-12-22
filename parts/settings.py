# -*- coding: utf-8 -*-

"""
Module gathering all the settings from different parts of the system.

Module attributes:
  time_units: a dictionary mapping the permitted time units to seconds.
"""

time_units = {'SEC': 1, 'MINS': 60, 'HOURS': 60*60, 'DAYS': 60*60*24}


class Template(object):
	"""
	A class that corresponds to a single setting.

	Attributes:
	  name: setting name.
	  desc: verbal description.
	  default: some sensible default value, required.
	  time_unit: required if the setting represents time.

	Time unit:
	  see `time_units`.
	"""

	def __init__(self, name, desc, default, time_unit=None):
		self.name = name
		self.desc = desc
		self.default = default
		if time_unit is not None:
			self.time_unit = time_unit.upper()
			assert self.time_unit in time_units
		else:
			self.time_unit = time_unit


class Settings(object):
	"""
	A class containing all of the custom settings.

	To create a new setting simply add an appropriate
	`Template` instance to the templates list.
	"""

	templates = [
		Template('threshold', 'The campaign selection threshold', 10, 'MINS'),
		Template('decay', 'The half-decay period of the CPU usage', 24, 'HOURS'),
		Template('default_limit', 'Default job time limit', 7, 'DAYS'),
		Template('share_file', 'File with user shares', 'shares.txt')
	]

	def __init__(self, **kwargs):
		"""
		Init the class from the values read from the command line.
		"""
		for temp in self.templates:
			value = kwargs[temp.name]
			# change the time if applicable
			if temp.time_unit in time_units:
				value *= time_units[temp.time_unit]
			# set the attribute
			setattr(self, temp.name, value)
