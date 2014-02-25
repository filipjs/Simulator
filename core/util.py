# -*- coding: utf-8 -*-

"""
Usefull functions.
"""

def delta(seconds):
	"""
	Return a nicer representation of a deltatime value.
	"""
	if seconds is None:
		return '()'
	m, s = divmod(seconds, 60)
	h, m = divmod(m, 60)
	return '{}:{:02}:{:02}'.format(h, m, s)
