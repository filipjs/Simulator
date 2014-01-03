# -*- coding: utf-8 -*-

def debug_print(*args, flag=None, name=None):
	"""
	Print the arguments if the `DEBUG_FLAG` is on.
	"""
	if flag:
		print name, ":",
		print ' '.join(map(str, args))
