# -*- coding: utf-8 -*-

def debug_print(flag, name, *args):
	"""
	Print the arguments if the `DEBUG_FLAG` is on.
	"""
	if flag:
		print name, ":",
		print ' '.join(map(str, args))
