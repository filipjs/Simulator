# -*- coding: utf-8 -*-

def debug_print(flag, module, *args):
	"""
	Print the arguments if the `DEBUG_FLAG` is on.
	"""
	if flag:
		name = module.split('.')[-1]
		msg = ' '.join(map(str, args))
		print '{:16}: {}'.format(name, msg)


def delta(seconds):
	"""
	Return a nicer representation of a deltatime value.
	"""
	if not seconds:
		return '()'
	m, s = divmod(seconds, 60)
	h, m = divmod(m, 60)
	return '{}:{:02}:{:02}'.format(h, m, s)
