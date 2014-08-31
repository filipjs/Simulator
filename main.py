#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import argparse
import functools
import glob
import importlib
import logging
import multiprocessing
import os
import sys
import time
import zlib
from core import parsers, simulator, spec_sim
from parts import settings


PROFILE_FLAG = False
PAGE_BREAK = '-' * 50


##
## Action ``run``.
##


class Block(object):
    """
    A block uses a set of indexes to extract the appropriate
    job slice from the full list of jobs.

    Note: all indexes are inclusive:
      left - (index of) the first job of the left margin
      first - (index of) the first job of the core
      last - (index of) the last job of the core
      right - (index of) the last job of the right margin
    """

    def __init__(self, jobs, inx, block_time, num):
        """
        Args:
          jobs: full list of jobs.
          inx: dictionary with indexes.
          block_time: core length.
          num: block number

        """
        self._jobs = jobs[inx['left']:inx['right']+1]
        self._first_core = jobs[inx['first']]
        self._block_time = block_time

        self.core_count = inx['last'] - inx['first'] + 1
        self.margin_count = len(self._jobs) - self.core_count

        self.number = num

    @property
    def core_period(self):
        return (self._first_core.submit,
            self._first_core.submit + self._block_time)

    def __len__(self):
        return len(self._jobs)

    def __getitem__(self, key):
        return self._jobs[key]

    def __iter__(self):
        return iter(self._jobs)

    def __repr__(self):
        s = 'Block {:2} (core id {}): {} jobs' \
            ' (inc. {} margin jobs), {} CPUs'
        return s.format(self.number, self._first_core.ID,
                len(self), self.margin_count, self.cpus)


def divide_jobs(jobs, first_job, block_time, block_margin):
    """
    Divide the jobs into potentially many smaller blocks.
    Each block can have extra jobs to fill up and empty the cluster.

    Args:
      jobs: list of all the jobs.
      first_job: ID of the first job to start with or ``zero``.
      block_time: length of each block in seconds or ``zero``.
      block_margin: extra length added to the blocks on both sides.

    Return:
      a list of consecutive blocks as `Block` instances.
    """
    if first_job:
        for i, j in enumerate(jobs):
            if j.ID == first_job:
                break
        else:
            raise Exception('job ID %s not found' % first_job)
    else:
        i = 0

    # 'i' now points to first job of the first block
    blocks = []

    while i < len(jobs):
        inx = {'first': i}
        st = jobs[i].submit
        end = st + block_time

        while i >= 0 and st - jobs[i].submit <= block_margin:
            i -= 1

        inx['left'] = i + 1

        if not block_time:
            inx['last'] = inx['right'] = len(jobs) - 1
            block_time = float('inf')
        else:
            i = inx['first']
            while i < len(jobs) and jobs[i].submit < end:
                i += 1
            inx['last'] = i - 1
            while i < len(jobs) and jobs[i].submit < end + block_margin:
                i += 1
            inx['right'] = i - 1

        blocks.append(
            Block(jobs, inx, block_time, len(blocks))
        )
        i = inx['last'] + 1  # margins from consecutive blocks can overlap

    return blocks


def cpu_percentile(block, percentile):
    """
    Return the number of CPUs equal to the p-th `percentile`
    of the system resource usage over the block simulation period.

    Note:
      We are assuming that the system has no CPU limit.
    """
    events = {}  # pairs <time-stamp, change in the number of CPUs>
    last_event = block[-1].submit

    for j in block:
        events[j.submit] = events.get(j.submit, 0) + j.proc
        end = min(j.submit + j.run_time, last_event)
        events[end] = events.get(end, 0) - j.proc

    events = sorted(events.iteritems())  # sort by time-stamp
    prev_event, cpus = events[0][0], 0
    util = {}  # pairs <number of CPUs, total period with that CPU count>

    for time_stamp, diff in events:
        period = time_stamp - prev_event
        prev_event = time_stamp
        util[cpus] = util.get(cpus, 0) + period
        cpus += diff

    total = sum(util.itervalues())  # total simulation period
    find = int(percentile / 100.0 * total)

    # sort by CPU usage and find the percentile
    util = sorted(util.iteritems())
    elements = 0
    for cpus, period in util:
        elements += period
        if elements >= find:
            return cpus
    else:
        raise Exception('invalid percentile %s' % percentile)


def remove_top(jobs, users, count):
    """
    Remove the jobs of the `count` most active users.
    """
    assert count < len(users), 'too many to remove'

    usage = {}
    for j in jobs:
        usage[j.user] = usage.get(j.user, 0) + j.run_time * j.proc
    usage = sorted(usage.iteritems(), key=lambda x: x[1], reverse=True)

    for i in range(count):
        to_del = usage[i][0]
        # mark jobs as None for now
        for k, j in enumerate(jobs):
            if j.user == to_del:
                j.user = None # unlink
                jobs[k] = None
        # filter marked jobs all at once
        jobs = filter(None, jobs)
        # now delete the user
        del users[to_del.ID]
    return jobs, users


def make_classes(name, conf, modules=[]):
    """
    Return an instance of the class `name` from the ``parts`` package.
    Each class must take *exactly one* init argument `conf`.

    Args:
      name: name of the class OR a list of names.
      conf: argument passed to the class constructor.
      modules: a cache with loaded modules from the `parts` package.

    """
    if not modules:
        for f in glob.glob('parts/*.py'):
            m = f.replace('/', '.')[:-3]
            m = importlib.import_module(m)
            modules.append(m)

    if isinstance(name, list):
        return [make_classes(n, conf) for n in name]

    for m in modules:
        if hasattr(m, name):
            cl = getattr(m, name)
            return cl(conf)  # create instance
    else:
        raise Exception('class not found %s' % name)


def print_runtime_stats(diag):
    """
    Print the diagnostic statistics from a simulation.
    """

    if diag.skipped:
        logging.warn("Skipped %s jobs that couldn't run" % diag.skipped)

    # change some stats to percentages
    diag.bf_jobs *= 100
    diag.avg_util *= 100

    line0 = '    Backfilled jobs {bf_jobs:.2f}%, average utilization {avg_util:.2f}%'
    line1 = '    Backfill loops {bf_pass}, sched loops {sched_pass}'
    line2 = '    Simulation time {sim_time:.2f}s, decay events {forced}'

    logging.info(line0.format(**vars(diag)))
    logging.info(line1.format(**vars(diag)))
    logging.info(line2.format(**vars(diag)))


def simulate_block(block, sched, alg_conf, part_conf):
    """
    Do a simulation on the specified block.

    Args:
      block: `Block` with `Jobs` to simulate.
      sched: selected scheduler from part_conf.schedulers.
      alg_conf: algorithmic settings.
      part_conf: parts instances.

    Return:
      a string with all the events.
      diagnostic statistics from the run.

    """
    assert block.cpus, 'invalid block cpu count'

    # extract the users and reset all instances
    users = {}

    for j in block:
        assert not hasattr(j, 'camp'), 'job not reset'
        j.reset()
        assert hasattr(j, 'camp'), 'invalid job reset'
        users[j.user.ID] = j.user

    for u in users.itervalues():
        assert not hasattr(u, 'active_camps'), 'user not reset'
        u.reset()
        assert hasattr(u, 'active_camps'), 'invalid user reset'

    part_conf.scheduler = sched
    params = (block, users, alg_conf, part_conf)

    assert not sched.only_virtual or not sched.only_real, 'invalid scheduler'
    if sched.only_virtual:
        my_sim = spec_sim.VirtualSimulator(*params)
    elif sched.only_real:
        my_sim = spec_sim.RealSimulator(*params)
    else:
        my_sim = simulator.GeneralSimulator(*params)

    logging.log(15, '{} using {}'.format(sched, my_sim))

    if not PROFILE_FLAG:
        return my_sim.run()
    else:
        import cProfile
        cProfile.runctx('print my_sim.run()[1].__dict__',
                globals(), locals(),
                sort='cumulative')
        sys.exit(0)


def setup_logging(debug, config):
    """
    Set the root logger.
    """
    if debug:
        lvl = logging.DEBUG
    else:
        lvl = 15
    fmt = '%(levelname)s: %(message)s'
    logging.basicConfig(
        filename='logs/sim-%s.log' % config.replace('/', '-'),
        filemode='w',
        format=fmt,
        level=lvl
    )
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(ch)


def group_jobs(jobs, threshold):
    """
    Change the submit time of jobs to simulate campaigns.
    """
    last_camp = {}

    for j in jobs:
        jid = j.user.ID
        if jid in last_camp:
            if j.submit < last_camp[jid] + threshold:
                j.submit = last_camp[jid]
        last_camp[jid] = j.submit


def run(workload, args):
    """
    Run the simulation described in `args` on the `workload`.

    Run one simulation for each supplied ``scheduler``.
    """

    multi_sched = len(args['schedulers']) > 1
    multi_blocks = args['block_time'] and not args['one_block']

    run_async = not PROFILE_FLAG and (multi_sched or multi_blocks)

    if run_async:
        # Prepare the worker pool. Leave one CPU free,
        # so the operating system can stay responsive.
        my_pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)

    # encapsulate different settings
    sim_conf = settings.Settings(settings.sim_templates, **args)
    alg_conf = settings.Settings(settings.alg_templates, **args)
    part_conf = settings.Settings(settings.part_templates, **args)

    # before we start, check the output directory
    if not os.path.isdir(sim_conf.output):
        raise Exception('invalid output directory %s' % sim_conf.output)

    # now we need to load and instantiate the classes from `part_conf`
    for key, value in part_conf.__dict__.items():
        setattr(part_conf, key, make_classes(value, alg_conf))

    # parse the workload
    my_parser = parsers.get_parser(workload)
    jobs, users = my_parser.parse_workload(workload, sim_conf.serial)
    jobs.sort(key=lambda j: j.submit)  # order by submit time

    for j in jobs:
        j.run_time = max(j.run_time, 60)

    if sim_conf.pre_group:
        group_jobs(jobs, alg_conf.threshold)

    # remove some jobs if requested
    if sim_conf.skip_top:
        jobs, users = remove_top(jobs, users, sim_conf.skip_top)

    # set job time limit and validate run time
    killed = 0
    for j in jobs:
        j.time_limit = part_conf.submitter.time_limit(j)
        if j.run_time > j.time_limit:
            j.run_time = j.time_limit
            killed += 1
    if killed:
        logging.warn('%s jobs will end prematurely due to insufficient'
                     ' time limit' % killed)

    # set user shares
    shares = {}
    for uid, u in users.iteritems():
        shares[uid] = part_conf.share.user_share(u)
    # shares must be normalized
    total_shares = sum(shares.itervalues()) * 1.0
    for uid, u in users.iteritems():
        u.shares = shares[uid] / total_shares

    # divide into blocks
    blocks = divide_jobs(jobs, sim_conf.job_id, sim_conf.block_time,
                         sim_conf.block_margin)
    if sim_conf.one_block:
        if sim_conf.block_number >= len(blocks):
            logging.error('Block number [%s] out of range'
                          % sim_conf.block_number)
            sys.exit(1)
        else:
            blocks = [ blocks[sim_conf.block_number] ]

    results = {sched: [] for sched in part_conf.schedulers}
    global_start = time.time()

    logging.info(PAGE_BREAK)
    logging.info('Simulation started. Block count %s' % len(blocks))

    for bl in blocks:
        # calculate the CPU number
        if sim_conf.cpu_count:
            cpus = sim_conf.cpu_count
        else:
            cpus = cpu_percentile(bl, sim_conf.cpu_percent)

        bl.cpus = cpus

        for sched in part_conf.schedulers:
            params = (bl, sched, alg_conf, part_conf)

            if run_async:
                async_r = my_pool.apply_async(simulate_block, params)
                results[sched].append(async_r)
            else:
                r = simulate_block(*params)
                results[sched].append(r)

    # wait for the asynchronous results
    if run_async:
        for sched_results in results.itervalues():
            for sim_result in sched_results:
                # ctr-c doesn't work without a timeout
                sim_result.wait(60*60*24*365)

    for sched, sched_results in results.iteritems():
        time_stamp = time.localtime(global_start)

        title = sim_conf.title
        if sim_conf.one_block:
            title += '-b%02d' % sim_conf.block_number

        filename = '{}-{}-{}'.format(
            title,
            sched,
            time.strftime('%b%d_%H-%M', time_stamp)
        )
        filename = os.path.join(sim_conf.output, filename)

        f = open(filename, 'w')
        f.write('# Description of the output can be found in core/simulator.py'
                ' in the GeneralSimulator._store_X methods\n')
        f.write('%s\n' % args)  # original arguments
        f.write('SIMULATION START %s\n' % time.ctime(global_start))

        for i, sim_result in enumerate(sched_results):
            if sim_conf.cpu_count:
                m = '{0} : {1}'
            else:
                m = '{0} ({2}-th percentile) : {1}'
            logging.info(m.format(blocks[i], sched, sim_conf.cpu_percent))

            if run_async:
                r, diag = sim_result.get(5)
            else:
                r, diag = sim_result

            print_runtime_stats(diag)
            # save partial results to file
            f.write('BLOCK START %s\n' % blocks[i].cpus)
            f.write(zlib.decompress(r))
            f.write('BLOCK END %s\n' % diag.__dict__)
        f.close()

        logging.info('Results saved to file %s' % filename)
        logging.info(PAGE_BREAK)

    logging.info('Simulation completed. Total run time %.2f'
                 % (time.time() - global_start))
    logging.info(PAGE_BREAK)

    if run_async:
        my_pool.close()
        my_pool.join()


##
## Action ``config``.
##


def config(args):
    """
    Create a configuration file based on the `Template` lists.
    Generating takes default values.
    Recreating takes values from a file with the simulation results.
    """
    if args['generate']:
        values = {}
    else:
        from ast import literal_eval
        with open(args['recreate']) as f:
            for line in f:
                if line[0] == '#':
                    continue
                elif line[0] == '{':
                    values = literal_eval(line)
                else:
                    raise Exception('no context in file %s'
                            % args['recreate'])

    def str_value(value):
        """
        Change the value to a printable version.
        """
        if isinstance(value, list):
            return ' '.join(map(str, value))
        return str(value)

    def print_template(temp):
        """
        Print a `Template`.
        """
        value = values.get(temp.name, temp.default)
        unit = temp.time_unit or ''

        print '--{:15}{}'.format(temp.name, str_value(value))
        print '# {}: (default) {} {}'.format(
              temp.desc, str_value(temp.default), unit)
        if temp.loc is not None:
            print '## Used by `{}` class.'.format(temp.loc)

    print '\n##\n## General simulation parameters\n##\n'
    map(print_template, settings.sim_templates)
    print '\n##\n## Algorithm specific parameters\n##\n'
    map(print_template, settings.alg_templates)
    print '\n##\n## Part selection parameters\n##\n'
    map(print_template, settings.part_templates)


##
## Action ``stats``.
##


def threshold_percentile(jobs):
    """
    Display the inter-arrival statistic.
    """
    last_sub = {}
    values = []

    for j in jobs:
        jid = j.user.ID
        if jid in last_sub:
            values.append(j.submit - last_sub[jid])
        last_sub[jid] = j.submit

    logging.info('Legend: inter-arrival cdf, time in seconds')
    values.sort()
    for i in xrange(70, 100):
        p = i / 100.
        inx = int(p * len(values))
        logging.info('{:.2f} {}'.format(p, values[inx]))
    logging.info(PAGE_BREAK)


def top_usage(jobs):
    """
    Display the usage of the most active users.
    """
    usage = {}
    total = 0.

    for j in jobs:
        jid = j.user.ID
        usage[jid] = usage.get(jid, 0) + j.run_time * j.proc
        total += j.run_time * j.proc

    logging.info('Legend: user ID, percent of total usage')
    usage = sorted(usage.iteritems(), key=lambda x: x[1], reverse=True)
    rest = total
    for i in range(min(10, len(usage))):
        logging.info('{:>4} {:.2f}'.format(
            usage[i][0],
            usage[i][1] / total * 100
        ))
        rest -= usage[i][1]
    logging.info('rest {:.2f}'.format(rest / total * 100))
    logging.info(PAGE_BREAK)


def display_stats(workload, args):

    my_parser = parsers.get_parser(workload)
    jobs, users = my_parser.parse_workload(workload, 0)

    threshold_percentile(jobs)
    top_usage(jobs)


##
## Argument parsing
##


run_opts = '[SIM_OPTS][ALG_OPTS][PART_OPTS] workload_file'

global_desc = """INSTRUCTIONS
----------------------------------------------------------------
To run a cluster simulation:
    `%(prog)s run {}`
To read the options from a config file:
    `%(prog)s run @myconfig workload_file`

You can generate a template of the configuration:
    `%(prog)s config --generate > my_conf`.
You can also recreate a config from a simulation:
    `%(prog)s config --recreate sim_file > my_conf`

To display some workload statistics:
    `%(prog)s stats workload_file
----------------------------------------------------------------
""".format(run_opts)


def arguments_from_templates(parser, templates):
    """
    Add arguments to the parser based on the `Template` list.
    """

    def str2bool(v):
        if v.lower() not in ['true', 'false']:
            raise argparse.ArgumentTypeError('choose from True, False')
        return v.lower() == 'true'

    def positive(atype, v):
        v = atype(v)
        if v < 0:
            raise argparse.ArgumentTypeError('negative value')
        return v

    for temp in templates:
        assert temp.default is not None, 'invalid default value'

        opts = {'metavar': temp.time_unit,
                'default': temp.default,
                'help': temp.desc}

        ftypes = {bool: str2bool,
                  int: functools.partial(positive, int),
                  float: functools.partial(positive, float),
                  str: str}

        if isinstance(temp.default, list):
            # multiple values
            opts['type'] = ftypes[type(temp.default[0])]
            opts['nargs'] = '*'
        else:
            opts['type'] = ftypes[type(temp.default)]

        parser.add_argument('--' + temp.name, **opts)


class MyHelpFormatter(argparse.HelpFormatter):
    """
    `HelpFormatter` with custom graphical parameters.
    """

    def __init__(self, prog, **kwargs):
        kwargs['max_help_position'] = 40
        kwargs['indent_increment'] = 4
        kwargs['width'] = 100
        argparse.HelpFormatter.__init__(self, prog, **kwargs)


class MyArgumentParser(argparse.ArgumentParser):
    """
    `ArgumentParser` with custom file parsing.
    """

    def convert_arg_line_to_args(self, arg_line):
        """
        Parse the file skipping the comments and using
        any whitespace as an argument delimiter.
        """
        if arg_line.startswith('#'):
            return
        for arg in arg_line.split():
            if not arg.strip():
                continue
            yield arg


if __name__=="__main__":

    parser = MyArgumentParser(description=global_desc,
                  formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest='command', help='Select a command')

    # run simulation parser
    run_parser = subparsers.add_parser('run', help='Run a simulation',
                                       usage='%(prog)s {}'.format(run_opts),
                                       fromfile_prefix_chars='@',
                                       formatter_class=MyHelpFormatter)
    run_parser.add_argument('--profile', action='store_true',
        help='Run a time profiler instead of the normal simulation')
    run_parser.add_argument('--debug', action='store_true',
        help='Set the logger level to DEBUG')
    run_parser.add_argument('workload', help='The workload file')

    sim_group = run_parser.add_argument_group('General simulation parameters')
    arguments_from_templates(sim_group, settings.sim_templates)

    alg_group = run_parser.add_argument_group('Algorithm specific parameters')
    arguments_from_templates(alg_group, settings.alg_templates)

    part_group = run_parser.add_argument_group('Part selection parameters')
    arguments_from_templates(part_group, settings.part_templates)

    # config parser
    config_parser = subparsers.add_parser('config', help='Create configuration')

    action_group = config_parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('--generate', action='store_true',
        help='Generate a new configuration template.')
    action_group.add_argument('--recreate', metavar='SIM FILE',
        help='Recreate the configuration from a simulation')

    # stats parser
    stats_parser = subparsers.add_parser('stats', help='Display various statistics')
    stats_parser.add_argument('workload', help='The workload file')

    args = vars(parser.parse_args())

    if args['command'] == 'run':
        # get the config file name
        configs = [arg[1:] for arg in sys.argv if arg[0] == '@']
        used_conf = (configs and configs[-1]) or 'no_conf'
        # extract flags
        PROFILE_FLAG = args.pop('profile')
        debug = args.pop('debug')
        # enable logger module
        setup_logging(PROFILE_FLAG and debug, used_conf)
        # and go!
        run(args['workload'], args)
    elif args['command'] == 'config':
        config(args)
    elif args['command'] == 'stats':
        setup_logging(False, 'stats')
        display_stats(args['workload'], args)
    else:
        print "Hmm...", args['command']
