# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta, abstractmethod
from util import delta


"""
#TODO OPIS
"""


class _NodeSpace(object):
    """

    """

    def __init__(self, begin, end, avail, reserved, next, job_ends):
        self.begin = begin
        self.end = end
        self.avail = avail
        self.reserved = reserved
        self.next = next
        self.job_ends = job_ends
        self.rsrv_starts = 0
        self.update()

    def update(self):
        self.length = self.end - self.begin

    def __repr__(self):
        s = '[{}, {}] last {} first {}\n\tavail {}\n\trsrvd {}'
        return s.format(delta(self.begin), delta(self.end),
            self.job_ends, self.rsrv_starts,
            self.avail, self.reserved)


class BaseManager(object):
    """

    """

    #__metaclass__ = ABCMeta
    #TODO STWORZYC META DLA TEJ KLASY?? (wtedy mozna inna strategie)
    #TODO I DOPISAC ZE JESLI SIE CHCE NODY TO TRZEBA PROBOWAC REIMPLEMENTOWAC KOD Z #TAGGED "LAST NODE SUPPORT"

    def __init__(self, cpus, settings):
        self._settings = settings
        self._space_list = _NodeSpace(
                    0,
                    float('inf'),
                    cpus,
                    0,
                    None,
                    0,
                   )
        self._reservations = 0
        self._cpu_limit = cpus
        self._debug = logging.getLogger().isEnabledFor(logging.DEBUG)

    def _dump_space(self, intro, *args):
        """
        Print the current state of node spaces.
        """
        logging.debug(intro, *args)
        it = self._space_list
        while it is not None:
            logging.debug('%s', it)
            it = it.next

    def runnable(self, job):
        """
        TODO
        """
        return job.proc <= self._cpu_limit

    def start_session(self, now):
        """
        Prepare the manager for the upcoming scheduling or backfilling pass.

        Args:
          now: session start time

        """
        self._window = now + self._settings.bf_window
        self._space_list.begin = now
        self._space_list.update()
        assert self._space_list.length > 0, 'some finished jobs not removed'
        assert not self._reservations, 'reservations not removed'

    def _allocate_resources(self, job, first, last, reservation):
        """
        Allocate resources to the job and update the space list.

        Args:
          job: job in question.
          first: starting space for the job.
          last: last space for the job.
          reservation: is this a reservation flag.

        """
        # The job spans the spaces from `first` to `last` (inclusive).
        # However we might have to split the last one.
        if (last.end - first.begin) > job.time_limit:
            # Divide the `last` space appropriately and
            # create a new space to occupy the gap.
            new_space = _NodeSpace(
                    first.begin + job.time_limit,
                    last.end,
                    last.avail,
                    last.reserved,
                    last.next,
                    last.job_ends,
                    )
            # new space is following `last`
            last.end = new_space.begin
            last.next = new_space
            last.job_ends = 0
            last.update()

        if not reservation:
            last.job_ends += 1
        else:
            first.rsrv_starts += 1
            self._reservations += 1

        # remove the used up resources
        it = first
        while True:
            it.avail -= job.proc
            if reservation:
                it.reserved += job.proc
            if it == last:
                break
            it = it.next

        if self._debug:
            self._dump_space('Added resources %s', job)

    def try_schedule(self, job):
        """
        TODO
        """
        assert not self._reservations, 'reservations are present'
        # In a space list without reservations, each space is
        # guaranteed to have more resources available than
        # the spaces before them.
        # This means we only have to check the first one
        # to see if the job can be executed.
        first = self._space_list
        if job.proc > first.avail:
            return False

        total_time = 0
        it = first

        while True:
            total_time += it.length
            if total_time >= job.time_limit:
                last = it
                break
            it = it.next

        self._allocate_resources(job, first, last, False)
        return True

    def try_backfill(self, job):
        """
        TODO
        Make a reservation for the job.
        Return if the job can be executed immediately.
        """
        total_time = 0
        it = first = self._space_list

        avail = it.avail
        must_check = True

        while True:
            if must_check:
                avail = min(avail, it.avail)

            if not must_check or job.proc <= avail:
                total_time += it.length
                if total_time >= job.time_limit:
                    last = it
                    break
                # next space #TODO OPIS (dlaczego tak sie sprawdza must_check??)
                it = it.next
                must_check = it.rsrv_starts > 0
            else:
                total_time = 0
                #TODO OPIS
                it = first = first.next
                avail = it.avail
                must_check = True
                # Maybe we can stop, if the potential start is already
                # outside of the backfilling window.
                if first.begin > self._window:
                    return False

        # check if the job can be executed now
        can_run = (first == self._space_list)

        self._allocate_resources(job, first, last, not can_run)
        return can_run

    def end_session(self):
        """
        #TODO
        Clear the created reservations.
        """
        before = self._reservations
        prev, it = None, self._space_list

        while it.next is not None:
            # update the count
            self._reservations -= it.rsrv_starts
            it.rsrv_starts = 0
            # now clean up
            if not it.job_ends:
                # we can safely remove this space
                remove, it = it, it.next
                it.begin = remove.begin
                it.update()
                #TODO DODAC JAKIES ASSERTY??
                #TODO WYGLADA NA TO ZE REMOVE.AVAIL + REMOVE.RESERVED == IT.AVAIL??
                prev.next = it
                remove.next = None
            else:
                it.avail += it.reserved
                it.reserved = 0
                prev, it = it, it.next

        assert not self._reservations, 'reservations not cleared'
        if self._debug:
            self._dump_space('Cleared %s reservations', before)

    def job_ended(self, job):
        """
        #TODO
        Free the resources taken by the job.
        """
        assert not self._reservations, 'reservations are present'
        self._space_list.begin = job.end_time
        self._space_list.update()
        assert self._space_list.length >= 0, 'some finished jobs not removed'
        #assert job.alloc is not None, 'missing job resources'
        #TODO TEN ASSERT TERAZ POWINIEN SPRAWDZAC CZY PRACA JEST JUZ FINISHED (STARTED?)

        last_space_end = job.start_time + job.time_limit
        it = self._space_list

        while it.end < last_space_end:
            it.avail += job.proc
            it = it.next

        assert it.end == last_space_end, 'missing job last space'
        assert it.job_ends > 0, 'invalid last space'

        if it.job_ends == 1:
            # we can safely merge this space with the next one
            remove = it.next
            it.end = remove.end
            it.avail = remove.avail
            #TODO TUTAJ TEZ ASSERT TYPE IT.AVAIL + JOB>PROC == REMOVE.AVAIL??
            it.reserved = remove.reserved #TODO ASSERT RESERVVED == 0??
            it.job_ends = remove.job_ends
            it.update()
            # move 'pointers' as the last step
            it.next = remove.next
            remove.next = None
        else:
            it.avail += job.proc
            it.job_ends -= 1

        if self._debug:
            self._dump_space('Removed resources %s', job)
