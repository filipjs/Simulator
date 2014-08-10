# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

"""
Shares:
    Assign the number of CPU shares to each user.
    One share translates to (100.0 / shares_sum) percent of all the CPUs.

Customizing:
    Create a new subclass of `BaseShare` and override the required methods.
    To add new settings to use in your subclass see :mod: `settings` documentation.
"""


class BaseShare(object):
    """
    Shares base class. Subclasses are required to override:

    1) _get_shares

    You can access the `Settings` using `self._settings`.
    """

    __metaclass__ = ABCMeta

    def __init__(self, settings):
        """
        Init the class with a `Settings` instance.
        """
        self._settings = settings

    def user_share(self, user):
        """
        Public wrapper method.
        Run and check the correctness of `_get_shares`.
        """
        share = self._get_shares(user)
        assert user.shares is None, 'share value already set'
        assert isinstance(share, int), 'invalid share type'
        assert share > 0, 'invalid share value'
        return share

    @abstractmethod
    def _get_shares(self, user):
        """
        Get the number of shares to assign to the user.

        Note:
          **DO NOT** set the `user.shares` yourself.
        """
        raise NotImplemented


class EqualShare(BaseShare):
    """
    Each user gets an equal number of shares.
    """

    def _get_shares(self, user):
        return 1


class CustomShare(BaseShare):
    """
    Read the share values from a file.

    Uses `_settings.share_file` to read from.
    Each line should consist of a pair "userID share".
    Users not specified in the file get a single share.
    """

    def __init__(self, *args):
        """
        Read the specified file only once in init.
        """
        BaseShare.__init__(self, *args)

        self._shares = {}
        with open(self._settings.share_file) as f:
            for line in f:
                if line:
                    uid, share = map(int, line.split())
                    self._shares[uid] = share

    def _get_shares(self, user):
        """
        Return the value read from the file or a default value.
        """
        return self._shares.get(user.ID, 1)
