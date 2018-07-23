# Author: Rishabh Sharma <rishabh.sharma.gunner@gmail.com>
# This module was developed under funding by
# Google Summer of Code 2014

from __future__ import absolute_import, division, print_function
import datetime

from sunpy.time import TimeRange
from sunpy.util.scraper import Scraper

from ..client import GenericClient


__all__ = ['EVEClient', 'EVELeve2BClient', 'EVELeve2Client', 'EVELeve3Client', 'EVELevel3MergedClient']

BASEURL = ('http://lasp.colorado.edu/eve/data_access/evewebdata/quicklook/'
           'L0CS/SpWx/%Y/%Y%m%d_EVE_L0CS_DIODES_1m.txt')


# TODO: Deprecate this name?
# TODO: Update to use the eve base class...
class EVEClient(GenericClient):
    """
    This EVEClient is for the Level 0C data
    from http://lasp.colorado.edu/home/eve/data/data-access/.

    To use this client you must request Level 0 data.

    Examples
    --------

    >>> from sunpy.net import Fido, attrs as a
    >>> results = Fido.search(a.Time("2016/1/1", "2016/1/2"),
    ...                       a.Instrument('EVE'), a.Level(0))  #doctest: +REMOTE_DATA
    >>> results  #doctest: +REMOTE_DATA +ELLIPSIS
    <sunpy.net.fido_factory.UnifiedResponse object at ...>
    Results from 1 Provider:
    <BLANKLINE>
    2 Results from the EVEClient:
         Start Time           End Time      Source Instrument Wavelength
           str19               str19         str3     str3       str3
    ------------------- ------------------- ------ ---------- ----------
    2016-01-01 00:00:00 2016-01-02 00:00:00    SDO        eve        nan
    2016-01-02 00:00:00 2016-01-03 00:00:00    SDO        eve        nan
    <BLANKLINE>
    <BLANKLINE>

    """

    def _get_url_for_timerange(self, timerange, **kwargs):
        """
        Return list of URLS corresponding to value of input timerange.

        Parameters
        ----------
        timerange: `sunpy.time.TimeRange`
            time range for which data is to be downloaded.

        Returns
        -------
        urls : list
            list of URLs corresponding to the requested time range

        """
        # If start of time range is before 00:00, converted to such, so
        # files of the requested time ranger are included.
        # This is done because the archive contains daily files.
        if timerange.start.time() != datetime.time(0, 0):
            timerange = TimeRange('{:%Y-%m-%d}'.format(timerange.start), timerange.end)
        eve = Scraper(BASEURL)
        return eve.filelist(timerange)

    def _get_time_for_url(self, urls):
        eve = Scraper(BASEURL)
        times = list()
        for url in urls:
            t0 = eve._extractDateURL(url)
            # hard coded full day as that's the normal.
            times.append(TimeRange(t0, t0 + datetime.timedelta(days=1)))
        return times

    def _makeimap(self):
        """
        Helper Function: used to hold information about source.
        """
        self.map_['source'] = 'SDO'
        self.map_['provider'] = 'LASP'
        self.map_['instrument'] = 'eve'
        self.map_['physobs'] = 'irradiance'
        self.map_['level'] = 'L0CS'

    @classmethod
    def _can_handle_query(cls, *query):
        """
        Answers whether client can service the query.

        Parameters
        ----------
        query : list of query objects

        Returns
        -------
        boolean
            answer as to whether client can service the query
        """
        chk_var = 0
        for x in query:
            if x.__class__.__name__ == 'Instrument' and x.value.lower() == 'eve':
                chk_var += 1

            elif x.__class__.__name__ == 'Level' and x.value == 0:
                chk_var += 1

        if chk_var == 2:
            return True
        return False


class _BaseEVEClient(GenericClient):
    """
    This EVEClient is for the level2+ data.

    """

    _HOST_URL = 'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/'
    _PRODUCT_URL = NotImplemented
    _PRODUCT_NAME = 'S'
    _VERSION = 6
    _REVISION = 2

    def __init__(self, *args, **kwargs):
        super(_BaseEVEClient, self).__init__(*args, **kwargs)
        if self._PRODUCT_URL is NotImplemented:
            raise NotImplementedError
        self._scrapper = Scraper(self._HOST_URL + self._PRODUCT_URL, product=self._PRODUCT_NAME,
                                 version=self._VERSION, revision=self._REVISION)

    def _makeimap(self):
        """
        Helper Function: used to hold information about source.
        """
        self.map_['source'] = 'SDO'
        self.map_['provider'] = 'LASP'
        self.map_['instrument'] = 'eve'
        self.map_['physobs'] = 'irradiance'
        # TODO: Add `wavelength`?


class EVELevel2BClient(_BaseEVEClient):
    _PRODUCT_URL = 'level2b/%Y/%j/EV{product}_L2B_%Y%j_{version:03d}_{revision:02d}.fit.gz'

    def _makeimap(self):
        super(EVELevel2BClient, self)._makeimap()
        self.map_['level'] = 'L2B'

    def _get_url_for_timerange(self, timerange, **kwargs):
        """
        Return list of URLS corresponding to value of input timerange.

        Parameters
        ----------
        timerange: `sunpy.time.TimeRange`
            time range for which data is to be downloaded.

        Returns
        -------
        urls : list
            list of URLs corresponding to the requested time range

        """
        # If start of time range is before 00:00, converted to such, so
        # files of the requested time ranger are included.
        # This is done because the archive contains daily files.
        if timerange.start.time() != datetime.time(0, 0):
            timerange = TimeRange('{:%Y-%m-%d}'.format(timerange.start), timerange.end)
        return self._scrapper.filelist(timerange)

    def _get_time_for_url(self, urls):
        times = list()
        for url in urls:
            t0 = self._scrapper._extractDateURL(url)
            # hard coded full day as that's the normal.
            # FIXME: This doesn't match the get/query logic (cant cycle)...
            times.append(TimeRange(t0, t0 + datetime.timedelta(days=1)))  # TODO: Hmmm
        return times

    @classmethod
    def _can_handle_query(cls, *query):
        """
        Answers whether client can service the query.

        Parameters
        ----------
        query : list of query objects

        Returns
        -------
        boolean
            answer as to whether client can service the query
        """
        chk_var = set()
        for x in query:
            name = x.__class__.__name__
            if name == 'Instrument' and x.value.lower() == 'eve':
                chk_var.add(name)

            elif name == 'Level' and isinstance(x.value, str) and x.value.lower() == '2b':
                chk_var.add(name)

            # No level-2b data is available before this time (preceded by L2).
            elif name == 'Time' and x.end >= datetime.datetime(2018, 4, 20):
                chk_var.add(name)

        # TODO: Why not check Source=SDO? Or at least allow it...
        return len(chk_var) == 3


class EVELevel2Client(_BaseEVEClient):
    _PRODUCT_URL = 'level2/%Y/%j/EV{product}_L2_%Y%j_%H_{version:03d}_{revision:02d}.fit.gz'

    def _makeimap(self):
        super(EVELevel2Client, self)._makeimap()
        self.map_['level'] = 'L2'

    def _get_url_for_timerange(self, timerange, **kwargs):
        """
        Return list of URLS corresponding to value of input timerange.

        Parameters
        ----------
        timerange: `sunpy.time.TimeRange`
            time range for which data is to be downloaded.

        Returns
        -------
        urls : list
            list of URLs corresponding to the requested time range

        """
        # Hourly data.
        if timerange.start.time() != datetime.time(timerange.start.hour, 0):
            timerange = TimeRange('{:%Y-%m-%d %H:00}'.format(timerange.start), timerange.end)
        return self._scrapper.filelist(timerange)

    def _get_time_for_url(self, urls):
        times = list()
        for url in urls:
            t0 = self._scrapper._extractDateURL(url)
            # hard coded one hour timespan.
            times.append(TimeRange(t0, t0 + datetime.timedelta(hours=1)))
        return times

    @classmethod
    def _can_handle_query(cls, *query):
        """
        Answers whether client can service the query.

        Parameters
        ----------
        query : list of query objects

        Returns
        -------
        boolean
            answer as to whether client can service the query
        """
        chk_var = set()
        for x in query:
            name = x.__class__.__name__
            if name == 'Instrument' and x.value.lower() == 'eve':
                chk_var.add(name)

            elif name == 'Level' and ((isinstance(x.value, int) and x.value == 2)
                                      or (isinstance(x.value, str) and x.value == '2')):
                chk_var.add(name)

            # No level-2 data is available after this time (replaced by L2B).
            elif name == 'Time' and x.start < datetime.datetime(2018, 4, 28):
                chk_var.add(name)

        return len(chk_var) == 3


class EVELevel3Client(_BaseEVEClient):
    _PRODUCT_URL = 'level3/%Y/EVE_L3_%Y%j_{version:03d}_{revision:02d}.fit'

    def _makeimap(self):
        super(EVELevel3Client, self)._makeimap()
        self.map_['level'] = 'L3'

    def _get_url_for_timerange(self, timerange, **kwargs):
        """
        Return list of URLS corresponding to value of input timerange.

        Parameters
        ----------
        timerange: `sunpy.time.TimeRange`
            time range for which data is to be downloaded.

        Returns
        -------
        urls : list
            list of URLs corresponding to the requested time range

        """
        # If start of time range is before 00:00, converted to such, so
        # files of the requested time ranger are included.
        # This is done because the archive contains daily files.
        if timerange.start.time() != datetime.time(0, 0):
            timerange = TimeRange('{:%Y-%m-%d}'.format(timerange.start), timerange.end)
        return self._scrapper.filelist(timerange)

    def _get_time_for_url(self, urls):
        times = list()
        for url in urls:
            t0 = self._scrapper._extractDateURL(url)
            # hard coded full day as that's the normal.
            times.append(TimeRange(t0, t0 + datetime.timedelta(days=1)))
        return times

    @classmethod
    def _can_handle_query(cls, *query):
        """
        Answers whether client can service the query.

        Parameters
        ----------
        query : list of query objects

        Returns
        -------
        boolean
            answer as to whether client can service the query
        """
        chk_var = set()
        for x in query:
            name = x.__class__.__name__
            if name == 'Instrument' and x.value.lower() == 'eve':
                chk_var.add(name)

            elif name == 'Level' and ((isinstance(x.value, int) and x.value == 3)
                                      or (isinstance(x.value, str) and x.value == '3')):
                chk_var.add(name)

        return len(chk_var) == 2


class EVELevel3MergedClient(_BaseEVEClient):
    _PRODUCT_URL = 'merged/latest_EVE_L3_merged.fit'
    # TODO: What are the l1a and 1nm sub-types?

    def _makeimap(self):
        super(EVELevel3MergedClient, self)._makeimap()
        self.map_['level'] = 'L3Merged'

    def _get_url_for_timerange(self, timerange, **kwargs):
        """
        Return list of URLS corresponding to value of input timerange.

        Parameters
        ----------
        timerange: `sunpy.time.TimeRange`
            time range for which data is to be downloaded.

        Returns
        -------
        urls : list
            list of URLs corresponding to the requested time range

        """
        # TODO: This is a full-mission file. How to handle...
        # Since there is no date in the filename the _scrapper will default to
        # 1900. In order to match that we must pass it in.
        full_timerange = TimeRange('1900/01/01', datetime.date.today())
        return self._scrapper.filelist(full_timerange)

    def _get_time_for_url(self, urls):
        times = list()
        for url in urls:
            # TODO: This is a full-mission file. How to handle...
            # First L2 date is '2010/04/20', but things appear to default to 1900.
            times.append(TimeRange('1900/01/01', datetime.date.today()))
        return times

    @classmethod
    def _can_handle_query(cls, *query):
        """
        Answers whether client can service the query.

        Parameters
        ----------
        query : list of query objects

        Returns
        -------
        boolean
            answer as to whether client can service the query
        """
        chk_var = set()
        for x in query:
            name = x.__class__.__name__
            if name == 'Instrument' and x.value.lower() == 'eve':
                chk_var.add(name)

            elif name == 'Level' and isinstance(x.value, str) and x.value.lower() in ['3m', '3merged']:
                chk_var.add(name)

        return len(chk_var) == 2
