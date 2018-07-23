# -*- coding: utf-8 -*-
"""EVE TimeSeries subclass definitions."""
from __future__ import absolute_import

import codecs
import re
import os
from os.path import basename
from datetime import datetime
from collections import OrderedDict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.io.parsers import read_csv
from astropy import units as u
from astropy.time import Time

from sunpy.timeseries.timeseriesbase import GenericTimeSeries
from sunpy.util.metadata import MetaDict
from sunpy.io import fits


__all__ = ['EVESpWxTimeSeries', 'EVEScanTimeSeries']


# FIXME: Do stuff here.


class EVESpWxTimeSeries(GenericTimeSeries):
    """
    SDO EVE LightCurve for level 0CS data.

    The Extreme Ultraviolet Variability Experiment (EVE) is an instrument on board
    the Solar Dynamics Observatory (SDO). The EVE instrument is designed to
    measure the solar extreme ultraviolet (EUV) irradiance. The EUV radiation
    includes the 0.1-105 nm range, which provides the majority of the energy
    for heating Earth’s thermosphere and creating Earth’s ionosphere (charged plasma).

    EVE includes several irradiance instruments: The Multiple EUV Grating
    Spectrographs (MEGS)-A is a grazing- incidence spectrograph that measures
    the solar EUV irradiance in the 5 to 37 nm range with 0.1-nm resolution,
    and the MEGS-B is a normal-incidence, dual-pass spectrograph that measures
    the solar EUV irradiance in the 35 to 105 nm range with 0.1-nm resolution.

    Level 0CS data is primarily used for space weather. It is provided near
    real-time and is crudely calibrated 1-minute averaged broadband irradiances
    from ESP and MEGS-P broadband. For other levels of EVE data, use
    `~sunpy.net.Fido`, with `sunpy.net.attrs.Instrument('eve')`.

    Data is available starting on 2010/03/01.

    Examples
    --------
    >>> import sunpy.timeseries
    >>> import sunpy.data.sample  # doctest: +REMOTE_DATA
    >>> eve = sunpy.timeseries.TimeSeries(sunpy.data.sample.EVE_TIMESERIES, source='EVE')  # doctest: +REMOTE_DATA
    >>> # TODO: The above is invalid?
    >>> eve = sunpy.timeseries.TimeSeries("http://lasp.colorado.edu/eve/data_access/quicklook/quicklook_data/L0CS/LATEST_EVE_L0CS_DIODES_1m.txt", source='EVE')  # doctest: +REMOTE_DATA
    >>> eve.peek(subplots=True)    # doctest: +SKIP

    References
    ----------
    * `SDO Mission Homepage <https://sdo.gsfc.nasa.gov/>`_
    * `EVE Homepage <http://lasp.colorado.edu/home/eve/>`_
    * `Level 0CS Definition <http://lasp.colorado.edu/home/eve/data/>`_
    * `EVE Data Acess <http://lasp.colorado.edu/home/eve/data/data-access/>`_
    * `Instrument Paper <https://doi.org/10.1007/s11207-009-9487-6>`_
    """
    # Class attribute used to specify the source class of the TimeSeries.
    _source = 'eve'

    def peek(self, column=None, **kwargs):
        """Plots the time series in a new figure. An example is shown below.

        .. plot::

            import sunpy.timeseries
            from sunpy.data.sample import EVE_TIMESERIES
            eve = sunpy.timeseries.TimeSeries(EVE_TIMESERIES, source='eve')
            eve.peek(subplots=True)

        Parameters
        ----------
        column : `str`
            The column to display. If None displays all.

        **kwargs : `dict`
            Any additional plot arguments that should be used
            when plotting.
        """
        # Check we have a timeseries valid for plotting
        self._validate_data_for_ploting()

        figure = plt.figure()
        # Choose title if none was specified
        if "title" not in kwargs and column is None:
            if len(self.data.columns) > 1:
                kwargs['title'] = 'EVE (1 minute data)'
            else:
                if self._filename is not None:
                    base = self._filename.replace('_', ' ')
                    kwargs['title'] = os.path.splitext(base)[0]
                else:
                    kwargs['title'] = 'EVE Averages'

        if column is None:
            self.plot(**kwargs)
        else:
            data = self.data[column]
            if "title" not in kwargs:
                kwargs['title'] = 'EVE ' + column.replace('_', ' ')
            data.plot(**kwargs)
        figure.show()

    @classmethod
    def _parse_file(cls, filepath):
        """Parses an EVE CSV file."""
        cls._filename = basename(filepath)
        with codecs.open(filepath, mode='rb', encoding='ascii') as fp:
            # Determine type of EVE CSV file and parse
            line1 = fp.readline()
            fp.seek(0)

            if line1.startswith("Date"):
                return cls._parse_average_csv(fp)
            elif line1.startswith(";"):
                return cls._parse_level_0cs(fp)

    @staticmethod
    def _parse_average_csv(fp):
        """Parses an EVE Averages file."""
        print('\nin _parse_average_csv()')
        # TODO: Does this even work? Incorrect number of returns and wrong order?
        return "", read_csv(fp, sep=",", index_col=0, parse_dates=True)

    @staticmethod
    def _parse_level_0cs(fp):
        """Parses and EVE Level 0CS file."""
        is_missing_data = False  # boolean to check for missing data
        missing_data_val = np.nan
        header = []
        fields = []
        line = fp.readline()
        # Read header at top of file
        while line.startswith(";"):
            header.append(line)
            if '; Missing data:' in line:
                is_missing_data = True
                missing_data_val = line.split(':')[1].strip()

            line = fp.readline()

        meta = MetaDict()
        for hline in header:
            if hline == '; Format:\n' or hline == '; Column descriptions:\n':
                continue
            elif ('Created' in hline) or ('Source' in hline):
                meta[hline.split(':',
                                 1)[0].replace(';',
                                               ' ').strip()] = hline.split(':', 1)[1].strip()
            elif ':' in hline:
                meta[hline.split(':')[0].replace(';', ' ').strip()] = hline.split(':')[1].strip()

        fieldnames_start = False
        for hline in header:
            if hline.startswith("; Format:"):
                fieldnames_start = False
            if fieldnames_start:
                fields.append(hline.split(":")[0].replace(';', ' ').strip())
            if hline.startswith("; Column descriptions:"):
                fieldnames_start = True

        # Next line is YYYY DOY MM DD
        date_parts = line.split(" ")

        year = int(date_parts[0])
        month = int(date_parts[2])
        day = int(date_parts[3])

        # function to parse date column (HHMM)
        parser = lambda x: datetime(year, month, day, int(x[0:2]), int(x[2:4]))

        data = read_csv(fp, sep="\s*", names=fields, index_col=0, date_parser=parser, header=None, engine='python')
        if is_missing_data:  # If missing data specified in header
            data[data == float(missing_data_val)] = np.nan

        # Add the units data
        units = OrderedDict([('XRS-B proxy', u.W/u.m**2),
                             ('XRS-A proxy', u.W/u.m**2),
                             ('SEM proxy', u.W/u.m**2),
                             ('0.1-7ESPquad', u.W/u.m**2),
                             ('17.1ESP', u.W/u.m**2),
                             ('25.7ESP', u.W/u.m**2),
                             ('30.4ESP', u.W/u.m**2),
                             ('36.6ESP', u.W/u.m**2),
                             ('darkESP', u.ct),
                             ('121.6MEGS-P', u.W/u.m**2),
                             ('darkMEGS-P', u.ct),
                             ('q0ESP', u.dimensionless_unscaled),
                             ('q1ESP', u.dimensionless_unscaled),
                             ('q2ESP', u.dimensionless_unscaled),
                             ('q3ESP', u.dimensionless_unscaled),
                             ('CMLat', u.deg),
                             ('CMLon', u.deg)])
        # Todo: check units used.
        return data, meta, units

    @classmethod
    def is_datasource_for(cls, **kwargs):
        """Determines if header corresponds to an EVE image"""
        if kwargs.get('source', ''):
            return kwargs.get('source', '').lower() == cls._source


class EVEScanTimeSeries(GenericTimeSeries):
    """
    SDO EVE Scan Time Series for level 2 and 2b data.

    The Extreme Ultraviolet Variability Experiment (EVE) is an instrument on board
    the Solar Dynamics Observatory (SDO). The EVE instrument is designed to
    measure the solar extreme ultraviolet (EUV) irradiance. The EUV radiation
    includes the 0.1-105 nm range, which provides the majority of the energy
    for heating Earth’s thermosphere and creating Earth’s ionosphere (charged plasma).

    EVE includes several irradiance instruments: The Multiple EUV Grating
    Spectrographs (MEGS)-A is a grazing- incidence spectrograph that measures
    the solar EUV irradiance in the 5 to 37 nm range with 0.1-nm resolution,
    and the MEGS-B is a normal-incidence, dual-pass spectrograph that measures
    the solar EUV irradiance in the 35 to 105 nm range with 0.1-nm resolution.

    Examples
    --------
    >>> import sunpy.timeseries
    >>> eve = sunpy.timeseries.TimeSeries('http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level2b/2018/120/EVS_L2B_2018120_006_02.fit.gz', source='eve_l2b')  # doctest: +REMOTE_DATA
    >>> eve.peek()    # doctest: +SKIP

    References
    ----------
    * `SDO Mission Homepage <https://sdo.gsfc.nasa.gov/>`_
    * `EVE Homepage <http://lasp.colorado.edu/home/eve/>`_
    * `Level 0CS Definition <http://lasp.colorado.edu/home/eve/data/>`_
    * `EVE Data Acess <http://lasp.colorado.edu/home/eve/data/data-access/>`_
    * `Instrument Paper <https://doi.org/10.1007/s11207-009-9487-6>`_
    """
    # Class attribute used to specify the source class of the TimeSeries.
    _source = 'eve_l2b'  # TODO: Hmmm

    def peek(self, column=None, **kwargs):
        """Plots the time series in a new figure. An example is shown below.

        .. plot::

            import sunpy.timeseries
            from sunpy.data.sample import EVE_TIMESERIES
            eve = sunpy.timeseries.TimeSeries(EVE_TIMESERIES, source='eve')
            eve.peek(subplots=True)

        Parameters
        ----------
        column : `str`
            The column to display. If None displays all.

        **kwargs : `dict`
            Any additional plot arguments that should be used
            when plotting.
        """
        # Check we have a timeseries valid for plotting
        self._validate_data_for_ploting()
        # TODO: Look at GOES (or others) for plotting improvements.

        figure = plt.figure()
        # Choose title if none was specified
        if "title" not in kwargs and column is None:
            if len(self.data.columns) > 1:
                kwargs['title'] = 'EVE (1 minute data)'
            else:
                if self._filename is not None:
                    base = self._filename.replace('_', ' ')
                    kwargs['title'] = os.path.splitext(base)[0]
                else:
                    kwargs['title'] = 'EVE Averages'

        if column is None:
            self.data.mean(axis=1).plot(**kwargs)
            # arr = self.data.dropna(axis=0, how='all').dropna(axis=1, how='all').values
            # plt.imshow(arr)
        else:
            data = self.data[column]
            if "title" not in kwargs:
                kwargs['title'] = 'EVE ({:0.2f} nm)'.format(column)
            data.plot(**kwargs)
            # plt.plot(self.data.index, self.data[self.data.columns[1500]].values)
            # plt.gca().set_yscale('log')
        figure.show()

    @classmethod
    def _parse_file(cls, filepath):
        """Parses an EVE FITS file."""
        cls._filename = basename(filepath)
        fdata = fits.read(filepath)  # FIXME: Return is a list...

        # TODO: Support multiple types (lines vs. scan and/or level)?
        # if 'EVS_2B' in cls._filename:
        #     return cls._parse_level_2b_fits(fdata)
        # raise NotImplementedError
        return cls._parse_level_2b_fits(fdata)

    @staticmethod
    def _parse_level_2b_fits(fdata):
        """Parses and EVE Level-2B file."""
        missing_data_val = -1  # TODO: Correct?

        # TODO: Metadata...
        meta = MetaDict()

        # Convert from TAI seconds using the GPS-TAI epoch difference.
        times = Time(fdata[3].data['tai'] - 694656019, format='gps', scale='utc', precision=6)

        # Build the dataframe...
        # TODO: What about everything else? E.g., flags and count rates.
        indx = pd.DatetimeIndex(times.to_datetime(), name='UTC')
        cols = fdata[1].data['wavelength']  # .astype(str)  # TODO: Hmmm...

        irrad = fdata[3].data['irradiance']
        if not irrad.dtype.isnative:
            irrad = irrad.astype('f4')

        df = pd.DataFrame(irrad, index=indx, columns=cols)
        df[df == missing_data_val] = np.nan

        # TODO: Add in the QFs?

        # Irradiance is in watts per meter^2 nanometer.
        irrad_units = u.W * u.m ** -2 * u.nm ** -1

        # TODO: Add the units data
        units = OrderedDict([(c, irrad_units) for c in df.columns])

        return df, meta, units

    @classmethod
    def is_datasource_for(cls, **kwargs):
        """Determines if header corresponds to an EVE image"""
        if kwargs.get('source', ''):
            # return kwargs.get('source', '').lower() == cls._source
            return re.match(r'^eve[_ ]l2b?$', kwargs['source'], re.I)
