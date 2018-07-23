import datetime
import pytest

from sunpy.time.timerange import TimeRange
from sunpy.net.vso import VSOClient
from sunpy.net.vso.attrs import Time, Instrument, Source, Level
from sunpy.net.dataretriever.client import QueryResponse
import sunpy.net.dataretriever.sources.eve as eve
from sunpy.net.fido_factory import UnifiedResponse
from sunpy.net import Fido
from sunpy.net import attrs as a

from hypothesis import given, settings
from hypothesis.strategies import datetimes
from sunpy.net.tests.strategies import time_attr

LCClient = eve.EVEClient()
L2bClient = eve.EVELevel2BClient()
L2Client = eve.EVELevel2Client()
L3Client = eve.EVELevel3Client()
L3mClient = eve.EVELevel3MergedClient()


@pytest.mark.remote_data
@pytest.mark.parametrize("client,timerange,url_start,url_end", [
    (LCClient, TimeRange('2012/4/21', '2012/4/21'),
     'http://lasp.colorado.edu/eve/data_access/evewebdata/quicklook/L0CS/SpWx/2012/20120421_EVE_L0CS_DIODES_1m.txt',
     'http://lasp.colorado.edu/eve/data_access/evewebdata/quicklook/L0CS/SpWx/2012/20120421_EVE_L0CS_DIODES_1m.txt'
     ),
    (LCClient, TimeRange('2012/5/5', '2012/5/6'),
     'http://lasp.colorado.edu/eve/data_access/evewebdata/quicklook/L0CS/SpWx/2012/20120505_EVE_L0CS_DIODES_1m.txt',
     'http://lasp.colorado.edu/eve/data_access/evewebdata/quicklook/L0CS/SpWx/2012/20120506_EVE_L0CS_DIODES_1m.txt',
     ),
    (LCClient, TimeRange('2012/7/7', '2012/7/14'),
     'http://lasp.colorado.edu/eve/data_access/evewebdata/quicklook/L0CS/SpWx/2012/20120707_EVE_L0CS_DIODES_1m.txt',
     'http://lasp.colorado.edu/eve/data_access/evewebdata/quicklook/L0CS/SpWx/2012/20120714_EVE_L0CS_DIODES_1m.txt',
     ),
    (L2bClient, TimeRange('2018/4/20', '2018/4/20'),
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level2b/2018/110/EVS_L2B_2018110_006_02.fit.gz',
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level2b/2018/110/EVS_L2B_2018110_006_02.fit.gz'
     ),
    (L2bClient, TimeRange('2018/05/30', '2018/06/01'),
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level2b/2018/150/EVS_L2B_2018150_006_02.fit.gz',
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level2b/2018/152/EVS_L2B_2018152_006_02.fit.gz'
     ),
    (L2Client, TimeRange('2018/01/01 14:00', '2018/01/01 14:00'),
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level2/2018/001/EVS_L2_2018001_14_006_02.fit.gz',
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level2/2018/001/EVS_L2_2018001_14_006_02.fit.gz'
     ),
    (L2Client, TimeRange('2018/01/01 14:00', '2018/01/01 15:00'),
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level2/2018/001/EVS_L2_2018001_14_006_02.fit.gz',
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level2/2018/001/EVS_L2_2018001_15_006_02.fit.gz'
     ),
    (L3Client, TimeRange('2018/04/20', '2018/04/20'),
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level3/2018/EVE_L3_2018110_006_02.fit',
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/level3/2018/EVE_L3_2018110_006_02.fit'
     ),
    (L3mClient, TimeRange('2018/04/20', '2018/04/20'),
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/merged/latest_EVE_L3_merged.fit',
     'http://lasp.colorado.edu/eve/data_access/evewebdataproducts/merged/latest_EVE_L3_merged.fit'
     ),
])
def test_get_url_for_time_range(client, timerange, url_start, url_end):
    urls = client._get_url_for_timerange(timerange)
    assert isinstance(urls, list)
    assert urls[0] == url_start
    assert urls[-1] == url_end


def test_can_handle_query():
    ans1 = eve.EVEClient._can_handle_query(
        Time('2012/8/9', '2012/8/10'), Instrument('eve'), Level(0))
    assert ans1 is True
    ans2 = eve.EVEClient._can_handle_query(Time('2012/7/7', '2012/7/7'))
    assert ans2 is False
    ans3 = eve.EVEClient._can_handle_query(
        Time('2012/8/9', '2012/8/10'), Instrument('eve'), Source('sdo'))
    assert ans3 is False

    ans4 = eve.EVELevel2BClient._can_handle_query(
        Time('2018/04/20', '2018/04/20'), Instrument('eve'), Level('2b'))
    assert ans4 is True, 'should handle valid query'
    ans5 = eve.EVELevel2BClient._can_handle_query(
        Time('2018/04/20', '2018/04/20'), Instrument('eve'), Instrument('eve'))
    assert ans5 is False, 'shouldn\'t handle query with a duplicate field'

    ans6 = eve.EVELevel2Client._can_handle_query(
        Time('2018/01/01 14:00', '2018/01/01 14:00'), Instrument('eve'), Level('2'))
    assert ans6 is True, 'should handle valid query (level as a str)'
    ans7 = eve.EVELevel2Client._can_handle_query(
        Time('2018/01/01 14:00', '2018/01/01 14:00'), Instrument('eve'), Level(2))
    assert ans7 is True, 'should handle valid query (level as an int)'
    ans8 = eve.EVELevel2Client._can_handle_query(
        Time('2018/05/01 14:00', '2018/05/01 14:00'), Instrument('eve'), Level(2))
    assert ans8 is False, 'shouldn\'t handle query for data after L2 production stopped'

    ans9 = eve.EVELevel3Client._can_handle_query(
        Time('2018/04/20', '2018/04/20'), Instrument('eve'), Level('3'))
    assert ans9 is True, 'should handle valid query'
    ans10 = eve.EVELevel3Client._can_handle_query(
        Time('2018/04/20', '2018/04/20'), Instrument('eve'), Level(3))
    assert ans10 is True, 'should handle valid query'

    ans11 = eve.EVELevel3MergedClient._can_handle_query(
        Time('2018/04/20', '2018/04/20'), Instrument('eve'), Level('3m'))
    assert ans11 is True, 'should handle valid query'


@pytest.mark.remote_data
@pytest.mark.parametrize("client,time,end_shift,n_urls", [
    (LCClient, Time('2012/8/9', '2012/8/10'), datetime.timedelta(days=1), 2),
    (L2bClient, Time('2018/05/31', '2018/06/01'), datetime.timedelta(days=1), 2),
    (L2Client, Time('2018/01/01 14:00', '2018/01/01 15:00'), datetime.timedelta(hours=1), 2),
    (L3Client, Time('2018/05/31', '2018/06/01'), datetime.timedelta(days=1), 2),
    (L3mClient, Time('1900/01/01', datetime.date.today()), datetime.timedelta(days=0), 1),
])
def test_query(client, time, end_shift, n_urls):
    qr1 = client.search(time, Instrument('eve'))
    assert isinstance(qr1, QueryResponse)
    assert len(qr1) == n_urls
    assert qr1.time_range().start == time.start
    assert qr1.time_range().end == time.end + end_shift  # includes end.


@pytest.mark.remote_data
@pytest.mark.parametrize("client,time", [
    (LCClient, Time('2012/11/27', '2012/11/27')),
    (L2bClient, Time('2018/04/20', '2018/04/20')),
    (L2Client, Time('2018/01/01 14:00', '2018/01/01 14:00'),),
    (L3Client, Time('2018/04/20', '2018/04/20')),
])
def test_get(client, time):
    qr1 = client.search(time, Instrument('eve'))
    res = client.fetch(qr1)
    download_list = res.wait(progress=False)
    assert len(download_list) == len(qr1)


@pytest.mark.remote_data
@pytest.mark.parametrize('cls,query', [
    (eve.EVEClient, a.Time('2012/10/4', '2012/10/6') & a.Instrument('eve') & a.Level(0)),
    (eve.EVELevel2BClient, a.Time('2018/04/20', '2018/04/20') & a.Instrument('eve') & a.Level('2b')),
])
def test_fido(cls, query):
    qr = Fido.search(query)
    client = qr.get_response(0).client
    assert isinstance(qr, UnifiedResponse)
    assert isinstance(client, cls)
    response = Fido.fetch(qr)
    assert len(response) == qr._numfile


@pytest.mark.remote_data
def test_fido_where_date_dir_doesnt_exist():
    query = a.Time('2018/03/19', '2018/03/19') & a.Instrument('eve') & a.Level('2b')
    qr = Fido.search(query)
    client = qr.get_response(0).client
    assert isinstance(qr, UnifiedResponse)
    assert not isinstance(client, eve.EVELevel2BClient)
    response = Fido.fetch(qr)
    assert len(response) == qr._numfile


@pytest.mark.remote_data
@given(time_attr(time=datetimes(
    max_value=datetime.datetime(datetime.datetime.utcnow().year, 1, 1, 0, 0),
    min_value=datetime.datetime(2010, 1, 1, 0, 0),
)))
@settings(max_examples=2)
def test_levels(time):
    """
    Test the correct handling of level 0 / 1.
    The default should be level 1 from VSO, level 0 comes from EVEClient.
    """
    eve_a = a.Instrument('EVE')
    qr = Fido.search(time, eve_a)  # TODO: Should the default remain L0?
    client = qr.get_response(0).client
    assert isinstance(client, VSOClient)

    qr = Fido.search(time, eve_a, a.Level(0))
    client = qr.get_response(0).client
    assert isinstance(client, eve.EVEClient)

    qr = Fido.search(time, eve_a, a.Level(0) | a.Level(1))
    clients = {type(item.client) for item in qr.responses}
    assert clients.symmetric_difference({VSOClient, eve.EVEClient}) == set()
