# from .pynse import *
#
# __VERSION__ = 0.1

import datetime as dt
import time
import enum
import logging
import urllib.parse
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import io
import zipfile
import os
import pickle
from fake_headers import Headers
from pprint import pprint

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)


class OutputType(enum.Enum):
    pandas = 'pd'
    dict = 'json'


class Format(enum.Enum):
    pkl = 'pkl'
    csv = 'csv'


class Segment(enum.Enum):
    EQ = 'EQ'
    FUT = 'FUT'
    OPT = 'OPT'


class OptionType(enum.Enum):
    CE = 'Call'
    PE = 'Put'


class IndexSymbol(enum.Enum):
    All = 'ALL'
    FnO = 'FNO'
    Nifty50 = 'NIFTY 50'
    NiftyNext50 = 'NIFTY NEXT 50'
    Nifty100 = 'NIFTY 100'
    Nifty200 = 'NIFTY 200'
    Nifty500 = 'NIFTY 500'
    NiftyMidcap50 = 'NIFTY MIDCAP 50'
    NiftyMidcap100 = 'NIFTY MIDCAP 100'
    NiftySmlcap100 = 'NIFTY SMLCAP 100'
    NiftyMidcap150 = 'NIFTY MIDCAP 150'
    NiftySmlcap50 = 'NIFTY SMLCAP 50'
    NiftySmlcap250 = 'NIFTY SMLCAP 250'
    NiftyMidsml400 = 'NIFTY MIDSML 400'
    NiftyBank = 'NIFTY BANK'
    NiftyAuto = 'NIFTY AUTO'
    NiftyFinService = 'NIFTY FIN SERVICE'
    NiftyFmcg = 'NIFTY FMCG'
    NiftyIt = 'NIFTY IT'
    NiftyMedia = 'NIFTY MEDIA'
    NiftyMetal = 'NIFTY METAL'
    NiftyPharma = 'NIFTY PHARMA'
    NiftyPsuBank = 'NIFTY PSU BANK'
    NiftyPvtBank = 'NIFTY PVT BANK'
    NiftyRealty = 'NIFTY REALTY'
    Nifty50Value20 = 'NIFTY50 VALUE 20'
    NiftyAlpha50 = 'NIFTY ALPHA 50'
    Nifty50EqlWgt = 'NIFTY50 EQL WGT'
    Nifty100EqlWgt = 'NIFTY100 EQL WGT'
    Nifty100Lowvol30 = 'NIFTY100 LOWVOL30'
    Nifty200Qualty30 = 'NIFTY200 QUALTY30'
    NiftyCommodities = 'NIFTY COMMODITIES'
    NiftyConsumption = 'NIFTY CONSUMPTION'
    NiftyEnergy = 'NIFTY ENERGY'
    NiftyInfra = 'NIFTY INFRA'
    NiftyMnc = 'NIFTY MNC'
    NiftyPse = 'NIFTY PSE'
    NiftyServSector = 'NIFTY SERV SECTOR'


class Nse:
    """
    pynse is a library to extract realtime and historical data from NSE website

    Examples
    --------

    >>> from pynse import *
    >>> nse = Nse()
    >>> nse.market_status()

    """
    __service_config = {'host': 'https://www.nseindia.com',
                        'path': {'marketStatus': '/api/marketStatus',
                                 'info': "/api/equity-meta-info?symbol={symbol}",
                                 'quote_eq': "/api/quote-equity?symbol={symbol}",
                                 'quote_derivative': "/api/quote-derivative?symbol={symbol}",
                                 'trade_info': "/api/quote-equity?symbol={symbol}&section=trade_info",
                                 'hist': "/api/historical/cm/equity?symbol={symbol}&series=[%22EQ%22]&from={from_date}&to={to_date}&csv=true",
                                 'bhavcopy': 'https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv',
                                 'bhavcopy_derivatives': 'https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month}/fo{date}bhav.csv.zip',
                                 'preOpen': "/api/market-data-pre-open?key=ALL",
                                 'fii_dii': "/api/fiidiiTradeReact",
                                 'option_chain_index': '/api/option-chain-indices?symbol={symbol}',
                                 'option_cahin_equities': '/api/option-chain-equities?symbol={symbol}',
                                 'indices': '/api/allIndices',
                                 'indices_hist_base': "https://www1.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType=",
                                 'symbol_list': '/api/equity-stockIndices?index={index}',
                                 'fnoSymbols': "/api/master-quote",
                                 'gainer_loser': '/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O'
                                 }
                        }

    def __init__(self):

        data_root = ''
        self.dir = {
            'data_root': data_root,
            'bhav_eq': f'{data_root}bhavcopy_eq/',
            'bhav_fno': f'{data_root}bhavcopy_fno/',
            'option_chain': f'{data_root}option_chain/',
            'symbol_list': f'{data_root}symbol_list/',
            'pre_open': f'{data_root}pre_open/',
            'hist': f'{data_root}hist/',
            'fii_dii': f'{data_root}fii_dii/'
        }
        self.__create_dir()

        self.expiry_list = []
        self.strike_list = []
        self.max_retries = 5
        self.timeout = 10
        self.headers = self.__generate_headers()

        symbol_files = {i.name: f"{self.dir['symbol_list']}{i.name}.pkl" for i in IndexSymbol}

        self.symbols = {'All': ['SBIN']}
        for i in IndexSymbol:
            if not os.path.exists(symbol_files[i.name]):
                for retry in range(5):
                    try:
                        self.__symbol_list(i)

                    except:
                        logger.info(f'retrying symbol downloads for{i}')
                        time.sleep(5)
                    else:
                        with open(symbol_files[i.name], 'rb')as f:
                            self.symbols[i.name] = pickle.load(f)
                        break
            else:
                with open(symbol_files[i.name], 'rb')as f:
                    self.symbols[i.name] = pickle.load(f)

    def __get_resp(self, url, retries=0, timeout=5):
        retries = self.max_retries if retries == 0 else retries
        timeout = self.timeout if timeout == 0 else timeout

        for nrt in range(retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=timeout)
            except Exception as e:
                logger.error(e)

                if nrt + 1 == retries:
                    raise ConnectionError()
                self.headers = self.__generate_headers()
                logger.debug('retrying')

                time.sleep(2)
            else:
                return response

    def __generate_headers(self):
        h = Headers(headers=True).generate()
        return h

    def __create_dir(self):
        if not os.path.exists(self.dir['symbol_list']):
            logger.debug('Create folders and downloading symbols. it may take some time')

        for _, path in self.dir.items():
            if path != '':
                if not os.path.exists(path):
                    os.mkdir(path)

    @staticmethod
    def __validate_symbol(symbol, _list):

        symbol = symbol if isinstance(symbol, IndexSymbol) else symbol.upper()
        if isinstance(symbol, IndexSymbol):
            symbol = urllib.parse.quote(symbol.value)

            return symbol

        elif symbol in _list:
            symbol = urllib.parse.quote(symbol.upper())

            return symbol


        else:
            symbol = None
            raise ValueError('not a vaild symbol')

    def market_status(self) -> dict:
        """
        get market status

        Examples
        --------

        >>> nse.market_status()

        """
        config = self.__service_config
        logger.info("downloading market status")
        url = config['host'] + config['path']['marketStatus']

        return self.__get_resp(url=url).json()

    def info(self, symbol: str = 'SBIN') -> dict:
        '''
        Get symbol information from nse

        Examples
        --------

        >>> nse.info('SBIN')

        '''
        config = self.__service_config
        symbol = self.__validate_symbol(symbol, self.symbols[IndexSymbol.All.name])
        if symbol is not None:
            logger.info(f"downloading symbol info for {symbol}")
            url = config['host'] + config['path']['info'].format(symbol=symbol)

            return self.__get_resp(url=url).json()

    def get_quote(self,
                  symbol: str = 'HDFC',
                  segment: Segment = Segment.EQ,
                  expiry: dt.date = None,
                  optionType: OptionType = OptionType.CE,
                  strike: str = '-') -> dict:
        """

        Get realtime quote for EQ, FUT and OPT

        if no expiry date is provided for derivatives, returns date for nearest expiry

        Examples
        --------
        for cash
        >>> nse.get_quote('RELIANCE')

        for futures
        >>> nse.get_quote('TCS', segment=Segment.FUT, expiry=dt.date( 2020, 6, 30 ))

        for options
        >>> nse.get_quote('HDFC', segment=Segment.OPT, optionType=OptionType.PE)

        """
        config = self.__service_config
        segment = segment.value
        optionType = optionType.value

        if symbol is not None:
            logger.info(f"downloading quote for {symbol} {segment}")
            quote = {}
            if segment == 'EQ':
                symbol = self.__validate_symbol(symbol,
                                                self.symbols[IndexSymbol.All.name] + [idx.value for idx in IndexSymbol])

                url = config['host'] + config['path']['quote_eq'].format(symbol=symbol)
                url1 = config['host'] + config['path']['trade_info'].format(symbol=symbol)
                data = self.__get_resp(url).json()
                data.update(self.__get_resp(url1).json())
                quote = data['priceInfo']
                quote['timestamp'] = dt.datetime.strptime(data['metadata']['lastUpdateTime'], '%d-%b-%Y %H:%M:%S')
                quote.update(series=data['metadata']['series'])
                quote.update(symbol=data['metadata']['symbol'])
                quote.update(data['securityWiseDP'])
                quote['low'] = quote['intraDayHighLow']['min']
                quote['high'] = quote['intraDayHighLow']['max']

            elif segment == 'FUT':
                symbol = self.__validate_symbol(symbol,
                                                self.symbols[IndexSymbol.FnO.name] + ['NIFTY', 'BANKNIFTY'])

                url = config['host'] + config['path']['quote_derivative'].format(symbol=symbol)

                data = self.__get_resp(url).json()
                quote['timestamp'] = dt.datetime.strptime(data['fut_timestamp'], '%d-%b-%Y %H:%M:%S')

                data = [
                    i for i in data['stocks']
                    if segment.lower() in i['metadata']['instrumentType'].lower()
                ]

                expiry_list = list(
                    dict.fromkeys([dt.datetime.strptime(i['metadata']['expiryDate'], '%d-%b-%Y').date() for i in data]))

                if expiry is None:
                    expiry = expiry_list[0]

                data = [i for i in data if
                        dt.datetime.strptime(i['metadata']['expiryDate'], '%d-%b-%Y').date() == expiry]

                quote.update(data[0]['marketDeptOrderBook']['tradeInfo'])
                quote.update(data[0]['metadata'])
                quote['expiryDate'] = dt.datetime.strptime(quote['expiryDate'], '%d-%b-%Y').date()

            elif segment == 'OPT':
                url = config['host'] + config['path']['quote_derivative'].format(symbol=symbol)

                data = self.__get_resp(url).json()

                quote['timestamp'] = dt.datetime.strptime(data['opt_timestamp'], '%d-%b-%Y %H:%M:%S')

                data = [
                    i for i in data['stocks']
                    if segment.lower() in i['metadata']['instrumentType'].lower()
                       and i['metadata']['optionType'] == optionType
                ]

                self.strike_list = list(dict.fromkeys(
                    [i['metadata']['strikePrice'] for i in data]))

                strike = strike if strike in self.strike_list else self.strike_list[0]

                self.expiry_list = list(
                    dict.fromkeys([dt.datetime.strptime(i['metadata']['expiryDate'], '%d-%b-%Y').date() for i in data]))

                if expiry is None:
                    expiry = self.expiry_list[0]

                data = [i for i in data if
                        dt.datetime.strptime(i['metadata']['expiryDate'], '%d-%b-%Y').date() == expiry and
                        i['metadata']['strikePrice'] == strike]
                quote.update(data[0]['marketDeptOrderBook']['tradeInfo'])
                quote.update(data[0]['marketDeptOrderBook']['otherInfo'])
                quote.update(data[0]['metadata'])
                quote['expiryDate'] = dt.datetime.strptime(quote['expiryDate'], '%d-%b-%Y').date()

            return quote

    def bhavcopy(self, req_date: dt.date = None,
                 series: str = 'eq') -> pd.DataFrame:
        """
        download bhavcopy from nse
        or
        read bhavcopy if already downloaded

        Examples
        --------

        >>> nse.bhavcopy()

        >>> nse.bhavcopy(dt.date(2020,6,17))
        """

        series = series.upper()
        req_date = self.___trading_days()[-1].date() if req_date is None else req_date

        filename = f'{self.dir["bhav_eq"]}bhav_{req_date}.pkl'

        bhavcopy = None
        if os.path.exists(filename):
            bhavcopy = pd.read_pickle(filename)
            logger.debug(f'read {filename} from disk')

        else:
            config = self.__service_config
            url = config['path']['bhavcopy'].format(date=req_date.strftime("%d%m%Y"))
            csv = self.__get_resp(url).content.decode('utf8').replace(" ", "")

            bhavcopy = pd.read_csv(io.StringIO(csv))
            bhavcopy["DATE1"] = bhavcopy["DATE1"].apply(lambda x: dt.datetime.strptime(x, '%d-%b-%Y').date())

            bhavcopy.to_pickle(filename)

        if bhavcopy is not None:
            if series != 'ALL':
                bhavcopy = bhavcopy.loc[bhavcopy['SERIES'] == series]

            bhavcopy.set_index(['SYMBOL', 'SERIES'], inplace=True)

        return bhavcopy

    def bhavcopy_fno(self, req_date: dt.date = None) -> pd.DataFrame:
        """
        download bhavcopy from nse
        or
        read bhavcopy if already downloaded

        Examples
        --------

        >>> nse.bhavcopy_fno()

        >>> nse.bhavcopy_fno(dt.date(2020,6,17))

        """
        req_date = self.___trading_days()[-1].date() if req_date is None else req_date

        filename = f'{self.dir["bhav_fno"]}bhav_{req_date}.pkl'

        bhavcopy = None
        if os.path.exists(filename):
            bhavcopy = pd.read_pickle(filename)
            logger.debug(f'read {filename} from disk')

        else:
            config = self.__service_config
            url = config['path']['bhavcopy_derivatives'].format(date=req_date.strftime("%d%b%Y").upper(),
                                                                month=req_date.strftime("%b").upper(),
                                                                year=req_date.strftime("%Y"))

            logger.debug("downloading bhavcopy for {}".format(req_date))
            stream = self.__get_resp(
                url).content

            filebytes = io.BytesIO(stream)
            zf = zipfile.ZipFile(filebytes)

            bhavcopy = pd.read_csv(zf.open(zf.namelist()[0]))

            bhavcopy.set_index('SYMBOL', inplace=True)
            bhavcopy.dropna(axis=1, inplace=True)
            bhavcopy.EXPIRY_DT = bhavcopy.EXPIRY_DT.apply(lambda x: dt.datetime.strptime(x, '%d-%b-%Y'))

            bhavcopy.to_pickle(filename)

        return bhavcopy

    def pre_open(self) -> pd.DataFrame:
        """

        get pre open data from nse

        Examples
        --------

        >>> nse.pre_open()

        """
        logger.debug("downloading preopen data")
        config = self.__service_config
        url = config['host'] + config['path']['preOpen']

        data = self.__get_resp(url).json()
        pre_open_data = pd.DataFrame([{
            "symbol":
                i["metadata"]["symbol"],
            "preOpen":
                i["metadata"]["lastPrice"],
            "change":
                i["metadata"]["change"],
            "pChange":
                i["metadata"]["pChange"],
            "finalQuantity":
                i["metadata"]["finalQuantity"],
            "lastUpdateTime":
                i["detail"]["preOpenMarket"]['lastUpdateTime']
        } for i in data["data"]])

        pre_open_data = pre_open_data.set_index("symbol")
        pre_open_data["lastUpdateTime"] = pre_open_data["lastUpdateTime"].apply(
            lambda x: dt.datetime.strptime(x, '%d-%b-%Y %H:%M:%S'))

        filename = f"{self.dir['pre_open']}{pre_open_data['lastUpdateTime'][-1].date()}.pkl"
        pre_open_data.to_pickle(filename)

        return pre_open_data

    def __option_chain_download(self, symbol):
        symbol = self.__validate_symbol(symbol, self.symbols[IndexSymbol.FnO.name] + ['NIFTY', 'BANKNIFTY', 'NIFTYIT'])
        logger.debug(f'download option chain')
        config = self.__service_config

        url = config['host'] + (config['path']['option_chain_index'] if 'NIFTY' in symbol else config['path'][
            'option_cahin_equities']).format(symbol=symbol)
        data = self.__get_resp(url).json()
        return data

    def __read_object(self, filename, format):
        if format == Format.pkl:
            with open(filename, 'rb')as f:
                obj = pickle.load(f)
            return obj
        elif format == Format.csv:
            with open(filename, 'r')as f:
                obj = f.read()
            return obj
        else:
            raise FileNotFoundError(f'{filename} not found')

    def __save_object(self, obj, filename, format):
        if format == Format.pkl:
            with open(filename, 'wb')as f:
                pickle.dump(obj, f)
        elif format == Format.csv:
            with open(filename, 'w')as f:
                f.write(obj)
        logger.debug(f'saved {filename}')

    def option_chain(self, symbol: str = 'NIFTY', req_date: dt.date = None) -> dict:
        """
        downloads the option chain
        or
        reads if already downloaded

        if no req_date is specified latest available option chain from nse website

        :returns dictonaly containing
            timestamp as str
            option chain as pd.Dataframe
            expiry_list as list

        Examples
        --------

        >>> nse.option_chain('INFY')

        >>> nse.option_chain('INFY',expiry=dt.date(2020,6,30))

        """
        dir = f"{self.dir['option_chain']}{symbol}/"
        if not os.path.exists(dir):
            os.mkdir(dir)

        if req_date is None:
            q = self.get_quote(np.random.choice(self.symbols['FnO']))
            logger.debug('got timestamp')
            timestamp = q['timestamp'] if q is not None else None

            if timestamp.date() == dt.date.today() and timestamp.time() <= dt.time(15, 30):
                filename = f"{dir}{dt.date.today()}_{dt.datetime.now().strftime('%H%M%S')}.pkl"
                download_req = True

            elif timestamp.date() == dt.date.today():
                filename = f"{dir}{dt.date.today()}_eod.pkl"

                download_req = False if os.path.exists(filename) else True

            else:
                prev_trading_day = self.___trading_days()[-1].date()
                filename = f"{dir}{prev_trading_day}_eod.pkl"

                download_req = False if os.path.exists(filename) else True

        else:
            if req_date == dt.date.today():
                q = self.get_quote()

                timestamp = dt.datetime.strptime(self.get_quote()['timestamp'],
                                                 "%d-%b-%Y %H:%M:%S") if q is not None else None

                if timestamp.date() == dt.date.today() and timestamp.time() <= dt.time(15, 30):
                    filename = f"{dir}{req_date}_{dt.datetime.now().strftime('%H%M%S')}.pkl"
                    download_req = True

                else:
                    filename = f"{dir}{req_date}_eod.pkl"
                    download_req = False if os.path.exists(filename) else True
            else:
                prev_trading_day = self.___trading_days()[-1]
                if req_date >= prev_trading_day:
                    filename = f"{dir}{prev_trading_day}_eod.pkl"
                    download_req = False if os.path.exists(filename) else True
                else:
                    filename = f"{dir}{req_date}_eod.pkl"
                    download_req = False

        if download_req:
            data = self.__option_chain_download(symbol)
            self.__save_object(data, filename, Format.pkl)
        data = self.__read_object(filename, Format.pkl)
        expiry_list = data['records']['expiryDates']
        option_chain = pd.json_normalize(data['records']['data'])
        timestamp = data['records']['timestamp']
        return {'timestamp': timestamp, 'data': option_chain, 'expiry_list': expiry_list}

    def fii_dii(self) -> dict:
        """
        get FII and DII data from nse

        Examples
        --------

        >>> nse.fii_dii()

        """
        config = self.__service_config
        url = config['host'] + config['path']['fii_dii']

        fii_file = f'{self.dir["fii_dii"]}fii.csv'
        dii_file = f'{self.dir["fii_dii"]}dii.csv'

        data = self.__get_resp(url).json()

        for i in range(len(data)):
            for k, v in data[i].items():
                try:
                    data[i][k] = float(v)
                except:
                    data[i][k] = v

        df = pd.DataFrame(data).set_index('date')

        df[df['category'] == 'FII/FPI *'].to_csv(fii_file, header=not os.path.exists(fii_file), mode='a+')
        df[df['category'] == 'DII *'].to_csv(dii_file, header=not os.path.exists(dii_file), mode='a+')

        return data

    def __get_hist(self, symbol='SBIN', from_date=None, to_date=None):
        config = self.__service_config

        max_date_range = 480

        if from_date == None:
            from_date = dt.date.today() - dt.timedelta(days=30)
        if to_date == None:
            to_date = dt.date.today()

        hist = pd.DataFrame()

        while True:
            if (to_date - from_date).days > max_date_range:

                marker = from_date + dt.timedelta(max_date_range)
                url = config['host'] + config['path']['hist'].format(symbol=symbol,
                                                                     from_date=from_date.strftime('%d-%m-%Y'),
                                                                     to_date=marker.strftime('%d-%m-%Y'))

                from_date = from_date + dt.timedelta(days=(max_date_range + 1))

                csv = self.__get_resp(url).content.decode('utf8').replace(" ", "")
                is_complete = False

            else:
                url = config['host'] + config['path']['hist'].format(symbol=symbol,
                                                                     from_date=from_date.strftime('%d-%m-%Y'),
                                                                     to_date=to_date.strftime('%d-%m-%Y'))

                from_date = from_date + dt.timedelta(max_date_range + 1)

                csv = self.__get_resp(url).content.decode('utf8').replace(" ", "")
                is_complete = True

            hist = pd.concat([hist, pd.read_csv(io.StringIO(csv))[::-1]])
            if is_complete:
                break
            time.sleep(1)

        hist['Date'] = pd.to_datetime(hist['Date'])
        hist.set_index('Date', inplace=True)
        hist.drop(['series', 'PREV.CLOSE', 'ltp', 'vwap', '52WH', '52WL', 'VALUE', 'Nooftrades'], axis=1, inplace=True)
        try:
            hist.columns = ['open', 'high', 'low', 'close', 'volume']
        except Exception as e:
            print(hist.columns, e)
            time.sleep(5)

        for column in hist.columns[:4]:
            hist[column] = hist[column].astype(str).str.replace(',', '').replace('-', '0').astype(float)
        hist['volume'] = hist['volume'].astype(int)
        return hist

    def __get_hist_index(self, symbol='NIFTY 50', from_date=None,
                         to_date=None):
        if from_date == None:
            from_date = dt.date.today() - dt.timedelta(days=30)
        if to_date == None:
            to_date = dt.date.today()

        config = self.__service_config
        base_url = config['path']['indices_hist_base']

        urls = []
        max_range_len = 100
        while True:
            if (to_date - from_date).days > max_range_len:
                s = from_date
                e = s + dt.timedelta(max_range_len)
                url = f"{base_url}{symbol}&fromDate={s.strftime('%d-%m-%Y')}&toDate={e.strftime('%d-%m-%Y')}"
                urls.append(url)
                from_date = from_date + dt.timedelta(max_range_len + 1)

            else:
                url = f"{base_url}{symbol}&fromDate={from_date.strftime('%d-%m-%Y')}&toDate={to_date.strftime('%d-%m-%Y')}"
                urls.append(url)

                break

        hist = pd.DataFrame(columns=[
            'Date', 'Open', 'High', 'Low', 'Close', 'SharesTraded',
            'Turnover(Cr)'
        ])
        for url in urls:
            page = self.__get_resp(url).content.decode('utf-8')
            raw_table = BeautifulSoup(page, 'lxml').find_all('table')[0]

            rows = raw_table.find_all('tr')

            for row_no, row in enumerate(rows):

                if row_no > 2:
                    _row = [
                        cell.get_text().replace(" ", "").replace(",", "")
                        for cell in row.find_all('td')
                    ]
                    if len(_row) > 4:
                        hist.loc[len(hist)] = _row

            time.sleep(1)
        hist.Date = hist.Date.apply(lambda d: dt.datetime.strptime(d, '%d-%b-%Y'))

        hist.set_index("Date", inplace=True)

        for col in hist.columns:
            hist[col] = hist[col].astype(str).replace(',', '').replace('-', '0').astype(float)

        return hist

    def get_hist(self, symbol: str = 'SBIN', from_date: dt.date = None, to_date: dt.date = None) -> pd.DataFrame:
        """
        get historical data from nse
        symbol index or symbol

        Examples
        --------

        >>> nse.get_hist('SBIN')

        >>> nse.get_hist('NIFTY 50', from_date=dt.date(2020,1,1),to_date=dt.date(2020,6,26))


        """
        symbol = self.__validate_symbol(symbol,
                                        self.symbols[IndexSymbol.All.name] + [idx.value for idx in IndexSymbol])

        if "NIFTY" in symbol:
            return self.__get_hist_index(symbol, from_date, to_date)
        else:
            return self.__get_hist(symbol, from_date, to_date)

    def get_indices(self, index: IndexSymbol = None) -> pd.DataFrame:
        """
        get realtime index value

        Examples
        --------

        >>> nse.get_indices(IndexSymbol.NiftyInfra)
        >>> nse.get_indices(IndexSymbol.Nifty50))

        """

        self.__validate_symbol(index, [idx for idx in IndexSymbol])
        config = self.__service_config
        url = config['host'] + config['path']['indices']
        data = self.__get_resp(url).json()['data']

        data = pd.json_normalize(data).set_index('indexSymbol')
        if index is not None:
            data = data[data.index == index.value]

        data.drop(['chart365dPath', 'chartTodayPath', 'chart30dPath'], inplace=True, axis=1)
        return data

    def __gainers_losers(self, sort=0, length=10):

        config = self.__service_config
        url = config['host'] + config['path']['gainer_loser']
        data = self.__get_resp(url).json()

        table = pd.DataFrame(data['data'])
        table.drop([
            'chart30dPath', 'chart365dPath', 'chartTodayPath', 'meta',
            'identifier', 'lastUpdateTime', 'yearHigh', 'yearLow', 'nearWKH',
            'nearWKL', 'perChange365d', 'date365dAgo', 'date30dAgo',
            'perChange30d'
        ],
            axis=1,
            inplace=True)
        table.set_index('symbol', inplace=True)
        table.sort_values(by=['pChange'],
                          axis=0,
                          ascending=False if sort == 0 else True,
                          inplace=True)
        return table.head(length)

    def top_gainers(self, length: int = 10) -> pd.DataFrame:
        """
        presently works only for SECURITIES IN F&O

        Examples
        --------

        >>> nse.top_gainers(10)

        """
        return self.__gainers_losers(sort=0, length=length)

    def top_losers(self, length: int = 10) -> pd.DataFrame:
        """
        presently works only for SECURITIES IN F&O

        Examples
        --------

        >>> nse.top_losers()(10)

        """
        return self.__gainers_losers(sort=1, length=length)

    def __symbol_list(self, index: IndexSymbol):

        if not isinstance(index, IndexSymbol):
            raise TypeError('index is not of type "Index"')

        config = self.__service_config

        if index == IndexSymbol.All:
            data = list(self.bhavcopy().reset_index().SYMBOL)

        elif index == IndexSymbol.FnO:
            url = config['host'] + config['path']['fnoSymbols']
            data = self.__get_resp(url).json()

            data.extend(['NIFTY', 'BANKNIFTY'])

        else:

            url = config['host'] + config['path']['symbol_list'].format(
                index=self.__validate_symbol(index, IndexSymbol))
            data = self.__get_resp(url).json()['data']
            data = [i['meta']['symbol'] for i in data if i['identifier'] != index.value]

        data.sort()
        with open(self.dir['symbol_list'] + index.name + '.pkl', 'wb')as f:
            pickle.dump(data, f)
            logger.info(f'symbol list saved for {index}')
        return data

    def update_symbol_list(self):
        """
        Update list of symbols
        no need to run frequently
        required when constituent of an index is changed
        or
        list of securities in fno are updates
        :return: None

        Examples:

        >>> nse.update_symbol_list()

        """
        for i in [a for a in IndexSymbol]:
            self.__symbol_list(i)
            time.sleep(1)

    def ___trading_days(self):

        filename = f'{self.dir["data_root"]}/trading_days.csv'

        if os.path.exists(filename):
            trading_days = pd.read_csv(filename, header=None)
            trading_days.columns = ['Date']
            trading_days['Date'] = trading_days['Date'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d'))
            previous_trading_day = list(trading_days.tail(1)['Date'])[0].date()

        else:
            previous_trading_day = dt.date.today() - dt.timedelta(days=100)
            trading_days = pd.DataFrame()

        if previous_trading_day == dt.date.today() or previous_trading_day == dt.date.today() - dt.timedelta(
                days=1) and dt.datetime.now().time() <= dt.time(18, 45):
            pass

        else:
            _trading_days = self.get_hist(symbol='SBIN', from_date=previous_trading_day - dt.timedelta(7),
                                          to_date=dt.date.today()).reset_index()[['Date']]

            trading_days = pd.concat([trading_days, _trading_days]).drop_duplicates()
            trading_days.to_csv(filename, mode='w', index=False, header=False)

        trading_days = pd.read_csv(filename, header=None, index_col=0)
        trading_days.index = trading_days.index.map(lambda x: dt.datetime.strptime(x, "%Y-%m-%d"))

        return trading_days.index


if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    nse = Nse()
    # pprint(nse.market_status())
    # pprint( nse.info('HDFC'))
    # pprint(nse.get_quote('HDFC', segment=Segment.OPT, optionType=OptionType.PE))
    # pprint(nse.bhavcopy())
    # pprint(nse.bhavcopy_fno(dt.date(2020,6,17)))
    # pprint(nse.bhavcopy_fno())
    # pprint(nse.option_chain('HDFC'))
    # pprint(nse.pre_open())
    # pprint(nse.fii_dii())
    # pprint(nse.get_hist('NIFTY 50'))
    # pprint(nse.get_indices(IndexSymbol.NiftyInfra))
    # pprint(nse.top_gainers())
    # pprint(nse.top_losers())