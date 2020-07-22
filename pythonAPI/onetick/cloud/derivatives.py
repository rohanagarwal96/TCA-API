from onetick.cloud.query import run
from abc import abstractproperty,abstractmethod
from .data import Config
import configparser
import inspect

REF_DATA_KEY='ref_data_by_product_code'
TRADES_KEY='trades_by_product_code'
QUOTES_KEY='quotes_by_product_code'
MKT_KEY='market_by_product_code'

def _schema_by(symbol):
    #TODO handle list of product codes - raise if mixed list
    prefix = symbol[0].split('::')[0] if isinstance(symbol,list) else symbol.split('::')[0]
    if prefix == 'GLOBEX':
        return _Globex(symbol)
    elif prefix == 'ICE':
        return _ICE(symbol)
    elif prefix == 'CFE':
        return _CFE(symbol)
    elif prefix == 'EUREX':
        return _Eurex(symbol)
    else:
        return _OTSymbol(symbol)
    
class _FuturesSchema(object):
    def __init__(self, symbol):
        self._symbol=symbol
        self._otq=None
        self.config = configparser.ConfigParser()
        self.config.read(Config.config)
        
    @property
    def otq(self):
        pass    
        
    @abstractproperty
    def lookup(self):
        pass
    
    @abstractmethod
    def query_params(self, sec_type, bars):
        pass
    
    def query(self, sec_type):
        key = '{}_by_{}'.format(sec_type,self.lookup) if sec_type not in ['continuous_trades','continuous_quotes','continuous_settlement_trades','settlement_trades'] else sec_type
        key =  self.otq.get(key)
        if key is None:
            func_name = inspect.stack()[2][3]
            raise ValueError("The function otc.{} is not implemented for symbol {}. Please contact the OneTick cloud team.".format(func_name,self._symbol))
        return key[0]
    
    def __str__(self):
        return self._symbol

class _FuturesProductCode(_FuturesSchema):
    
    def __init__(self, symbol):
        super().__init__(symbol)
        self.otq_info={'continuous_trades':(self.config.get(Config.user,'getContinuousContractTradesForRoot'),'PRODUCT_CODE="{}",BAR="{}"'),
                       'continuous_quotes':(self.config.get(Config.user,'getContinuousContractQuotesForRoot'),'PRODUCT_CODE="{}",BAR="{}"'),
                       'continuous_settlement_trades':(self.config.get(Config.user,'getCMEContinuousContractSettlementsForRoot'),'PRODUCT_CODE="{}",ROLLAT_DAYS_FROM_EXPIRY="{}"'),
                       'settlement_trades':(self.config.get(Config.user,'getCMEContinuousContractSettlementsForRoot'),'PRODUCT_CODE="{}",ROLLAT_DAYS_FROM_EXPIRY="1"'),

					   
            }
        
    @property
    def lookup(self):
        return 'product_code'

    @property
    def otq(self):
        return self.otq_info
    
    @property
    def suffix(self):
        return self._symbol.split('::')[1]
        
    def query_params(self, sec_type, bars=None, expiry=None):
        key = '{}_by_{}'.format(sec_type,self.lookup) if sec_type not in ['continuous_trades','continuous_quotes','continuous_settlement_trades','settlement_trades'] else sec_type
        param_string = self.otq.get(key)
        if param_string is None:
            func_name = inspect.stack()[2][3]
            raise ValueError("The function otc.{} is not implemented for symbol {}. Please contact the OneTick cloud team.".format(func_name,self._symbol))
        param_string=param_string[1]
        if expiry:
            expiry = 0 if expiry == 'front' else -30       
            return param_string.format(self.suffix,expiry) 
        return param_string.format(self.suffix,bars) if bars is not None else param_string.format(self.suffix)
     
class _Globex(_FuturesProductCode):
    
    def __init__(self, symbol):
        super().__init__(symbol)
        otq_info = {'ref_data_by_product_code'  : (self.config.get(Config.user,'getCMEContractsForRoot'),'PRODUCT_CODE="{}"'),
                         'trades_by_product_code'    : (self.config.get(Config.user,'getCMEContractTradesForRoot'),'PRODUCT_CODE="{}",BAR="{}"'),
                         'quotes_by_product_code'    : (self.config.get(Config.user,'getCMEContractQuotesForRoot'),'PRODUCT_CODE="{}",BAR="{}"'),
                         'market_by_product_code'    : (self.config.get(Config.user,'getCMEContractMarketPricesForRoot'),'PRODUCT_CODE="{}",LEVELS="{}"'),
                        }
        self.otq_info.update(otq_info)
        
class _ICE(_FuturesProductCode):
    
    def __init__(self, symbol):
        super().__init__(symbol)
        self.otq_info['ref_data_by_product_code'] = (self.config.get(Config.user,'getICEContractsForRoot'),'PRODUCT_CODE="{}"')
        self.otq_info['trades_by_product_code']= (self.config.get(Config.user,'getICEContractTradesForRoot'),'PRODUCT_CODE="{}",BAR="{}"')
        self.otq_info['quotes_by_product_code']= (self.config.get(Config.user,'getICEContractQuotesForRoot'),'PRODUCT_CODE="{}",BAR="{}"')

class _CFE(_FuturesProductCode):
    
    def __init__(self, symbol):
        super().__init__(symbol)
        self.otq_info['ref_data_by_product_code'] = (self.config.get(Config.user,'getCFEContractsForRoot'),'PRODUCT_CODE="{}"')

class _Eurex(_FuturesProductCode):
    
    def __init__(self, symbol):
        super().__init__(symbol)
        self.otq_info['ref_data_by_product_code'] = (self.config.get(Config.user,'getEUREXContractsForRoot'),'PRODUCT_CODE="{}"')
          
class _OTSymbol(_FuturesSchema):
    
    def __init__(self,symbol):
        super().__init__(symbol)
        prefix = self._symbol[0].split('::')[0] if isinstance(self._symbol,list) else self._symbol.split('::')[0]
        if prefix not in ['CME', 'MS77','MS64','MS75','MS67','MS66']:
            raise ValueError('Only CME symbols supported for trades/quotes by symbol-lookup. Please contact the OneTick Cloud team')
        self.otq_info =  {'trades_by_symbol' : (self.config.get(Config.user,'getCMETradesForContractSymbol'),'SYMBOL_LIST="{}",DELIMITER="|",BAR={}'),
                          'quotes_by_symbol'  : (self.config.get(Config.user,'getCMEQuoteForContractSymbol'),'SYMBOL_LIST="{}",DELIMITER="|",BAR={}'),
                          'market_by_symbol'  : (self.config.get(Config.user,'getCMEMarketPricesForContractSymbol'),'SYMBOL_LIST="{}",LEVELS="{}"')
                        }
    @property
    def otq(self):
        return self.otq_info
        
    @property
    def lookup(self):
        return 'symbol'
    
    def query_params(self, sec_type, bars, expiry=None):
        key = '{}_by_{}'.format(sec_type,self.lookup) if sec_type not in ['continuous_trades','continuous_quotes'] else sec_type
        param_string = self.otq.get(key)
        if param_string is None:
            func_name = inspect.stack()[2][3]
            raise ValueError("The function otc.{} is not implemented for symbol {}. Please contact the OneTick cloud team.".format(func_name,self._symbol))
        param_string=param_string[1]
        return param_string.format('|'.join(self._symbol) if isinstance(self._symbol,list) else self._symbol,bars)

def _get_futures(otq, params, startdate, enddate, timezone='GMT', compression=True, filepath=None):
    username = Config.user
    password = Config.password
    response = run(otq=otq,url=Config.url,start_date=startdate,end_date=enddate,user=username,password=password,timezone=timezone,otq_params=params,response='csv',compression=compression)
    return response.write(filepath,tofile=True) if filepath is not None else response.write(filepath,tofile=False)

def _otq_and_params(symbol, bar, sec_type, expiry=None, **kwargs):
    schema = _schema_by(symbol)
    if isinstance(schema,_OTSymbol) and sec_type == 'market' and bar == 1: #TODO Move logic to '_schema_by' and return appropriate 'TopOfBook' object or such
        schema.otq['market_by_symbol'] = (schema.config.get(Config.user,'getCMETradesQuotesForContractSymbol'),'SYMBOL_LIST="{}",DELIMITER="|"')
    params=schema.query_params(sec_type,bar,expiry)
    params=','.join(['{}={}'.format(k.upper(),v) for k,v in kwargs.items()] + params.split(','))
    return schema.query(sec_type), params
  
    
def trades(symbol, startdate, enddate, timezone='GMT', compression=True, filepath=None, bar=86400, is_continuous=False,is_settlement=False,expiry=None,**kwargs):
    if expiry and expiry not in ['front','back']:
        raise ValueError('expiry must be either "front" or "back" or None')
    sec_type = 'continuous_trades' if is_continuous else 'trades'
    sec_type = 'continuous_settlement_trades' if is_settlement and is_continuous else sec_type
    sec_type = 'settlement_trades' if is_settlement and not is_continuous else sec_type


    otq, params = _otq_and_params(symbol, bar, sec_type,expiry,**kwargs)
    return _get_futures(otq, params, startdate, enddate, timezone, compression, filepath)

def quotes(symbol, startdate, enddate, timezone='GMT', compression=True, filepath=None, bar=86400, is_continuous=False,**kwargs):
    sec_type = 'continuous_quotes' if is_continuous else 'quotes'
    otq, params = _otq_and_params(symbol, bar, sec_type, **kwargs)
    return _get_futures(otq, params, startdate, enddate, timezone, compression, filepath)

def market_prices(symbol, startdate, enddate, timezone='GMT', compression=True, levels=3, filepath=None,**kwargs):
    otq, params = _otq_and_params(symbol, levels, 'market',**kwargs)
    return _get_futures(otq, params, startdate, enddate, timezone, compression, filepath)

def reference_data(symbol, startdate, enddate, timezone='GMT', compression=True, filepath=None, **kwargs):
    otq, params = _otq_and_params(symbol, None, 'ref_data', **kwargs)
    return _get_futures(otq, params, startdate, enddate, timezone, compression, filepath)


