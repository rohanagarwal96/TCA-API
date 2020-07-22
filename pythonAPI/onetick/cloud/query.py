import json
from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
from io import BytesIO,StringIO
from abc import abstractmethod
from sys import platform
import gzip

CLOUD_URL = 'https://cloud.onetick.com:443/omdwebapi/rest'
CLOUD2_URL= 'https://cloud2.onetick.com:443/omdwebapi/rest'
CLDR_URL= 'https://cldr.onetick.com:443/omdwebapi/rest'

class QueryResponse(object):
    
    def __init__(self, response):
        self._response = response
        
    @property
    def response(self):
        return self._response
    
    @response.setter
    def reponse(self):
        raise ValueError('Error! Cannot set the Response object.')
    
    @property
    def _decode(self):
        return self.response.content.decode('utf-8')
        
    @abstractmethod
    def to_dataframe(self):
        pass
    
    @abstractmethod
    def write(self,*args):
        with open(args[0], "wb+") as code:
            code.write(self._response.content)

    def __str__(self):
        return str(super())

class JSONResponse(QueryResponse):
    def __init__(self,response):
        super(JSONResponse,self).__init__(response)
        
    def to_dataframe(self):
        return pd.read_csv(self._to_csv())
    
    def __str__(self):
        return self._decode
    
    def write(self, path, csv=False):
        import shutil
        with open (path, 'w+') as fd:
            if csv:
                buffer = self._to_csv()
                buffer.seek (0)
                shutil.copyfileobj (buffer, fd)
            else:
                fd.write(str(self))                
        return True
    
    def _to_csv(self):
        csv = ''
        fnames = []
        for line in self._decode.splitlines():
            data = json.loads(line)
            msg_type = data['MSG_TYPE']
            if ( msg_type == "PROCESS_TICK_DESCRIPTOR"):
                fnames = data["FIELD_NAMES"]
                fnames.insert(0, 'TIMESTAMP')
                csv += '#' + ','.join(['_SYMBOL_NAME'] +fnames) + '\n'
            elif ( msg_type == "PROCESS_EVENT"):
                sn = data['_SYMBOL_NAME']
                for i in range(len(data['TIMESTAMP'])):
                    values = [sn]
                    for fname in fnames:
                        values.append(data[fname][i])
                    csv += ','.join(values) + '\n'
        return StringIO(csv)
    
class CSVResponse(QueryResponse):
    def __init__(self, response):
        super(CSVResponse,self).__init__(response)

    def to_dataframe(self):
        csv = ''
        for line in self._decode.splitlines():
            csv+='{},\n'.format(line)
        del csv[-1]
        return pd.read_csv(StringIO(csv))
    
    def write(self, path, tofile=None):
        return super(CSVResponse,self).write(path) if path is not None else self.to_dataframe()
        
    def __str__(self):
        return self._decode
    
class CompressedResponse(QueryResponse):
    def __init__(self, response):
        super(CompressedResponse,self).__init__(response)

    def write(self, path, tofile=True):
        if tofile:
            outfile=path
            outfile = '{}.gz'.format(outfile) if not (outfile.endswith('.gz') or outfile.endswith('.gzip')) else outfile
            super(CompressedResponse,self).write(path)
        else:
            #===================================================================
            # out = BytesIO()
            # with gzip.GzipFile(fileobj=out, mode="w") as f:
            #     f.write(self.response.content)
            # return f
            #===================================================================
            if platform == "win32":
              outfile='C:/temp/temp.csv.gz'
            else:
              outfile='/tmp/temp.csv.gz'

            outfile = '{}.gz'.format(outfile) if not (outfile.endswith('.gz') or outfile.endswith('.gzip')) else outfile
            super(CompressedResponse,self).write(outfile)
            try:
               return pd.read_csv(outfile, compression='gzip', header=0, sep=',', quotechar='"')
            except:
               return pd.DataFrame()
        
    def to_dataframe(self):
        raise RuntimeError('Not Implemented')
    
    
        
def _args_to_dict(otq, user, password, url, symbol, otq_params, start_date, end_date, timezone,context,query_type,output_only_one_header,response,batch_size,otq_file_content_as_string,msec,symbol_date,apply_times_daily, running_query,compression):
    if url is None or '':
        raise RuntimeError("url not specified!")
    
    #TODO use one list instead of three
    #mandatory_args = dict(zip(['url','otq','user','password'],[url,otq,user,password]))
    mandatory_args = dict(zip(['otq'],[otq]))
    optional_args = ['symbols','otq_params','timezone','context','query_type','output_only_one_header','response','bs_ticks','otq_file_content_as_string','msec','apply_times_daily','running_query']
    optional_vals = [symbol,otq_params,timezone,context,query_type,output_only_one_header,response,batch_size,otq_file_content_as_string,msec,apply_times_daily,running_query]
    optional_args = {k:v for k, v in zip(optional_args,optional_vals) if v is not None}
    date_args = {k:v.strftime("%Y%m%d%H%M%S") for k,v in zip(['s','e','symbol_date'],[start_date,end_date,symbol_date]) if v is not None}
    if compression:
        mandatory_args['compression']='gzip'
    d=  {k.strip():v.strip() for d in [mandatory_args,optional_args,date_args] for k, v in d.items()}
    #print(d)
    return d
    
def _json_request(js_str,url,user,password):
    headers = { 'Content-Type' : 'application/json' }
    #print(js_str)
    return requests.post( url, data=js_str, auth=HTTPBasicAuth(user,password), headers=headers)

def run(otq,
        user, 
        password, 
        url=CLOUD_URL,
        symbol=None,
        otq_params=None, 
        start_date=None, 
        end_date=None, 
        timezone='GMT', 
        context='DEFAULT',
        query_type='otq',
        output_only_one_header='true',
        response='json', 
        batch_size=100, 
        otq_file_content_as_string=False, 
	msec='false',
        symbol_date=None, 
        apply_times_daily=False,
        running_query=False,
        compression=False
        ):
    
    def response_factory(compression, response, file_type):
        response_ = None
        
        if compression:
            response_ = CompressedResponse(response)
        elif file_type.lower() == 'json':
            response_ = JSONResponse(response)
        elif file_type.lower() == 'csv':
            response_ = CSVResponse(response)
        else:
            raise RuntimeError('Invalid response specified!')
        
        return response_
    #fmt = '["TIMESTAMP=%|{}|%Y-%m-%d %H:%M:%S.%q"]'.format(timezone)
    #print(fmt)
    query_args = _args_to_dict(otq, 
                               user, 
                               password, 
                               url, 
                               symbol, 
                               otq_params, 
                               start_date, 
                               end_date, 
                               timezone,
                               context,
                               query_type,
			       output_only_one_header,
                               response,
                               str(batch_size),
                               str(otq_file_content_as_string).lower(),
			       msec,
                               symbol_date,
                               None,#str(apply_times_daily).lower(),
                               None,#str(running_query).lower(),
                               compression=compression
                               )
    js = json.dumps(query_args)
    response_ = _json_request(js,url,user,password)
    
    if not response_.ok:
        raise RuntimeError('Unable to get request:\n{}'.format(response_.text))
            
    return response_factory(compression, response_, response)

