import os

#import sys
#sys.path.append("C:\\Users\\rohha\\Documents\\pythonAPI")
import onetick.cloud as otc

from datetime import datetime
import argparse


def main():
    username=os.environ['OTC_USER']
    password=os.environ['OTC_PASSWORD']
	
    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--params_file',required=True,help='file containing otq_params |-delimited string')
    args=parser.parse_args()
    
    with open(args.params_file,'r') as f:
        line = f.read()


#   louis.lovas@onetick.com - "otq":"1/8/otq/f6f410dc-308d-4275-8765-9a85fd12a6b6.otq::Futures_Interval"
#   bassel@area11.io        - "otq":"85/297/otq/2d5cbe9d-5b5a-472b-8603-a14fc4ec8382.otq::Futures_Interval"
#   rohan@area11.io         - "otq":"85/298/otq/32738a97-74c9-4435-934b-46929a2f34d0.otq::Futures_Interval"


        response = otc.run('1/8/otq/f6f410dc-308d-4275-8765-9a85fd12a6b6.otq::Futures_Interval', #Interval Query Futures Markets
                   otq_params='SYMBOL_LIST=' + line,
                   url=otc.CLOUD2_URL,
                   timezone='GMT',                     # ignored
                   start_date=datetime(2022,3,1,0,0),  # start/end are not used for Interval query, dates on per-symbol
                   end_date=datetime(2022,3,2,0,0),
                   response='csv',
                   compression=True,
                   user=username,password=password
                  )
        response.write('C:\\Users\\rohha\\Documents\\pythonAPI\\Interval_results_by_Instrument.csv.gz',tofile=True)

main()

