import onetick.cloud as otc
import os
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

#  louis.lovas@onetick.com - "otq":"1/8/otq/aac53df8-a081-4848-a87a-ba9d6b5862b5.otq::Futures_PointInTime"
#  bassel@area11.io        - "otq":"85/297/otq/3537cb9c-aaea-4b8e-841b-64cc1d2c721b.otq::Futures_PointInTime"
#  rohan@area11.io         - "otq":"85/298/otq/a1cd6085-ce95-43a6-82ab-f44b03783b68.otq::Futures_PointInTime"


        response = otc.run('1/8/otq/aac53df8-a081-4848-a87a-ba9d6b5862b5.otq::Futures_PointInTime', #PointInTime Query - Futures
                   otq_params='SYMBOL_LIST=' + line,
                   url=otc.CLOUD2_URL,
                   timezone='GMT',                     # ignored
                   start_date=datetime(2022,1,1,0,0),
                   end_date=datetime(2022,12,1,0,0),   # start/end are not used for Interval query, dates on per-symbol
                   response='csv',
                   compression=True,
                   user=username,password=password
                  )
        response.write('C:/temp/PointInTime_results_by_Instrument.csv.gz',tofile=True)

main()

