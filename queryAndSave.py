import requests
import json
import csv
import datetime
import logging
import sys
import re
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

BATCHES = 4
ACCESS_KEY = 'e5cd04db-880c-494a-8c8b-d51b18080706'
TOKEN_URL = 'https://www.ura.gov.sg/uraDataService/getTokenForm'
DATA_URL = 'https://www.ura.gov.sg/uraDataService/invokeUraDS'
SERVICE = 'PMI_Resi_Transaction'
HEADERS = [ 'project', 'marketSegment', 'street', 'contractDate', 'areaSqm', 'areaSqft', 'price', 'psf', 'propertyType', 'typeOfArea', 'tenure', 'floorRange', 'typeOfSale', 'district', 'noOfUnits' ]

def getToken( accessKey ):
    ''' get URA token, which expires daily, given the constant accessKey (valid for 1 year) issued '''
    tokenRes = requests.get( TOKEN_URL, params = { 'txtAcessKey': accessKey } )
    tokenSubres = tokenRes.text[ tokenRes.text.find( 'value=' ): ]
    token = re.match( r'value=\"([0-9a-zA-Z-!@$%^&*()_+|~=`{}\[\]:\";\'<>?,.\/]*)\">', tokenSubres ).groups()[0]
    logging.info( "Token obtained: %s" % token )
    return token

def main():
    today = datetime.date.today().strftime( '%Y%m%d' )
    token = getToken( ACCESS_KEY )

    allProjs = []
    for i in range( 1, BATCHES+1 ):
        logging.info( 'Querying data for batch %d...' % i )
        response = requests.get( DATA_URL, headers = { 'AccessKey': ACCESS_KEY, 'Token': token }, params  = { 'service': SERVICE, 'batch': i } )
        if response:
            logging.info( response.headers )
            result = response.json()[ 'Result' ]
            allProjs.extend( result )

    outpath = 'data/%s_%s.csv' % ( today, SERVICE )
    count = 0
    with open( outpath, 'w+' ) as outfile:
        writer = csv.writer( outfile )
        writer.writerow( HEADERS )
        for proj in allProjs:
            for txn in proj[ 'transaction' ]:
                writer.writerow([
                    proj[ 'project' ],
                    proj[ 'marketSegment' ],
                    proj[ 'street' ],
                    '01/%s/20%s' % ( txn[ 'contractDate' ][0:-2], txn[ 'contractDate' ][-2:] ),
                    txn[ 'area' ],
                    float( txn[ 'area' ] )*10.7639,
                    txn[ 'price' ],
                    float( txn[ 'price' ] )/ ( float( txn[ 'area' ] )*10.7639 ),
                    txn[ 'propertyType' ],
                    txn[ 'typeOfArea' ],
                    txn[ 'tenure' ],
                    txn[ 'floorRange' ],
                    txn[ 'typeOfSale' ],
                    txn[ 'district' ],
                    txn[ 'noOfUnits' ],
                ])
                count += 1
         
        logging.info( 'Wrote %s rows into %s.' % ( count, outpath ) )

    

if __name__ == '__main__':
    main()