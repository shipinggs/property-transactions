import pandas as pd
import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

DATA_PATH = 'data/'
FILE_1 = '20190526_PMI_Resi_Transaction.csv'
FILE_2 = '20190630_PMI_Resi_Transaction.csv'

def markDuplicates( df ):
    count = 1
    while sum( df.duplicated() ):
        logging.info( 'File 1: Round %d, %d duplicates' % ( count, sum( df.duplicated() ) ) )
        df[ count ] = df.duplicated()
        count += 1

def main():
    
    one = pd.read_csv( DATA_PATH+FILE_1 )
    two = pd.read_csv( DATA_PATH+FILE_2 )

    columns = one.columns

    markDuplicates( one )
    markDuplicates( two )

    merged = pd.concat( [ one, two ] ).drop_duplicates()
    merged = merged[ columns ]
    merged.to_csv( DATA_PATH + 'merged.csv' , index=False )
    print( 'Merged: File 1 has %d rows, File 2 has %d rows, Merged file has %d rows.' % ( len( one.index ), len( two.index ), len( merged.index ) ) )


if __name__ == '__main__':
    main()