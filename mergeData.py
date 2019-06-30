import pandas as pd

DATA_PATH = 'data/'
FILE_1 = '20190526_PMI_Resi_Transaction.csv'
FILE_2 = '20190630_PMI_Resi_Transaction.csv'

def main():
    
    one = pd.read_csv( DATA_PATH+FILE_1 )
    two = pd.read_csv( DATA_PATH+FILE_2 )

    columns = one.columns

    count = 1
    while sum( one.duplicated() ):
        print( 'File 1: Round %d, %d duplicates' % ( count, sum( one.duplicated() ) ) )
        one[ count ] = one.duplicated()
        count += 1

    count = 1
    while sum( two.duplicated() ):
        print( 'File 2: Round %d, %d duplicates' % ( count, sum( two.duplicated() ) ) )
        two[ count ] = two.duplicated()
        count += 1

    merged = pd.concat( [ one, two ] ).drop_duplicates()
    merged = merged[ columns ]
    merged.to_csv( DATA_PATH + 'merged.csv' , index=False )
    print( 'Merged: File 1 has %d rows, File 2 has %d rows, Merged file has %d rows.' % ( len( one.index ), len( two.index ), len( merged.index ) ) )


if __name__ == '__main__':
    main()