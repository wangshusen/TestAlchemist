import numpy
import h5py
import csv

def h5_to_csv(infilepath, outfilepath):
    fin = h5py.File(filepath, 'r')
    data = fin["rows"]
    num_rows = data.shape[0]
    print('The h5 data have ' + str(num_rows) + ' rows and ' + str(data.shape[1]) + ' columns.')
    
    writer = csv.writer(open(outfilepath, 'w'))

    for i in range(num_rows):
        writer.writerow(data[i])
        if i % 50 == 0:
            print(str(i+1) + " rows has been processed!")
    

def test_csv(outfilepath):
    reader = csv.reader(open(outfilepath, 'r'))
    count = 0
    for row in reader:
        print(row[0:10])
        count += 1
        if count > 100:
            break
    
    

if __name__ == '__main__':
    infilepath = '/global/cscratch1/sd/wss/mjo/Precipitation_rate_1979_to_1983.h5'
    outfilepath = '/global/cscratch1/sd/wss/mjo/Precipitation_rate_1979_to_1983.csv'
    
    h5_to_csv(infilepath, outfilepath)
    
    
    
    

    
    