import numpy
import h5py

if __name__ == '__main__':
    numSample = 200
    infilepath = '/global/cscratch1/sd/wss/mjo/Precipitation_rate_1979_to_1983.h5'
    outfilepath = '/global/cscratch1/sd/wss/mjo/Precipitation_rate_1979_to_1983_subset.h5'
    
    # read data
    fin = h5py.File(infilepath, 'r')
    arr = fin["rows"][0:numSample]
    
    # write the subset to file
    fx=h5py.File(outfilepath, 'w')
    dset = fx.create_dataset('rows', data=arr)
    