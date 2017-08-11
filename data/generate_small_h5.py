import numpy
import h5py

def create_data(filepath):
    fx=h5py.File(filepath,'w')
    arr=numpy.arange(500).reshape(50, 10)
    dset = fx.create_dataset('rows', data=arr)
    fx.close()

def read_data(filepath):
    fin = h5py.File(filepath, 'r')
    data = fin["rows"]
    print('The data have ' + str(data.shape[0]) + ' rows and ' + str(data.shape[1]) + ' columns.')
    for i in range(data.shape[0]):
        print(data[i])
    

if __name__ == '__main__':
    filepath = './small_data.h5'
    create_data(filepath)
    read_data(filepath)
    
    
    
    
    
    
    
    