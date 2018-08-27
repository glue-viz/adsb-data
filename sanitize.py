import os
import h5py
import glob
import numpy as np
from astropy.time import Time

f = h5py.File('adsb_data.hdf5', 'r')
f_new = h5py.File('adsb_data_v1.0.hdf5', 'w')

# Boston airport
lat0 = 42.3656
lon0 = -71.0096

R_earth = 6371

# Read in temperatures
times, temperatures = [], []
for filename in sorted(glob.glob(os.path.join('raw', '?????_temp.log'))):
    for line in open(filename):
        cols = line.strip().split()
        times.append(float(cols[0].replace('\x00', '')))
        temperatures.append(float(cols[1]))
times = np.array(times)
temperatures = np.array(temperatures)

# Interpolate to measured times
temp_interpolated = np.interp(f['timestamp'].value, times, temperatures, left=np.nan, right=np.nan)

# Find x/y positions
lon_off = f['longitude'].value - lon0
lat_off = f['latitude'].value - lat0
x = np.radians(lon_off) * np.cos(np.radians(lat0)) * R_earth
y = np.radians(lat_off) * R_earth

# Compute date/time from timestamp
date = Time(f['timestamp'].value, format='unix').plot_date
date -= np.floor(np.min(date))
time = date % 1
time = (time * 24 + 18.8) % 24

r_off = np.hypot(x, y)
keep = r_off < 1000

f_new.create_dataset(name='x', data=x[keep])
f_new.create_dataset(name='y', data=y[keep])
f_new.create_dataset(name='date', data=date[keep])
f_new.create_dataset(name='time', data=time[keep])

for att in f:
    f_new.create_dataset(name=att, data=f[att].value[keep])

f_new.create_dataset(name='pi_temperature', data=temp_interpolated[keep])

f.close()
