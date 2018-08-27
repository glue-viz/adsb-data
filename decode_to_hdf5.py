import glob

from collections import defaultdict, Counter

import h5py
import numpy as np
import pyModeS as pms
from astropy.table import Table

counter = Counter()

lon_lat_cache = defaultdict(dict)
last_row = defaultdict(dict)

COLNAMES = ('timestamp', 'aircraft', 'callsign', 'longitude', 'latitude',
            'altitude', 'ground_speed', 'air_speed', 'heading',
            'vertical_rate')

COLTYPES = (float, 'S6', 'S8', float, float, float, float, float, float, float)

EMPTY_ROW = {name: np.ma.masked for name in COLNAMES}


def process(filenames, output):

    rows = []

    for filename in filenames:

        print("Processing {0}".format(filename))

        for line in open(filename):

            timestamp, hex_data = line.strip().split()

            try:
                timestamp = float(timestamp)
            except Exception:
                print('Issue in parsing timestamp')
                try:
                    timestamp = float(timestamp.replace('\x00', ''))
                    print('(managed to fix: {0})'.format(timestamp))
                except Exception:
                    continue

            bin_data = "{0:0112b}".format(int(hex_data, 16))

            # ItcO aircraft address
            aircraft = "{0:6x}".format(int(bin_data[8:32], 2))

            # Type code
            tc = int(bin_data[32:37], 2)

            if len(last_row[aircraft]) == 0:
                last_row[aircraft] = EMPTY_ROW.copy()

            row = last_row[aircraft].copy()
            row['timestamp'] = timestamp
            row['aircraft'] = aircraft

            write = False

            if tc <= 4:  # Aircraft identification

                try:
                    row['callsign'] = pms.adsb.callsign(hex_data).replace('_', '')
                except:
                    pass

            elif 9 <= tc <= 18:  # Airborne position (Baro Alt)

                if bin_data[53] == '0':
                    lon_lat_cache[aircraft]['even'] = (timestamp, hex_data)
                else:
                    lon_lat_cache[aircraft]['odd'] = (timestamp, hex_data)

                if 'even' in lon_lat_cache[aircraft] and 'odd' in lon_lat_cache[aircraft]:

                    t_e, msg_e = lon_lat_cache[aircraft]['even']
                    t_o, msg_o = lon_lat_cache[aircraft]['odd']

                    if abs(t_e - t_o) < 5:

                        result = pms.adsb.position(msg_e, msg_o, t_e, t_o)

                        if result is not None:
                            lat, lon = pms.adsb.position(msg_e, msg_o, t_e, t_o)
                            alt_e = pms.adsb.altitude(msg_e) * 0.3048 / 1000.

                            row['longitude'] = lon
                            row['latitude'] = lat
                            row['altitude'] = alt_e

                            write = True

            elif tc == 19:  # Airborne velocities

                velocity_info = pms.adsb.velocity(hex_data)

                if velocity_info is None:
                    continue

                speed, heading, vertical_rate, speed_type = velocity_info

                if speed_type == 'GS':
                    row['ground_speed'] = speed
                else:
                    row['air_speed'] = speed
                row['heading'] = heading
                row['vertical_rate'] = vertical_rate

            last_row[aircraft] = row

            if write:
                rows.append(row)

    t = Table(names=COLNAMES,
              dtype=COLTYPES,
              masked=True,
              rows=rows)

    f = h5py.File(output, 'w')
    for colname in COLNAMES:
        f.create_dataset(name=colname, data=t[colname])

    # t.write(output, overwrite=True)

if __name__ == "__main__":
    process(sorted(glob.glob('raw/?????')), 'adsb_data.hdf5')
