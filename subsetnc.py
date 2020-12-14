import shutil
import sys
import os
import cftime
import numpy as np
import pandas as pd
import xarray as xr
import datetime

input_file = sys.argv[1]
minlat = float(sys.argv[2])
maxlat = float(sys.argv[3])
minlon = float(sys.argv[4])
maxlon = float(sys.argv[5])
if len(sys.argv) > 6 :
  period_start_time = sys.argv[6]
  period_end_time = sys.argv[7]

  year_start = period_start_time[0:4]
  month_start = period_start_time[4:6]
  day_start = period_start_time[6:8]
  starttime = year_start+"-"+month_start+"-"+day_start
  start = datetime.datetime(int(year_start), int(month_start), int(day_start))

  year_end = period_end_time[0:4]
  month_end = period_end_time[4:6]
  day_end = period_end_time[6:8]
  endtime = year_end+"-"+month_end+"-"+day_end
  end = datetime.datetime(int(year_end), int(month_end), int(day_end))

else :
  period_start_time = -1
  period_end_time = -1

f = open(input_file,"r")
allfiles=f.readlines()

if not( os.path.isdir('results')):
  os.mkdir('results')

files = []
for f in allfiles:

  f = f.strip()
  print(f)
  fb = f.rsplit('/', 1)[-1]

  process = True

  if period_start_time == -1 :
    dset = xr.open_dataset(f, mask_and_scale=False, decode_coords=True)
    fbs = fb.strip('.nc')
    outf = fbs + "_subset.nc"
  else :
    try:
      dset = xr.open_dataset(f, chunks={'time': '100MB'}, mask_and_scale=False, decode_coords=True, decode_times=True, use_cftime=True)
    except:
      dset = xr.open_dataset(f, mask_and_scale=False, decode_coords=True, decode_times=True, use_cftime=True)
    year_startf = dset.time.dt.year[0].data
    month_startf = dset.time.dt.month[0].data
    day_startf = dset.time.dt.day[0].data
    startf = datetime.datetime(year_startf, month_startf, day_startf)
    year_endf = dset.time.dt.year[-1:].data[0]
    month_endf = dset.time.dt.month[-1:].data[0]
    day_endf = dset.time.dt.day[-1:].data[0]
    endf = datetime.datetime(year_endf, month_endf, day_endf)
    if not ((startf >= start and startf <= end) or (endf >= start and endf <= end)) :
      process = False
    else :
      if start > startf :
        file_start = start
        fstart_year = year_start
        fstart_month = month_start
        fstart_day = day_start
      else :
        file_start = startf
        fstart_year = str(year_startf)
        fstart_month = str(month_startf)
        fstart_day = str(day_startf)
      if end < endf :
        file_end = end
        fend_year = year_end
        fend_month = month_end
        fend_day = day_end
      else :
        file_end = endf
        fend_year = str(year_endf)
        fend_month = str(month_endf)
        fend_day = str(day_endf)
      fbs = fb.strip('.nc')
      outf = fbs + "_subset_" + fstart_year + fstart_month + fstart_day + "-" + fend_year + fend_month + fend_day + ".nc"
  try:
    del dset.attrs['_NCProperties']
  except:
    pass

  if process == True :
    if minlon > maxlon or minlon < 0:
      if period_start_time == -1 :
        dset = dset.sel(lat=slice(minlat,maxlat))
      else :
        dset = dset.sel(time=slice(starttime,endtime), lat=slice(minlat,maxlat))
    else:
      if period_start_time == -1 :
        dset = dset.sel(lon=slice(minlon,maxlon), lat=slice(minlat,maxlat))
      else :
        dset = dset.sel(time=slice(starttime,endtime), lon=slice(minlon,maxlon), lat=slice(minlat,maxlat))

    print("Saving to: "+"results/"+outf)
    dims = dset.dims
    dimsf = {k: v for k, v in dims.items() if k.startswith('lat') or k.startswith('lon') or k.startswith('time')}
    enc = dict(dimsf)
    enc = dict.fromkeys(enc, {'_FillValue': None})

    if period_start_time == -1 :
      dset.to_netcdf(path="results/"+outf, mode='w', format='NETCDF4', engine='netcdf4', encoding=enc)
    else:
      files.append("results/"+outf)
      dset.to_netcdf(path="results/"+outf, mode='w', format='NETCDF4', unlimited_dims='time', engine='netcdf4', encoding=enc)
      tunits = dset.time.encoding['units']
  else :
    print("Not processing file because time range is outside time period requested.")

  dset.close()
  del dset

# Reorder longitudes if needed, and subset longitudes in that specific case differently (need to do it on local file for reasonable performance)
  if process == True :
    if minlon > maxlon or minlon < 0:
      print("Subsetting for non-contiguous longitude")
      if period_start_time == -1 :
        dsetl = xr.open_dataset("results/"+outf, mask_and_scale=False, decode_coords=True)
      else :
        try:
          dsetl = xr.open_dataset("results/"+outf, chunks={'time': '100MB'}, mask_and_scale=False, decode_coords=True, decode_times=True, use_cftime=True)
        except:
          dsetl = xr.open_dataset("results/"+outf, mask_and_scale=False, decode_coords=True, decode_times=True, use_cftime=True)
      saveattrs = dsetl.lon.attrs
      dsetl = dsetl.assign_coords(lon=(((dsetl.lon + 180) % 360) - 180)).roll(lon=(dsetl.dims['lon'] // 2), roll_coords=True)
      if minlon >= 180:
        minlon = minlon - 360
        if maxlon >= 180:
          maxlon = maxlon - 360
      dsetl = dsetl.sel(lon=slice(minlon,maxlon))
      dsetl.lon.attrs = saveattrs
      if period_start_time == -1 :
        dsetl.to_netcdf(path="results/tmp"+outf, mode='w', format='NETCDF4', engine='netcdf4', encoding=enc)
      else :
        dsetl.time.encoding['units'] = tunits
        dsetl.to_netcdf(path="results/tmp"+outf, mode='w', format='NETCDF4', unlimited_dims='time', engine='netcdf4', encoding=enc)
      dsetl.close()
      del dsetl
      os.rename("results/tmp"+outf, "results/"+outf)
