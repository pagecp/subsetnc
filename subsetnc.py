import shutil
import sys
import os
import cftime
import numpy as np
import pandas as pd
import xarray as xr
import datetime

input_file = sys.argv[1]

# No spatial subsetting if one value is -9999 for a dimension
minlat = float(sys.argv[2])
maxlat = float(sys.argv[3])
minlon = float(sys.argv[4])
maxlon = float(sys.argv[5])
if minlat == -9999 or maxlat == -9999:
  minlat = -9999
  maxlat = -9999
if minlon == -9999 or maxlon == -9999:
  minlon = -9999
  maxlon = -9999

# Optional time subsetting
if len(sys.argv) > 6 :
  period_start_time = sys.argv[6]
  period_end_time = sys.argv[7]

  # 2 formats possible: YYYYMMDD or YYYY-MM-DD
  if period_start_time[4] == '-' and period_start_time[7] == '-':
    year_start = period_start_time[0:4]
    month_start = period_start_time[5:7]
    day_start = period_start_time[8:10]
  else:
    year_start = period_start_time[0:4]
    month_start = period_start_time[4:6]
    day_start = period_start_time[6:8]

  starttime = year_start+"-"+month_start+"-"+day_start
  start = datetime.datetime(int(year_start), int(month_start), int(day_start))

  if period_end_time[4] == '-' and period_end_time[7] == '-':
    year_end = period_end_time[0:4]
    month_end = period_end_time[5:7]
    day_end = period_end_time[8:10]
  else:
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

   f = f.rstrip()
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
     if 'time' in dset.keys():
       year_startf = dset.time.dt.year[0]
       month_startf = dset.time.dt.month[0]
       day_startf = dset.time.dt.day[0]
       startf = datetime.datetime(int(year_startf), int(month_startf), int(day_startf))
       year_endf = dset.time.dt.year[-1:]
       month_endf = dset.time.dt.month[-1:]
       day_endf = dset.time.dt.day[-1:]
       endf = datetime.datetime(int(year_endf), int(month_endf), int(day_endf))
       if not ((start >= startf and start <= endf) or (end >= startf and end <= endf)) :
         process = False
       else :
         if start > startf :
           fstart_year = year_start
           fstart_month = month_start
           fstart_day = day_start
         else :
           fstart_year = dset.time.dt.strftime("%Y")[0].values
           fstart_month = dset.time.dt.strftime("%m")[0].values
           fstart_day = dset.time.dt.strftime("%d")[0].values
         if end < endf :
           fend_year = year_end
           fend_month = month_end
           fend_day = day_end
         else :
           fend_year = dset.time.dt.strftime("%Y")[-1].values
           fend_month = dset.time.dt.strftime("%m")[-1].values
           fend_day = dset.time.dt.strftime("%d")[-1].values
         fbs = fb.strip('.nc')
         outf = fbs + "_subset_" + fstart_year + fstart_month + fstart_day + "-" + fend_year + fend_month + fend_day + ".nc"
     else:
       period_start_time = -1
       fbs = fb.strip('.nc')
       outf = fbs + "_subset.nc"
     try:
       del dset.attrs['_NCProperties']
     except:
       pass

   if process == True :
     if minlon > maxlon or minlon < 0:
       if period_start_time == -1 and minlat != -9999 :
         print("Subsetting latitude")
         dset = dset.sel(lat=slice(minlat,maxlat))
       elif period_start_time != -1 and minlat != -9999 :
         print("Subsetting time and latitude")
         dset = dset.sel(time=slice(starttime,endtime), lat=slice(minlat,maxlat))
       elif period_start_time != -1 and minlat == -9999 :
         print("Subsetting time")
         dset = dset.sel(time=slice(starttime,endtime))
     else:
       if period_start_time != -1 and minlon != -9999 and minlat != -9999 :
         print("Subsetting time, longitude and latitude")
         dset = dset.sel(time=slice(starttime,endtime), lon=slice(minlon,maxlon), lat=slice(minlat,maxlat))
       elif period_start_time != -1 and minlon != -9999 and minlat == -9999 :
         print("Subsetting time and longitude")
         dset = dset.sel(time=slice(starttime,endtime), lon=slice(minlon,maxlon))
       elif period_start_time != -1 and minlon == -9999 and minlat != -9999 :
         print("Subsetting longitude and latitude")
         dset = dset.sel(time=slice(starttime,endtime), lat=slice(minlat,maxlat))
       elif period_start_time == -1 and minlon != -9999 and minlat != -9999 :
         print("Subsetting longitude and latitude")
         dset = dset.sel(lon=slice(minlon,maxlon), lat=slice(minlat,maxlat))
       elif period_start_time == -1 and minlon != -9999 and minlat == -9999 :
         print("Subsetting longitude")
         dset = dset.sel(lon=slice(minlon,maxlon))
       elif period_start_time == -1 and minlon != -9999 and minlat == -9999 :
         print("Subsetting latitude")
         dset = dset.sel(lat=slice(minlat,maxlat))
       elif period_start_time != -1 and minlon == -9999 and minlat == -9999 :
         print("Subsetting time")
         dset = dset.sel(time=slice(starttime,endtime))
       else :
         sys.exit("Error subsetting selection...")

     print("Saving to: "+"results/"+outf)
     dims = dset.dims
     dimsf = {k: v for k, v in dims.items() if k.startswith('lat') or k.startswith('lon')}
     enc = dict(dimsf)
     enc = dict.fromkeys(enc, {'_FillValue': None})
     tdimsf = {k: v for k, v in dims.items() if k == 'time'}
     if len(tdimsf) > 0:
       tunits = dset.time.encoding['units']
       tenc = dict(tdimsf)
       tenc = dict.fromkeys(tenc, {'_FillValue': None, 'units': tunits})
       enc.update(tenc)

     varsd = dset.data_vars
     varsdf = {k: v for k, v in varsd.items() if k.endswith('_bnds') or k.endswith('_bounds')}
     if len(varsdf) > 0:
       venc = dict(varsdf)
       venc = dict.fromkeys(venc, {'_FillValue': None})
       enc.update(venc)

     if period_start_time == -1 :
       dset.to_netcdf(path="results/"+outf, mode='w', format='NETCDF4', engine='netcdf4', encoding=enc)
     else:
       files.append("results/"+outf)
       dset.to_netcdf(path="results/"+outf, mode='w', format='NETCDF4', unlimited_dims='time', engine='netcdf4', encoding=enc)
   else :
     print("Not processing file because time range is outside time period requested.")
       
   dset.close()
   del dset

# Reorder longitudes if needed, and subset longitudes in that specific case differently (need to do it on local file for reasonable performance)
   if process == True :
     if (minlon > maxlon or minlon < 0) and maxlon != -9999 :
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
         dsetl.to_netcdf(path="results/tmp"+outf, mode='w', format='NETCDF4', unlimited_dims='time', engine='netcdf4', encoding=enc)
       dsetl.close()
       del dsetl
       os.rename("results/tmp"+outf, "results/"+outf)
