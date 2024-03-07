import os
import netCDF4
import logging
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter
from commons.time_interval import TimeInterval 
from datetime import datetime, timedelta
from layer_integral import coastline
import pickle
import matplotlib.cm as cm
import matplotlib.colors as mcolors



logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.info("Start")

################### EDIT PART

varname = "Water body nitrate"
varname_model="N3n"


ADMITTED_PLATFORMS=['man-powered small boat (3A)','offshore structure (16)','ship (30)',\
'research vessel (31)','vessel of opportunity (32)', 'self-propelled small boat (33)',\
'vessel at fixed position (34)','vessel of opportunity on fixed route (35)', \
'fishing vessel (36)', 'self-propelled boat (37)','man-powered boat (38)',\
'naval vessel (39)','moored surface buoy (41)','subsurface mooring (43)',\
'fixed subsurface vertical profiler (45)','mooring (48)']


datadir = "/g100_scratch/userexternal/vdibiagi/EMODnet_2022/from1999/profiles/" 
datafile = os.path.join(datadir, "emodnet-chem_2022_eutrophication_MediterraneanSea_Eutrophication_Med_profiles_2022_unrestricted_8XdVNSe1.nc")

yearStart='1999' # NB La selezione sui tempi va spostata dopo, nel bit.sea, tuttavia qui cmq utile in futuro, quando useremo il dataset dal 1980, per i check grafici su 1999-2022
yearEnd ='2022'
###################

# Time interval in which the user is interested in (Dataset can be longer in time) 
TI= TimeInterval(starttime=yearStart+"0101", endtime=yearEnd+"1231", dateformat='%Y%m%d')


# Open the netCDF file
with netCDF4.Dataset(datafile, "r") as nc:
    
    # Get the dimensions: number of stations and number of samples
    nstations = int(nc.dimensions["N_STATIONS"].size)
    nsamples = int(nc.dimensions["N_SAMPLES"].size)

    logger.info(f"Whole dataset including {nstations} stations")
    
    # Read coordinates
    obsproflon = nc.get_variables_by_attributes(standard_name="longitude")[0][:]
    obsproflat = nc.get_variables_by_attributes(standard_name="latitude")[0][:]
    obsproftime = nc.get_variables_by_attributes(standard_name="time")[0][:] 
    # Decimal Gregorian Days of the station, e.g.  days since 1999-01-01 00:00:00 UTC, Relative Gregorian Days with decimal part
    # It has dimension equal to nstations
    ncvar_z = nc.get_variables_by_attributes(long_name="Depth")[0][:]

    try:
        ncvar = nc.get_variables_by_attributes(long_name=varname)[0][:]
        ncvarQF = nc.get_variables_by_attributes(long_name='Quality flag of '+varname)[0][:] 
        obsp = nc.get_variables_by_attributes(long_name='Platform type')[0][:]
    except IndexError:
        logger.warning(f"Cannot find a variable named {varname}")

    # This extract the Start time of the dataset, that is a reference for times included in obsproftime 
    EPOCH = datetime.strptime(nc.variables['date_time'].units[11:30],'%Y-%m-%d %H:%M:%S') 

    #List of all Platform Type originally included in the Dataset, before selections of variable, specific time, platforms etc
    PlatformTypes = [b''.join(ob).decode() for ob in obsp.data]
    count_PlatformTypes=Counter(PlatformTypes)
    print("Platform Types in the whole downloaded Dataset: ")
    print(count_PlatformTypes.keys())
    PlatformData = np.array(list(count_PlatformTypes.items()))     
    namePlat=PlatformData[:,0] 
    nPlatData=PlatformData[:,1].astype(int)      
   
    GOODVALUES=np.zeros(nstations).astype(int)  

    # Loop on the stations 
    istart = 0
    LON = []
    LAT = []
    TIME = []

    for ii in range(0, nstations):
        
        # Load the variable profile
        profilevar = ncvar[ii,:]

        # Load the QF profile
        QFvar = ncvarQF[ii,:]

        # Load the Platform type (to exclude e.g. Argo floats)
        obsplatvar = PlatformTypes[ii]
        
        # Extract only the good values 
        # as those which are 
        # 1) not masked (here mask==True means already masked, i.e. missing) 
        # 2) with QC = 49 or QC = 50 -> fixme da generalizzare attraverso lettura attributi, qui selezione hard coded 
        # 3) that are in the ADMITTED_PLATFORMS list
        # 4) that are in the requested period TI (e.g. year)       
     
        delta = timedelta(days=obsproftime[ii]) 
        # For 3 or more conditions, use np.logical_and.reduce and pass a list of masks
        MASK = np.logical_and.reduce([np.logical_or(QFvar.data==49,QFvar.data==50), profilevar.mask == False, np.full(nsamples, obsplatvar in ADMITTED_PLATFORMS), np.full(nsamples, TI.contains(EPOCH+delta))])
        goodvalues = np.where(MASK)[0] 

        # Count number of good values (i.e., values passing the selections)
        ngood = len(goodvalues)
        GOODVALUES[ii]=ngood
        if ngood:
           LON.append(obsproflon[ii])
           LAT.append(obsproflat[ii])
           TIME.append((EPOCH + delta).strftime('%Y-%m-%d'))
           # information on "profile" objects
        iend = istart + ngood 
        # ... parte eliminata
        istart = iend
        

logger.info(f"Final size of the vectors: {iend}")
nProfiles_def=GOODVALUES[GOODVALUES>0].shape[0]
print("nprofiles passing selection: ",nProfiles_def) #numero di stazioni (ovvero profili) con almeno 1 valore buono


############ for colorbar of years
def colorbar_index(firstL, ncolors, cmap):
    cmap = cmap_discretize(cmap, ncolors)
    mappable = cm.ScalarMappable(cmap=cmap)
    mappable.set_array([])
    mappable.set_clim(-0.5, ncolors+0.5)
    colorbar = plt.colorbar(mappable)
    colorbar.set_ticks(np.linspace(0, ncolors, ncolors))
    colorbar.set_ticklabels(range(firstL,ncolors+firstL))

def cmap_discretize(cmap, N):
    """Return a discrete colormap from the continuous colormap cmap.

        cmap: colormap instance, eg. cm.jet. 
        N: number of colors.

    Example
        x = resize(arange(100), (5,100))
        djet = cmap_discretize(cm.jet, 5)
        imshow(x, cmap=djet)
    """

    if type(cmap) == str:
        cmap = plt.get_cmap(cmap)
    colors_i = np.concatenate((np.linspace(0, 1., N), (0.,0.,0.,0.)))
    colors_rgba = cmap(colors_i)
    indices = np.linspace(0, 1., N+1)
    cdict = {}
    for ki,key in enumerate(('red','green','blue')):
        cdict[key] = [ (indices[i], colors_rgba[i-1,ki], colors_rgba[i,ki])
                       for i in range(N+1) ]
    # Return colormap object.
    return mcolors.LinearSegmentedColormap(cmap.name + "_%d"%N, cdict, 1024)

########### end colorbar of years

# Plot of profiles after selections 
clon,clat = coastline.get()        
fig = plt.figure(figsize=(20,10))
ax = plt.subplot(111)
cmap = plt.cm.get_cmap('viridis', int(yearEnd)-int(yearStart)+1)
scpl=ax.scatter(LON, LAT, c=[int(r[:4]) for r in TIME], s=10,cmap=cmap)
ax.plot(clon,clat, color='#000000',linewidth=0.5)
ax.set_xlim([-6, 36])
ax.set_ylim([30, 46])
ax.set_xlabel('Lon').set_fontsize(14)
ax.set_ylabel('Lat').set_fontsize(14)
for tick in ax.xaxis.get_major_ticks():
                tick.label.set_fontsize(14) 
for tick in ax.yaxis.get_major_ticks():
                tick.label.set_fontsize(14) 
ax.set_title(f"Observations of {varname}",fontsize=18)
ax.text(-5,33,"number of profiles = "+str(nProfiles_def),horizontalalignment='left',verticalalignment='center',fontsize=18, color='black')
ax.text(-5,32,"number of values = "+str(iend),horizontalalignment='left',verticalalignment='center',fontsize=18, color='black')
colorbar_index(int(yearStart),ncolors=int(yearEnd)-int(yearStart)+1, cmap=cmap)    
title_fig='PLOTS/profiles/prof_'+varname_model+'_LON_LAT_'+yearStart+'_'+yearEnd+'.png'     
fig.savefig(title_fig) 
plt.close('all')

#salvo il file con le posizioni dei profili per variabile
with open('PKL/profiles/Coords_'+varname_model+'.pkl', 'wb') as f:  
    pickle.dump([LON, LAT, TIME], f) 

#Getting back the objects: 
#with open('filename.pkl','rb') as f:  
#    obj0, obj1, obj2 = pickle.load(f) 


# TO DO:
#devo aggiungere la generalizzazione delle quality flag (qui hard coded) 
#poi la lettura simultanea delle altre variabili 
#la riorganizzazione del dataset"
#e infine la scrittura nel formato desiderato





