import numpy as np
import pandas as pd
from datetime import datetime
import io
import json
import requests
import zipfile

class CosmosData:
    """
    A simple class to extract data for COSMOS-UK site.

    Parameters
    ----------
    years: vector of years in integers
    site: COSMOS-UK site code to extract data

    Examples
    --------

    >>> CosmosData(years=[2018,2019],site='CHIMN')

    """
    def __init__(self, years, site, method='api'):
        self.years = years
        self.site = site
        #self.driving_data = pd.DataFrame()
        
        # order is important!
        self.cosmos_data =  self.get_cosmos_data()
        self.PE_data =  self.get_PE_data()
        self.PREC_data =  self.get_PREC_data()
        self.MODIS_data =  self.get_MODIS_data()
        self.LAI_data =  self.get_LAI_data()
        self.driving_data =  pd.concat([self.PE_data,self.PREC_data,self.LAI_data], axis=1)
        self.api_data = self.get_cosmos_api_data()
        
        self.driving_data =  pd.concat([self.get_cosmos_api_single('pe'),self.get_cosmos_api_single('precip'),self.LAI_data], axis=1)
        self.atmo_data = self.write_atmo_data()

        
#     def __str__(self):
#         return f"{self.site}"

    def get_cosmos_api_data(self):
        
        def format_datetime(dt):
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        def get_api_response(url, csv=False):
            """ Helper function to send request to API and get the response 

            :param str url: The URL of the API request
            :param bool csv: Whether this is a CSV request. Default False. 
            :return: API response
            """ 
            # Send request and read response
            print(url)
            response = requests.get(url)

            if csv:
                return response
            else:
                # Decode from JSON to Python dictionary
                return json.loads(response.content)
        
        def read_json_collection_data(json_response):
            """ Wrangle the response JSON from a COSMOS-API data collection request into a more usable format - in this case a Pandas Dataframe

            :param dict json_response: The JSON response dictionary returned from a COSMOS-API data collection request
            :return: Dataframe of data
            :rtype: pd.DataFrame
            """
            # The response is a list of dictionaries, one for each requested site

            # You can choose how you want to build your dataframes.  Here, I'm just loading all stations into one big dataframe.  
            # But you could modify this for your own use cases.  For example you might want to build a dictionary of {site_id: dataframe} 
            # to keep site data separate, etc.
            master_df = pd.DataFrame()

            for site_data in resp['coverages']:
                # Read the site ID
                site_id = site_data['dct:identifier']

                # Read the time stamps of each data point
                time_values = pd.DatetimeIndex(site_data['domain']['axes']['t']['values'])

                # Now read the values for each requested parameter at each of the time stamps
                param_values = {param_name: param_data['values'] for param_name, param_data in site_data['ranges'].items()}

                # And put everything into a dataframe
                site_df = pd.DataFrame.from_dict(param_values)
                site_df['datetime'] = time_values
                site_df['site_id'] = site_id

                site_df = site_df.set_index(['datetime', 'site_id']) 
                master_df = pd.concat([master_df, site_df])

            return master_df

        
        
        BASE_URL = 'https://cosmos-api.ceh.ac.uk'


        start_date = format_datetime(datetime(min(self.years), 1, 1))
        end_date = format_datetime(datetime(max(self.years), 12, 31))
        query_date_range = f'{start_date}/{end_date}'
                    
            
        # loop through sites and concat
        df = []
        #for site_id_ii in self.site:
        site_id_ii = self.site
        #loop
        query_url = f'{BASE_URL}/collections/1D/locations/{site_id_ii}?datetime={query_date_range}'
        resp = get_api_response(query_url)
        #print(resp)
        df.append(read_json_collection_data(resp))
            
        df = pd.concat(df)
        return df
    
    def get_cosmos_api_single(self,column):
        return self.api_data[[column]].droplevel('site_id')\
                .reset_index()\
                .assign(date = lambda x: pd.to_datetime(x.datetime).dt.date)\
                .set_index('date')\
                .drop(columns=['datetime'])
    
    def get_cosmos_data(self):
        cosmos_vwc = pd.read_csv('/data/moisturedata/COSMOS-UK_HydroSoil_Daily/COSMOS-UK_'+self.site+'_HydroSoil_Daily_2013-2019.csv', na_values=-9999)\
            .rename(columns={'DATE_TIME':'Date'})
        cosmos_vwc["Date"] = pd.to_datetime(cosmos_vwc["Date"])
        return cosmos_vwc[['Date','COSMOS_VWC']].loc[(cosmos_vwc['Date'].dt.year.isin(self.years))].set_index('Date')

    def get_PE_data(self):
        cosmos_vwc = pd.read_csv('/data/moisturedata/COSMOS-UK_HydroSoil_Daily/COSMOS-UK_'+self.site+'_HydroSoil_Daily_2013-2019.csv', na_values=-9999)\
            .rename(columns={'DATE_TIME':'Date'})
        cosmos_vwc["Date"] = pd.to_datetime(cosmos_vwc["Date"])
        return  cosmos_vwc[['Date','PE']].loc[(cosmos_vwc['Date'].dt.year.isin(self.years))].set_index('Date')
    
    def get_PREC_data(self):
        precip_cosmos = pd.read_csv('/data/moisturedata/COSMOS-UK_HydroSoil_SH_2013-2019/COSMOS-UK_'+self.site+'_HydroSoil_SH_2013-2019.csv', na_values=-9999)
        precip_cosmos = precip_cosmos[['PRECIP']].groupby(pd.to_datetime(precip_cosmos.DATE_TIME).dt.date).PRECIP.agg(['sum','count']).reset_index() \
            .rename(columns={'DATE_TIME':'Date','sum':'PRECIP','count':'prec_count'})
        precip_cosmos["Date"] = pd.to_datetime(precip_cosmos["Date"])
        return precip_cosmos.loc[(precip_cosmos['Date'].dt.year.isin(self.years))].set_index('Date')
    
    def get_LAI_data(self):
        print('LAI')
        return self.get_MODIS_data()

    def get_MODIS_data(self):
        MODIS = pd.read_csv('data/MODIS_LAI_2015-12-15_to_2023-03-03.csv',parse_dates=['Date'])
        MODIS = MODIS.loc[MODIS['SITE_ID']==self.site] \
                .loc[MODIS['Date'].dt.year.isin(self.years)]  \
                .sort_values('Date').reset_index(drop=True)
        #MODIS.plot(x='Date',y='LAI', style='ro')
        MODIS['CALC'] = MODIS['LAI'].shift(1) + (MODIS['LAI']-MODIS['LAI'].shift(1))/(MODIS['Confidence']+1)
        MODIS['CALC'][0] = MODIS['LAI'][0] # this gives a warning
        MODIS['CALC2'] = MODIS['CALC']
        MODIS['Date2'] = MODIS['Date']
        MODIS['Date0'] = MODIS['Date2'].shift(1)
        MODIS['Date0'][0] = MODIS['Date2'][0] # this gives a warning

        ########## padding missing dates #################
        idx = pd.date_range(start='1/1/'+str(min(self.years)), end='31/12/'+str(max(self.years))) 
        MODIS = MODIS.set_index(['Date'])#.drop(columns='SITE_ID')
        MODIS = MODIS.reindex(idx)

        MODIS['SITE_ID'].ffill(inplace=True)
        MODIS['CALC'].ffill(inplace=True)
        MODIS['Date2'].bfill(inplace=True) # just for the first entries
        MODIS['CALC2'].bfill(inplace=True) 
        MODIS['Date0'].ffill(inplace=True) # just for the first entries

        # ifelse(~nan(Conf), CALC, CALC-1)
        # now do traingle rule between CALC and CALC2
        MODIS['LAI_pred'] = MODIS['CALC2'] + (MODIS['CALC']-MODIS['CALC2']) *((MODIS['Date2']-MODIS.index)/ np.timedelta64(1, 'D')).astype(int)/ \
                                ((MODIS['Date2']-MODIS['Date0'])/ np.timedelta64(1, 'D')).astype(int)
        MODIS['LAI_pred'][0] = MODIS['LAI'][0] 
        return MODIS[['LAI_pred']]
    
 #   def get_driving_data(self):
 #       self.driving_data = pd.concat([self.PE_data(),self.PREC_data(),self.LAI_data()], axis=1)
 #       return #pd.concat([self.PE_data(),self.PREC_data(),self.LAI_data()], axis=1)
    
    def write_atmo_data(self):
        """
        Method to write driving data input file for Hydrus (atmosphere.csv).

        Parameters
        ----------

        Returns
        -------
        atmo: dataframe that contains data for atmosphere.csv

        """
        ########## API ######
        atmo = self.get_cosmos_api_single('pe').join(self.get_cosmos_api_single('precip')).join(self.MODIS_data) \
            .assign(rSoil=lambda x: (x['pe']*np.exp(-0.463*x['LAI_pred']) )) \
            .assign(rRoot=lambda x: (x['pe']-x["rSoil"])) \
            .drop(columns=['LAI_pred','pe']) \
            .transform(lambda x: x * 0.1) \
            .assign(hCritA=lambda x: 100000) \
            .assign(tAtm=lambda x: range(len(x))).set_index('tAtm')\
            .rename(columns={"precip":"Prec"}).reset_index()\
            .assign(tAtm=lambda x: x['tAtm']+1) 
        
        print('printing atmo API data.')
        atmo.to_csv('data/atmosphere_API.csv', float_format='%6.4f') # will stuck if too many digits
        
        ########## files ######
        atmo = self.PE_data.join(self.PREC_data).join(self.MODIS_data) \
            .assign(rSoil=lambda x: (x['PE']*np.exp(-0.463*x['LAI_pred']) )) \
            .assign(rRoot=lambda x: (x['PE']-x["rSoil"])) \
            .drop(columns=['LAI_pred','PE']) \
            .transform(lambda x: x * 0.1) \
            .assign(hCritA=lambda x: 100000) \
            .assign(tAtm=lambda x: range(len(x))).set_index('tAtm')\
            .rename(columns={"PRECIP":"Prec"}).reset_index()\
            .assign(tAtm=lambda x: x['tAtm']+1) 
        
        print('printing atmo data.')
        atmo.to_csv('data/atmosphere.csv', float_format='%6.4f') # will stuck if too many digits
        
        return atmo
    
  

