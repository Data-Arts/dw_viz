import pandas as pd
import numpy as np
import os,sys,json
import collections
from pathlib import Path
from scipy.stats import mstats
import matplotlib.pyplot as plt
import seaborn as sns

from snowflake import connector
import preproc


dbname = 'smartsdb'

cds_file = Path(__file__).parent / 'metadata.txt'
with open(cds_file,'r') as f:
    meta = json.load(f)

chunk_size = int(meta['chunk_size'])
cds = meta['sfcds']
load_infos = meta['dbs']


begin_dt = meta['begin_dt']
end_dt = meta['end_dt']


trendYears = meta['trendYears']
leng = len(trendYears) #FYEND

orgmapfilepath = meta['orgmapfilepath']
var = meta['var']
limits_winsorized = meta['limits_winsorized']



############## utils ##############
def _connect_snowflake(account,warehouse,database,schema,cds):
    conn = connector.connect(
                user=cds['uid'],
                password=cds['pwd'],
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
                role="SYSADMIN"
                )
    return conn


def _read_cdpdata_sf(engine,survey_id=None,time_thresh=None,time_threshhi=None):
    """
    read_cdpdata_sf(
        engine (str): 'an instance of snowflake-connector-python engine',
        survey_id (list|None): 'a list of survey_id for DATAARTSDB.SMARTSDB.DATA table',
        time_thresh (str): 'starting date string: yyyy-mm-dd',
        time_threshhi (str|None): 'ending date string: yyyy-mm-dd'
    )
    
    
    return: Raw CDP data from SmartsDB
    """
    
    #- first get viewid for data query #9
    vquery='select distinct id,name from DATAARTSDB.SMARTSDB.VIEWS'
    
    cur = engine.cursor().execute(vquery)
    viewsDF = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    vid=viewsDF[viewsDF['name'.upper()]=='SAS']['id'.upper()].values[0]
    
    #- for given survey id
    if survey_id is not None:
        query="select survey_response_id, v.alias as colname, d.value as value,d.updated_at as updated_at from (select * from DATAARTSDB.SMARTSDB.VIEW_FIELD where view_id="+str(vid)+")v left outer join (select distinct on (field_id) survey_response_id,field_id,value,updated_at from DATAARTSDB.SMARTSDB.DATA where survey_response_id="+str(survey_id)+" order by field_id,value,updated_at desc)d on v.field_id=d.field_id;"
        
    #- for time threshold
    
    # beginning datetime
    if time_thresh is not None:
        #- get the field id for updated_dt
        squery='select distinct id,name from DATAARTSDB.SMARTSDB.FIELDS;'
        
        cur = engine.cursor().execute(squery)
        fieldsDF = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
        
        dtid=fieldsDF[fieldsDF['name'.upper()]=='survey_responses_updated_at']['id'.upper()].values[0]
        
        squery='select distinct survey_response_id,max(value) as latest_dt from DATAARTSDB.SMARTSDB.DATA where field_id='+str(dtid)+' group by survey_response_id;'
        
        cur = engine.cursor().execute(squery)
        surveydata = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
        
        
        surveydata['latest_dt'.upper()]=pd.to_datetime(surveydata['latest_dt'.upper()])
        surveydata.dropna(subset=['survey_response_id'.upper()],inplace=True)
        
        # ending datetime, if not none
        if time_threshhi is not None:
            print(f"Getting all surveys updated between {time_thresh} and {time_threshhi}")
            updated_surveys=set(surveydata[(surveydata['latest_dt'.upper()]>time_thresh)&(surveydata['latest_dt'.upper()]<=time_threshhi)]['survey_response_id'.upper()].values)
        else:
            print(f"Getting all surveys updated since {time_thresh}")
            updated_surveys=set(surveydata[surveydata['latest_dt'.upper()]>time_thresh]['survey_response_id'.upper()].values)
            
        # check if length of updated_surveys is 0
        if len(updated_surveys)>0:
            print(f"No. of surveys updated since {time_thresh}: {len(updated_surveys)}")
        else:
            print("No updated surveys in this time")
            sys.exit()
            
        
        if len(updated_surveys)==1:
            survey_ids=tuple(list(updated_surveys)+[0])
        else:
            survey_ids=tuple(list(updated_surveys))
        
        # load from snowflake by chunk
        n = len(survey_ids)//chunk_size # number of loadings
        if len(survey_ids)%chunk_size!=0:
            n+=1
            
        # load a chunk and append to a dataframe list
        cdpdata = []
        for i in range(n):
            _survey_ids = survey_ids[chunk_size*i:chunk_size*(i+1)]
           
            query="select survey_response_id, v.alias as colname, d.value as value,d.updated_at as updated_at from (select * from DATAARTSDB.SMARTSDB.VIEW_FIELD where view_id="+str(vid)+")v left outer join (select survey_response_id,field_id,value,updated_at from DATAARTSDB.SMARTSDB.DATA where survey_response_id in "+str(_survey_ids)+" QUALIFY ROW_NUMBER() OVER (PARTITION BY (survey_response_id,field_id) ORDER BY (survey_response_id,field_id)) = 1)d on v.field_id=d.field_id;"
        
        #     cdpdata=pd.read_sql(query,engine)
            cur = engine.cursor().execute(query)
            _cdpdata = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
            cdpdata.append(_cdpdata)
            
        # combine the chunks
        cdpdata = pd.concat(cdpdata,axis=0)
    
    # sort data
    cdps=cdpdata.sort_values(['survey_response_id'.upper(),'colname'.upper(),'updated_at'.upper()])
    #drop duplicates by survey_response_id and colname
    cdpss=cdps.drop_duplicates(subset=['survey_response_id'.upper(),'colname'.upper()],keep='last')
    #drop duplicates by rows
    cdpdata=cdpss.drop_duplicates().reset_index(drop=True)
    
    
    return cdpdata


def _preprocess_1(df,allOrgIDsList):
    ##############
    # Add Overall, Sector, Budget Category, and Geographic Columns
        # Before deprec total expense field
    ##############

    #CleanDepreciation
    depreConditions = [(df['DPRCNTO'].isna()),(~df['DPRCNTO'].isna())] # DPRCNTO: total_depreciation
    depreChoices = [(0),(df['DPRCNTO'])]
    df['CleanDepreciation'] = np.select(depreConditions,depreChoices)

    #CleanOpRevUn
    oprevConditions = [(df['OPUN'].isna()),(~df['OPUN'].isna())] #OPUN: total_operating_revenue_unrestricted
    oprevChoices = [(df['OPTO']),(df['OPUN'])] # OPTO: total_operating_revenue_formula
    df['CleanOpRevUn'] = np.select(oprevConditions,oprevChoices)

    #Total_Exp_B4_Dep, Op_Surplus_before_Depre_CALC, Op_Surplus_after_Depre_CALC, Un_Surplus_before_Depre_CALC
    df['Total_Exp_B4_Dep'] = df['TOTEXTOX'] - df['CleanDepreciation'] # TOTEXTOX: total_expenses_formula
    df['Op_Surplus_before_Depre_CALC'] = df['CleanOpRevUn'] - df['Total_Exp_B4_Dep']
    df['Op_Surplus_after_Depre_CALC'] = df['CleanOpRevUn'] - df['TOTEXTOX']
    df['Un_Surplus_before_Depre_CALC'] = df['TOTUNIN'] - df['Total_Exp_B4_Dep'] # TOTUNIN: total_revenue_unrestricted

    #(total_revenue_unrestrcted - (total_expenses_formula - DPRCNTO)) / (total_expenses_formula - DPRCNTO)  
    df['Unrestricted_operating_bottomline'] = df['Un_Surplus_before_Depre_CALC']/df['Total_Exp_B4_Dep']

    #TOTDEVCD
    df['TOTDEVCD'] = df['TOTCNTTO']/df['DEVEXPERSF'] # total_contributed_revenue_formula/fundraising_expenses_percentage_sf

    #fulltime_employees, fulltime_employees_turnover
    df[['FTEMPS', 'FTSEAS']] = df[['FTEMPS','FTSEAS']].fillna(value=0)
    df['fulltime_employees'] = df['FTEMPS'] + df['FTSEAS']
    df[['FTTURNCD', 'FTSEASTURN']] = df[['FTTURNCD','FTSEASTURN']].fillna(value=0)
    df['fulltime_employees_turnover'] = df['FTTURNCD'] + df['FTSEASTURN']

    #digitalLiveOfferings, digitalOnDemandOfferings, digitalOfferings, inPersonOfferings, totalOfferings
    df['digitalLiveOfferings'] = df['TOSHOWSDIGLIVE'] + df['TOBOOKINDIGLIVE'] + df['OFFEDDIGLIVE'] + df['EDPROFDIGLIVE'] + df['FIELDTDIGLIVE'] + df['GUIDTDIGLIVE'] + df['LECTDIGLIVE'] + df['FILMSCRNDIGLIVE'] + df['WKSRDGDIGLIVE'] + df['FESCONFDIGLIVE'] + df['BCSTSHOWLIVE'] + df['OTHPRGDIGLIVE'] + df['COMPRGDIGLIVE']
    df['digitalOnDemandOfferings'] = df['TOSHOWSDIGODEM'] + df['TOBOOKINDIGODEM'] + df['OFFEDDIGODEM'] + df['EDPROFDIGODEM'] + df['FIELDTDIGODEM'] + df['GUIDTDIGODEM'] + df['LECTDIGODEM'] + df['FILMSCRNDIGODEM'] + df['WKSRDGDIGODEM'] + df['FESTCONFDIGODEM'] + df['BCSTSHOWODEM'] + df['PMEXDIGODEM'] + df['TMEXDIGODEM'] + df['TREXDIGODEM'] + df['PUBSDIG'] + df['OTHPRGDIGODEM']
    df['digitalOfferings'] = df['digitalLiveOfferings'] + df['digitalOnDemandOfferings']
    df['inPersonOfferings'] = df['TOSHOWS'] + df['TOBOOKIN'] + df['OFFEDINP'] + df['EDPROFINP'] + df['FIELDTINP'] + df['GUIDTINP'] + df['LECTINP'] + df['FILMSCRNINP'] + df['WKSRDGINP'] + df['FESCONFINP'] + df['OTHPRGINP'] + df['COMPRGINP']
    df['totalOfferings'] = df['digitalOfferings'] + df['inPersonOfferings']

    #Overall
    df['Overall'] = 'Overall'

    ############ Sector ############
    arttsAlliancesList = ['A01','A02','A03','A12']
    artsEdList = ['A25','A6E']
    artMuseumList = ['A51']
    communityList = ['A20','A23','A24','A26','A27','A40']
    danceList = ['A62','A63']
    musicList = ['A68','A6B','A6C']
    operaList = ['A6A']
    performingArtsCentersList = ['A61']
    syphonyOrchestraList = ['A69']
    theaterList = ['A65']
    otherMuseumsList = ['A50','A52','A54','A56','A57']
    generalPerformingArtsList = ['A60']

    sectorConditions = [(df['OrgNTEE'].isin(arttsAlliancesList)),
                        (df['OrgNTEE'].isin(artsEdList)),
                        (df['OrgNTEE'].isin(artMuseumList)),
                        (df['OrgNTEE'].isin(communityList)),
                        (df['OrgNTEE'].isin(danceList)),
                        (df['OrgNTEE'].isin(musicList)),
                        (df['OrgNTEE'].isin(operaList)),
                        (df['OrgNTEE'].isin(performingArtsCentersList)),
                        (df['OrgNTEE'].isin(syphonyOrchestraList)),
                        (df['OrgNTEE'].isin(theaterList)),
                        (df['OrgNTEE'].isin(otherMuseumsList)),
                        (df['OrgNTEE'].isin(generalPerformingArtsList)),]
    sectorChoices = [('Arts Alliances'),('Arts Education'),('Art Museums'),('Community'),('Dance'),('Music'),('Opera'),
                     ('Performing Arts Centers'),('Symphony Orchestras'),('Theater'),('Other Museums'),
                     ('General Performing Arts')]
    df['Sector'] = np.select(sectorConditions,sectorChoices)


    #Size
    sizeConditions = [((df['Sector'] == 'Arts Education') & (df['TOTEXTOX'] < 364494)),
                      ((df['Sector'] == 'Arts Education') & (df['TOTEXTOX'] >= 364494) & (df['TOTEXTOX'] <= 2436552)),
                      ((df['Sector'] == 'Arts Education') & (df['TOTEXTOX'] > 2436552)),
                      ((df['Sector'] == 'Art Museums') & (df['TOTEXTOX'] < 1599040)), 
                      ((df['Sector'] == 'Art Museums') & (df['TOTEXTOX'] >= 1599040) & (df['TOTEXTOX'] <= 14213117)), 
                      ((df['Sector'] == 'Art Museums') & (df['TOTEXTOX'] > 14213117)),
                      ((df['Sector'] == 'Community') & (df['TOTEXTOX'] < 261496)), 
                      ((df['Sector'] == 'Community') & (df['TOTEXTOX'] >= 261496) & (df['TOTEXTOX'] <= 1731579)),
                      ((df['Sector'] == 'Community') & (df['TOTEXTOX'] > 1731579)),
                      ((df['Sector'] == 'Dance') & (df['TOTEXTOX'] < 211758)), 
                      ((df['Sector'] == 'Dance') & (df['TOTEXTOX'] >= 211758) & (df['TOTEXTOX'] <= 1503530)),
                      ((df['Sector'] == 'Dance') & (df['TOTEXTOX'] > 1503530)),
                      ((df['Sector'] == 'Music') & (df['TOTEXTOX'] < 170745)), 
                      ((df['Sector'] == 'Music') & (df['TOTEXTOX'] >= 170745) & (df['TOTEXTOX'] <= 969847)), 
                      ((df['Sector'] == 'Music') & (df['TOTEXTOX'] > 969847)),
                      ((df['Sector'] == 'Opera') & (df['TOTEXTOX'] < 523508)), 
                      ((df['Sector'] == 'Opera') & (df['TOTEXTOX'] >= 523508) & (df['TOTEXTOX'] <= 4888184)),
                      ((df['Sector'] == 'Opera') & (df['TOTEXTOX'] > 4888184)),
                      ((df['Sector'] == 'Performing Arts Centers') & (df['TOTEXTOX'] < 623041)), 
                      ((df['Sector'] == 'Performing Arts Centers') & (df['TOTEXTOX'] >= 623041) & (df['TOTEXTOX'] <= 7999999)),
                      ((df['Sector'] == 'Performing Arts Centers') & (df['TOTEXTOX'] > 7999999)),
                      ((df['Sector'] == 'Symphony Orchestras') & (df['TOTEXTOX'] < 288647)), 
                      ((df['Sector'] == 'Symphony Orchestras') & (df['TOTEXTOX'] >= 288647) & (df['TOTEXTOX'] <= 2436552)), 
                      ((df['Sector'] == 'Symphony Orchestras') & (df['TOTEXTOX'] > 2436552)),
                      ((df['Sector'] == 'Theater') & (df['TOTEXTOX'] < 409028)), 
                      ((df['Sector'] == 'Theater') & (df['TOTEXTOX'] >= 409028) & (df['TOTEXTOX'] <= 3041233)), 
                      ((df['Sector'] == 'Theater') & (df['TOTEXTOX'] > 3041233)),
                      ((df['Sector'] == 'Other Museums') & (df['TOTEXTOX'] < 650217)), 
                      ((df['Sector'] == 'Other Museums') & (df['TOTEXTOX'] >= 650217) & (df['TOTEXTOX'] <= 4888184)), 
                      ((df['Sector'] == 'Other Museums') & (df['TOTEXTOX'] > 4888184)),
                      ((df['Sector'] == 'General Performing Arts') & (df['TOTEXTOX'] < 244357)), 
                      ((df['Sector'] == 'General Performing Arts') & (df['TOTEXTOX'] >= 244357) & (df['TOTEXTOX'] <= 2150685)),
                      ((df['Sector'] == 'General Performing Arts') & (df['TOTEXTOX'] > 2150685))
        ]

    sizeChoices = [('Small'),('Medium'),('Large'),
                    ('Small'),('Medium'),('Large'),
                    ('Small'),('Medium'),('Large'),
                    ('Small'),('Medium'),('Large'),
                    ('Small'),('Medium'),('Large'),
                    ('Small'),('Medium'),('Large'),
                    ('Small'),('Medium'),('Large'),
                    ('Small'),('Medium'),('Large'),
                    ('Small'),('Medium'),('Large'),
                    ('Small'),('Medium'),('Large'),
                    ('Small'),('Medium'),('Large'),
                    ]

    df['Size'] = np.select(sizeConditions,sizeChoices)

    sizeList = ['Small','Medium','Large']

    ################################
    # Consistuency
    dfConstit = df[df['TARGRACE'].notnull()] #constituencies_served_yn]=='Yes'] # TARGRACE: constituencies_ethnic_served
    constitOrgIDList = list(set(dfConstit['organizations_id'].tolist()))
    constitConditions = [(df['organizations_id'].isin(constitOrgIDList)),
                         (~df['organizations_id'].isin(constitOrgIDList))] #mission_voice_yn
    constitChoices = [('Yes'),('No')]
    df['CleanConstituency'] = np.select(constitConditions,constitChoices)

    # Mission
    dfMission = df[df['MISSETH'].notnull()] #mission_voice_ethnic_served
    missionOrgIDList = list(set(dfMission['organizations_id'].tolist()))
    missionConditions = [(df['organizations_id'].isin(missionOrgIDList)),(~df['organizations_id'].isin(missionOrgIDList))]
    missionChoices = [('Yes'),('No')]
    df['CleanMission'] = np.select(missionConditions,missionChoices)

    # Disability
    dfDisability = df[df['TARGDISAB']=='Yes'] #constituencies_disabled_served
    disabilityOrgIDList = list(set(dfDisability['organizations_id'].tolist()))
    disabilityConditions = [(df['organizations_id'].isin(disabilityOrgIDList)),
                            (~df['organizations_id'].isin(disabilityOrgIDList))]
    disabilityChoices = [('Yes'),('No')]
    df['CleanDisability'] = np.select(disabilityConditions,disabilityChoices)
    
    ####################
    # Create BS OrgID List
    ####################
    # dfBSYes = df[(~df['WRKCAPCD'].isna())]  #total_working_capital_unrestricted
    dfBSYes =df.copy()
    allBSOrgIDsList = dfBSYes['organizations_id'].tolist()

    # Clean df for just relevant single year
    dfSingleYear = df[df['CYEAR']==trendYears[-1]] #fiscal_year
    singleYearOrgIDList = list(set(dfSingleYear['organizations_id'].tolist()))

    dfBSYesSingleYear = dfBSYes[dfBSYes['CYEAR']==trendYears[-1]]
    BSYesSingleYearOrgIDList = list(set(dfBSYesSingleYear['organizations_id'].tolist()))


    print("Single Year All Orgs: " + str(len(singleYearOrgIDList)))
    print("Single Year BS Orgs: " + str(len(BSYesSingleYearOrgIDList)))
    
    # Clean df to only have orgs with CDPs for all trend years
    trendOrgIDListPre = [orgid for orgid, count in collections.Counter(allOrgIDsList).items() if count == leng]
    trendOrgIDList = list(set(trendOrgIDListPre).intersection(singleYearOrgIDList))

    trendWBSOrgIDListPre = [orgid for orgid, count in collections.Counter(allBSOrgIDsList).items() if count == leng]
    trendWBSOrgIDList = list(set(trendWBSOrgIDListPre).intersection(BSYesSingleYearOrgIDList))

    print("Trend Years All Orgs: " + str(len(trendOrgIDList)))
    print("Trend Years BS Orgs: " + str(len(trendWBSOrgIDList)))
    
    dfsList = [dfSingleYear,]
    cols_bl = ['organizations_id','CDPNAME','ADDCITY','COUNTRY','ORGTYPE','Sector','Size',var]

    for y in trendYears:
        dfTrend = df[((df['CYEAR']==y) & df['organizations_id'].isin(trendOrgIDList))]
        trend2List = list(set(dfTrend['organizations_id'].tolist()))
        dfBSYesTrend = dfBSYes[((dfBSYes['CYEAR']==y) & dfBSYes['organizations_id'].isin(trendWBSOrgIDList))]
        BSYesTrend2List = list(set(dfBSYesTrend['organizations_id'].tolist()))

        dfsList.append(dfTrend[cols_bl])
        
    return dfsList

    

#processing.ipynb
def _preprocess_2(dfs):
    # ensure no 0 in Sector and Size columns
    df0 = []
    for i,df in enumerate(dfs[1:]):
        df0.append(df[(df['Sector']=='0')|(df['Size']=='0')])
        df['year'] = trendYears[i]
    dfs = pd.concat(dfs,axis=0)
    
    #sector
    sector0 = np.unique(dfs.loc[dfs['Sector']=='0','organizations_id'])
    for cdpn in sector0:
        df = dfs[dfs['organizations_id']==cdpn]
        sc = df['Sector'].unique()
        if len(sc)==1 and sc[0]=='0':
    #         print('continue')
            continue
        print(cdpn,sc)
        s = [ss for ss in sc if ss!='0'][0]
        dfs.loc[dfs['organizations_id']==cdpn,'Sector'] = s

    #Size
    size0 = np.unique(dfs.loc[dfs['Size']=='0','organizations_id'])
    for cdpn in size0:
        df = dfs[dfs['organizations_id']==cdpn]
        sc = df['Size'].unique()
        if len(sc)==1 and sc[0]=='0':
    #         print('continue')
            continue
        print(cdpn,sc)
        s = [ss for ss in sc if ss!='0'][0]
        dfs.loc[dfs['organizations_id']==cdpn,'Size'] = s

    # use the longest cdp name for one organizations_id
    for oid in np.unique(dfs['organizations_id']):
        cdpns = list(dfs.loc[dfs['organizations_id']==oid,'CDPNAME'])
        indmax = np.argmax([len(i) for i in cdpns])
        if len(np.unique(cdpns)) > 1:
            print(oid,cdpns[indmax])
        dfs.loc[dfs['organizations_id']==oid,'CDPNAME'] = cdpns[indmax]
    
    print('dfs shape:',dfs.shape)
    
    #Remove orgs having sector==0 or size==0
    orgs0 = np.unique(dfs.loc[(dfs['Sector']=='0')|(dfs['Size']=='0'),'organizations_id'])
    dfs = dfs[~dfs['organizations_id'].isin(orgs0)].sort_values(['organizations_id','year','Size','Sector']).reset_index(drop=True)
    
    return dfs
    
    
    
def create_cdpdataset():
    #snowflake connector
    engine = _connect_snowflake(account=load_infos[dbname]['sf_account'],
                                 warehouse=load_infos[dbname]['warehouse'],
                                 database=load_infos[dbname]['database'],
                                 schema=dbname,
                                 cds=cds)
    
    #loading cdp data
    df = _read_cdpdata_sf(engine,time_thresh=begin_dt,time_threshhi=end_dt)
    #change column names
    df.columns = ['survey_response_id', 'colname', 'value', 'updated_at']
    #create pivot table
    df = df.pivot(index="survey_response_id",columns="colname",values="value").reset_index(drop=True)
    #keep data where CYEAR>2015
    df['CYEAR'] = pd.to_numeric(df['CYEAR'],errors='coerce')
    df=df[df['CYEAR']>2015].reset_index(drop=True)
    
    df = preproc.main(df,throw_examples=True)
    
    #chang column name
    df.rename(columns={'ORGID':'organizations_id'},inplace=True)
    
    # df should have rows with CYEAR in trendYears list
    df = df[df['CYEAR'].isin(trendYears)]
    #list of all org ids
    allOrgIDsList = df['organizations_id'].tolist()

    #read orgmap
    dfOrgMap = pd.read_excel(Path(__file__).parent / orgmapfilepath).rename(columns={'TRGI':'organizations_id'})
    # Join with OrgMap to Get NTEE and CBSA
    df = pd.merge(df, dfOrgMap, how='left', on='organizations_id',suffixes=('','_orgmap'))
    del dfOrgMap
    print('After merging with orgmap',df.shape)
    
    
    df = _preprocess_1(df,allOrgIDsList)
    df = _preprocess_2(df)
    return df
    
    
def winsorize_data(df,limits_winsorized,var):
    #Winsorizing by size and sector
    dfy_winsorized = []

    for y in df['year'].unique():
        dfy = df.loc[df['year']==y,['organizations_id',var,
                                    'Size','year','Sector']]
        for s in dfy['Size'].unique():
            dfsy = dfy[dfy['Size']==s]
            tp = np.array(dfsy[var])
            tp = np.array(mstats.winsorize(tp, limits=limits_winsorized))
            dfsy[var] = tp
            dfy_winsorized.append(dfsy)
    dfy_winsorized = pd.concat(dfy_winsorized,axis=0).sort_values(['organizations_id',
                                                                   'year','Size','Sector']).reset_index(drop=True)
    return dfy_winsorized




    

def create_boxplot(dfy_winsorized,var,hue='Size',savepath=None):
    f,ax = plt.subplots(1,1,figsize=(20,10))
    colors = ['red','blue','green']*len(dfy_winsorized['year'].unique())


    dfy_winsorized[[var,hue,'year']].boxplot(column=var,by=['year',hue],rot=90,ax=ax)
    if savepath is None:
        plt.savefig(f'boxplot_by_{hue}.png')
    else:
        plt.savefig(savepath)
    return

        
def create_barplot(df,var,hue='Sector',style='darkgrid',savepath=None):
    if savepath is None:
        savepath = f'Mean_{var}_by_Year_and_{hue}.png'
    df['year'] = df['year'].astype(int)
    # Group data by year and type and calculate mean values
    grouped_data = df[['year',hue,var]].groupby(['year', hue]).mean(numeric_only=True).reset_index()

    # Set seaborn style
    sns.set_style(style)

    _,ax = plt.subplots(1,1,figsize=(20,10))

    # Create bar plot
    sns.barplot(x='year', y=var, hue=hue, data=grouped_data, ax=ax)

    # set legend fontsize larger
    # sns.set(font_scale=1.5)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.,title=hue)
    # Set x-axis label
    plt.xlabel('Year',size=15)

    # Set y-axis label
    plt.ylabel(f'Mean {var}',size=15)

    # Set title
    plt.title(f'Mean {var} by Year and {hue}',size=20)

    plt.savefig(savepath)
    return


def create_OneBarplot(df,var,yr,sz,st,agg='mean',style='darkgrid',width=0.1,color='orange',alpha=1,xlabel_size=30,title_size=30,xticks_size=20,chartTitle="Graph Title",savepath=None):

    # Group data by year and type and calculate mean/median values
    if agg=='median':
        grouped_data = df[['year', 'Size','Sector',var]].groupby(['year', 'Size','Sector']).median().reset_index()
    else:
        grouped_data = df[['year', 'Size','Sector',var]].groupby(['year', 'Size','Sector']).mean().reset_index()
    
    grouped_data_sub = grouped_data[(grouped_data['year']==yr)&(grouped_data['Size']==sz)&(grouped_data['Sector']==st)]

    # Set seaborn style
    sns.set_style(style)

    _,ax = plt.subplots(1,1,figsize=(20,10))

    # Create bar plot
    sns.barplot(x='year', y=var, 
                data=grouped_data_sub, ax=ax,width=width,color=color,alpha=alpha)

    # set legend fontsize larger
    # sns.set(font_scale=4)

    # Set x-axis label
    plt.xlabel('Year',size=xlabel_size)
    # Set x-axis ticks
    plt.xticks(size=xticks_size)

    # Set y-axis label
    plt.ylabel('')

    # Set title
    #plt.title(f'{var}-{agg} {yr} ({sz} - {st})',size=title_size)
    
    plt.title(chartTitle)

    if savepath is None:
        plt.savefig(f'{var}-{agg}_{yr}_{sz}_{st}.png')
    else:
        plt.savefig(savepath)
        
    return


def create_oneyear_Barplot(df,var,yr,hue='Sector',agg='mean',style='darkgrid',alpha=1,xlabel_size=30,title_size=30,xticks_size=20):
    
    # Group data type and calculate mean values for one year
    if agg=='median':
        grouped_data = df.loc[df['year']==yr,['year',hue,var]].groupby(['year', hue]).median().reset_index()
    else:
        grouped_data = df.loc[df['year']==yr,['year',hue,var]].groupby(['year', hue]).mean().reset_index()
        
    
    # Set seaborn style
    sns.set_style(style)
    
    _,ax = plt.subplots(1,1,figsize=(20,10))
    
    # Create bar plot
    sns.barplot(x='year', y=var, hue=hue, data=grouped_data, ax=ax)
    
    # set legend fontsize larger
    # sns.set(font_scale=1.5)
    
    # Set x-axis label
    plt.xlabel('Year',size=xlabel_size)
    
    # Set y-axis label
    plt.ylabel('')
    
    # Set title
    plt.title(f'{var}-{agg} {yr} (by {hue})',size=title_size)
    
    plt.savefig(f'{var}-{agg}_{yr}_by_{hue}.png');
    return 


if __name__ == '__main__':
    print("========= preparing data =========")
    #create cdp dataset from snowflake smartsdb
    df = create_cdpdataset()
    
    #winsorize data by Size and Sector
    df = winsorize_data(df,limits_winsorized,var)
    
    print('========= Display graph =========')
    #boxplot
    create_boxplot(df,var,hue='Size',savepath='boxplot_by_size.png')
    
    #barplot
    create_barplot(df,var,hue='Sector',style='darkgrid',savepath=None)
    create_barplot(df,var,hue='Size',style='whitegrid',savepath=None)
    
    #barplot for one year - Size - Sector
    create_OneBarplot(df=df, # dataset
                      var='Unrestricted_operating_bottomline', #variable
                      yr=2020,sz='Small',st='Community', # year, size, sector
                      agg='mean', # method to aggregate mean/median
                      style='darkgrid',width=0.1,color='orange',alpha=1, #canvas style, bar width, bar color, tranparency 0-1
                      xlabel_size=30,title_size=30,xticks_size=20) #xlabel size, title size, xticks size
    
    #barplot for one year - Size or Sector
    create_oneyear_Barplot(df=df,
                           var='Unrestricted_operating_bottomline',
                           yr=2020,hue='Sector',agg='mean',
                           style='darkgrid',alpha=1,
                           xlabel_size=30,title_size=30,xticks_size=20)
    
    
    
    
