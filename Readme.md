## 1. setup Environment
python >= 3.8  
  
cd DataViz  
pip install -r requirements.txt  
or   
cd DataViz  
pip3 -m pip install -r requirements.txt  
  
  
## 2. metadata.txt: meta data to set the hyperparameters
#### (1) Use your own Snowflake credentials:  
"sfcds":{"uid":"XXXXX",  
         "pwd":"XXXXX"}  
  
#### (2) SmartsDB database info on Snowflake:  
"dbs":{  
        "smartsdb":{"sf_account":"smu-da",  
                    "warehouse":"dataarts",  
                    "database":"dataartsdb"}  
      }  
  
#### (3) Chunk size for query, it is recommendated to not change:  
"chunk_size":16000  
  
#### (4) The CDP data time range, end_dt can be set as: "end_dt":None  
"begin_dt":"2017-12-31"  
"end_dt":"2022-12-31"  
  
#### (5) The trend years you want to include in CDP data:  
"trendYears":[2018,2019,2020,2021,2022]  
  
#### (6) The OrgMap.xlsx file path:  
"orgmapfilepath":"OrgMap.xlsx"  
  
#### (7) The target field you want to display, the default is:  
"var":"Unrestricted_operating_bottomline"  
  
#### (8) The low and high boundries to winsorize/trim the abnormalies for data of "var":  
"limits_winsorized":[0.1, 0.1]  
  
  
## 3. To run the script to create the default figures
python3 utils.py  
or  
python utils.py  
  
  
## 4. Functions
#### (1) create_cdpdataset() -> pandas.DataFrame:
Pull CDP data from Snowflake  
  
#### (2) winsorize_data(df,limits_winsorized,var) -> pandas.DataFrame:  
Winsorize the CDP data (df:pandas.DataFrame) using "limits_winsorized" hyperparameter based on the field "var"  
  
#### (3) create_barplot(df,var,hue,style,savepath) -> None:  
create bar plots and save to "savepath" (string) for the field "var"  
hue: The bar plots to grouped by with, could be "Sector" or "Size"  
style: figure style, options: {"darkgrid", "whitegrid", "dark", "white", "ticks"}  




