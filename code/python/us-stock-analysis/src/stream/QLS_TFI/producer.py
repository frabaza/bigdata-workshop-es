from time import sleep
import pandas as pd
from datetime import datetime
import json
import sys
import numpy as np

unit_concern = pd.read_csv("datasets/unit_concern.csv", encoding='latin-1')
unit_concern = unit_concern.fillna(0)

unit_collection_point = pd.read_csv("datasets/unit_collection_point.csv", encoding='latin-1')
unit_collection_point = unit_collection_point.fillna(0)
#json does not accept numpy data types, need to change from them to python data types...


# le cambio el nombre para escribir menos
uc = unit_concern
ucp = unit_collection_point


convert_dict_unitconcern = {
            'qls_unit_id':int,
            # 'unit_collection_pt_timestamp':datetime,
            'collection_point_id':int,
            'concern_id':int,
            'zone_charged_id':int,
            }

convert_dict_unitcollection = {
            'qls_unit_id':int,
            # 'unit_collection_pt_timestamp':datetime,
            'collection_point_id':int,
            'zone_id':int,
            }

# unit_concern = unit_concern.astype(convert_dict_unitconcern)


################################################
#creo funcion convert para convertir a int los numpy

#encoder => numpy types are not json serializable

def convert(o):
    if isinstance(o, np.int64): return int(o)
    elif isinstance(o, np.int32): return int(o)
    elif isinstance(o, np.datetime64): return datetime(o)
    elif isinstance(o, np.integer): return float(o)
    elif isinstance(o, np.ndarray): return o.tolist()
    

columns_uc = list(uc)
columns_ucp = list(ucp)


dfuc = pd.DataFrame({'key':'unit_concern','timestamp':uc['unit_collection_pt_timestamp']})
dfucp = pd.DataFrame({'key':'unit_collection_point','timestamp':ucp['unit_collection_pt_timestamp']})
df=pd.concat([dfuc,dfucp])
df.sort_values(by=['timestamp'], inplace=True)
df = df.reset_index(drop=True)

for i in range(len(df)):
    if df.key[i] == 'unit_collection_point':
        concern = dict(zip(columns_ucp, ucp.astype(convert_dict_unitcollection).iloc[i,:].tolist())) #convierto aca los tipos
        print(concern)
    elif df.key[i] == 'unit_concern':
        concern = dict(zip(columns_uc, uc.astype(convert_dict_unitconcern).iloc[i,:].tolist())) #convierto aca los tipos
        print(concern)
        
    if concern['collection_point_id'] > 50:
        print('alto')
#         qls_json = concern.select(from_json(F.col("value").cast("string"), schema, json_options).alias("content"))
#         qls = qls_json.select("content.*")
#         qls = qls.select("qls_unit_id", "unit_collection_pt_timestamp","collection_point_id")
    elif concern['collection_point_id'] <= 50:
        print('bajo')
    
       
    sleep(.2)