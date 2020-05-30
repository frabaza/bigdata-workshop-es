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


convert_dict_unitconcern = {
            'qls_unit_id':int,
            # 'unit_collection_pt_timestamp':datetime,
            'unit_collection_pt_timestamp_s':int,
            'unit_concern_id':int,
            'collection_point_id':int,
            'collection_point_section_id':int,
            'ii_product_line_group_id':int,
            'inspection_item_group_id':int,
            'concern_id':int,
            'zone_charged_id':int,
            'department_charged_id':int,
            'area_charged_id':int,
            'plant_unit_location_id':int,
            'unit_concern_state':str,
            'evaluation_comment':str,
            'unit_concern_source':str,
            'online_repair':str
            }

convert_dict_unitcollection = {
            'qls_unit_id':int,
            # 'unit_collection_pt_timestamp':datetime,
            'unit_collection_pt_timestamp_s':int,
            'collection_point_id':int,
            'collection_point_section_id':int,
            'collector_id':int
            }

# unit_concern = unit_concern.astype(convert_dict_unitconcern)


################################################
#creo funcion convert para convertir a int los numpy


def convert(o):
    if isinstance(o, np.int64): return int(o)
    # elif isinstance(o, np.datetime64): return datetime(o)


columns = list(unit_concern) 

for i in range(len(unit_concern)):
    concern = dict(zip(columns, unit_concern.astype(convert_dict_unitconcern).iloc[i,:].tolist())) #convierto aca los tipos
    print(concern)
    sleep(2)