################################################
#Formateador de tablas de QLS
################################################

# Abrirlo desde QLS_TFI (por el path...)

import pandas as pd

df = pd.read_csv('datasets/backup/unit_concern.csv', encoding = 'latin-1')

df = df[['qls_unit_id',
'unit_collection_pt_timestamp',
'unit_collection_pt_timestamp_s',
'unit_concern_id',
'collection_point_id',
'collection_point_section_id',
'ii_product_line_group_id',
'inspection_item_group_id',
'concern_id',
'zone_charged_id',
'department_charged_id',
'area_charged_id',
'plant_unit_location_id',
'unit_concern_state',
'evaluation_comment',
'unit_concern_source',
'online_repair']]

df.to_csv("datasets/unit_concern.csv", index = False)

df = pd.read_csv('datasets/backup/unit_collection_point.csv', encoding = 'latin-1')

df = df[['qls_unit_id',
 'unit_collection_pt_timestamp',
 'unit_collection_pt_timestamp_s',
 'collection_point_id',
 'collection_point_section_id',
 'collector_id']]

df.to_csv("datasets/unit_collection_point.csv", index = False)

df = pd.read_csv("datasets/backup/unit_concern_rep_anls.csv", encoding = 'latin-1')

df = df[['qls_unit_id',
 'unit_collection_pt_timestamp',
 'unit_collection_pt_timestamp_s',
 'unit_concern_id',
 'unit_repair_timestamp',
 'unit_repair_timestamp_s',
 'repair_analysis_id',
 'ii_causal_part_id',
 'concern_id',
 'zone_id',
 'department_id',
 'area_id',
 'repair_comment',
 'unit_concern_rep_anls_source']]

df.to_csv("datasets/unit_concern_rep_anls.csv", index = False)

