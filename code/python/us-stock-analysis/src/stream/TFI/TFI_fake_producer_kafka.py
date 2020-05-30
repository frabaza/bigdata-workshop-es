from time import sleep
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
import json
import sys
import numpy as np


if __name__ == '__main__':
    # Initialization
    args = sys.argv

    if len(args) != 3:
        print(f"""
        |Usage: {args[0]} <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topic> one kafka topic to produce to
        | 
        |  Example: {args[0]} kafka:9092 QLS
        """)
        sys.exit(1) 
    _, brokers, topic = args

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

    #Creo un df con la tabla y el timestamp y lo ordeno, de esa manera simulo como se envian los mensajes
    #cronologicamente a la base de datos de acuerdo a como ocurren

    dfuc = pd.DataFrame({'key':'unit_concern','timestamp':uc['unit_collection_pt_timestamp']})
    dfucp = pd.DataFrame({'key':'unit_collection_point','timestamp':ucp['unit_collection_pt_timestamp']})
    df=pd.concat([dfuc,dfucp])
    df.sort_values(by=['timestamp'], inplace=True)
    df = df.reset_index(drop=True)
    

    # df.to_csv("datasets/timestamps.csv", index = False)  

    ################################################
    #creo funcion convert para convertir a int los numpy
    
    
    def convert(o):
        if isinstance(o, np.int64): return int(o)
        # elif isinstance(o, np.datetime64): return datetime(o)
        elif isinstance(o, np.int32): return int(o)
        elif isinstance(o, np.datetime64): return datetime(o)
        elif isinstance(o, np.integer): return float(o)
        elif isinstance(o, np.ndarray): return o.tolist()

    producer = KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v,default = convert).encode('utf-8'), \
        key_serializer = str.encode) 
    

    # columns = list(unit_concern) 
    
    # for i in range(len(unit_concern)):
    #     concern = dict(zip(columns, unit_concern.astype(convert_dict_unitconcern).iloc[i,:].tolist())) #convierto aca los tipos
    #     producer.send(topic,key = "unit_concern", value=concern)
    #     print(concern)
    #     sleep(2)

    columns_uc = list(uc)
    columns_ucp = list(ucp)

    for i in range(len(df)):
        if df.key[i] == 'unit_collection_point':
            concern = dict(zip(columns_ucp, ucp.astype(convert_dict_unitcollection).iloc[i,:].tolist())) #convierto aca los tipos
            producer.send(topic,key = "unit_collection_point", value=concern)
            print(concern)
            sleep(.5)
        elif df.key[i] == 'unit_concern':
            concern = dict(zip(columns_uc, uc.astype(convert_dict_unitconcern).iloc[i,:].tolist())) #convierto aca los tipos
            producer.send(topic,key = "unit_concern", value=concern)
            print(concern)
            sleep(.5)
    
       
    