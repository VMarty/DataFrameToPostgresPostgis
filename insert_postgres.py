# -*- coding: utf-8 -*-
"""
@author: Victor MARTY-JOURJON
license = "MIT"
"""


import numpy as np
import psycopg2


def insert_postgres(schemas,table, db, geom, sird, geomtype, geomdim):  
    

    """ 
    schemas='public'    
    table='temp'             :nom de la table dans la base de données postgres (schéma public) (attention!! : si la table existe déjà, elle sera remplacée !!!!)
    db=test                :nom de la table (data frame pandas) à incérer
    geom='geom'        :nom de la colonne géométrie (si pas de géométrie:'')
    sird='2154'               :sytéme de coordonnées de la colonne géométrie
    geomtype='LINESTRING'     :type de géométrie
    geomdim=3                 :nombre de dimension de la géométrie
    """
    #####################################################################################################
    #connexion à la base de données
    #####################################################################################################
	
    params = {
    'database': 'WRITE_database',
    'user': 'WRITE_user',
    'password': 'WRITE_password',
    'host': 'WRITE_host',
    'port': 0000}
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    li1="index serial PRIMARY KEY,"
    for i in range(len(list(eval('db').columns.values))):
        li1=li1+"%s %s,"%(eval('db').columns.values[i], eval('db').dtypes[i])
    li1=li1.replace("object", "varchar")
    li1=li1.replace("float64", "float")
    li1=li1.replace("int64", "integer")
    li1=li1.replace("int32", "integer")
    li1=li1.replace("numpy.int64", "integer")
    li1=li1[:-1]
    
    #transformation des colonnes de type int64 (non traités par postqres) en float    
    dbint64=eval('db').select_dtypes(include=[np.int64]).columns
    for i in range(len(dbint64)):
        print (dbint64[i])
        eval('db')[dbint64[i]]=eval('db')[dbint64[i]].astype(float)    
		
    #transformation des colonnes de type int64 (non traités par postqres) en float    
    dbint32=eval('db').select_dtypes(include=[np.int32]).columns
    for i in range(len(dbint32)):
        print (dbint32[i])
        eval('db')[dbint32[i]]=eval('db')[dbint32[i]].astype(float)    
		
		
    if len(geom)>0:#si géométrie
        li2="INSERT INTO %s.%s (geom0,"%(schemas,table)
    else:
        li2="INSERT INTO %s.%s ("%(schemas,table)
    for i in range(len(list(eval('db').columns.values))):
        li2=li2+"%s,"%(eval('db').columns.values[i])
    li2=li2[:-1]
    
    if len(geom)>0:#si géométrie
        li2=li2+") VALUES (ST_SetSRID(%(geom0)s::geometry,%(srid)s), %(" 
    else:
        li2=li2+") VALUES (%(" 
        
    for i in range(len(list(eval('db').columns.values))):
        li2=li2+"%s)s,"%(eval('db').columns.values[i])
        li2=li2+" %("
    li2=li2[:-4]   
    li2=li2+")"
    
    if len(geom)>0:#si géométrie
        li3="{'geom0':"+'db'+"['%s'][i], 'srid': %s,"%(geom,eval('sird'))
    else:
        li3="{"
        
    for i in range(len(list(eval('db').columns.values))):
        li3=li3+"'%s'"%(eval('db').columns.values[i])+":"+'db'+"['%s'][i],"%(eval('db').columns.values[i])
    li3=li3[:-1]
    li3=li3+"}"
    
    
    #insertion des données dans postgres lignes par lignes
    cur.execute("DROP TABLE IF EXISTS %s.%s" %(schemas,table)) #supression de la table postgre s'il elle existe 
    cur.execute("CREATE TABLE %s.%s (%s);" %(schemas,table,li1)) #création de la nouvelle table

    if len(geom)>0: # ajout d'une colonne géométrie si l'entrée geom non vide
        cur.execute("SELECT AddGeometryColumn('%s','%s', 'geom0', %s, '%s', %s );"%(schemas,table,eval('sird'),geomtype,geomdim))
    for i in range(len(eval('db'))): # insertion des données lignes par lignes
        cur.execute("%s"%(li2),eval(li3))
    conn.commit()  #commit 

