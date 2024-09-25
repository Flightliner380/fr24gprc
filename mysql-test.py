import os
import pandas as pd
import io
# import sqlite3
import csv
from sqlalchemy import create_engine, event, text
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import insert

def insert_sqlite (df, db_filename):
    # Generate database filename with current date
    # current_date = datetime.now().strftime("%Y-%m")
    # db_filename = f"fr24-{current_date}.db"

    # Check if the database file exists
    if not os.path.exists(db_filename):
        print(f"Database '{db_filename}' does not exist. Creating a new database.")
        create_table = True
    else:
        print(f"Database '{db_filename}' already exists.")
        create_table = False

    # Create a SQLite database connection
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()


    # Create a table if it doesn't exist
    if create_table:
        with open('/home/ubuntu/test/fr24gprc/create_fr.sql', 'r') as file:
            create_table_sql = file.read()
        cursor.executescript(create_table_sql)
        cursor.executescript("CREATE UNIQUE INDEX IF NOT EXISTS ymd_hex_cs ON fr24 (ymd, frid, gnd) ")
        print("Table created successfully.")


    # "INSERT INTO tar (ymd,fcontact,lcontact, hex,reg,ac,cs,iso,flat,flon,llat,llon,gnd,fspeed,fhdg,falt,lalt,fvert,fsq,frdr,flt,al) 
    # VALUES (@ymd,@ts,@ts,@hex,@r,@t,@flight,@iso,@lat,@lon,@lat,@lon,@gnd,@gs,@track,@alt_baro,@alt_baro,@baro_rate,@squawk,@type,@flt,@al)  
    # ON CONFLICT (ymd,hex,cs,gnd) DO UPDATE SET lcontact=@ts, lalt=@alt_baro, llat=@lat, llon=@lon, lspeed=@gs, lrdr=@type, lvert=@baro_rate, lalt=@alt_baro, lhdg=@track, lsq=@squawk"
    # Define the insert statement
    insert_stmt = '''
    INSERT INTO fr24 (
        ymd, frid, fcontact, lcontact, hex, reg, ac, cs, iso, flat, flon, llat, llon, gnd, fspeed, fhdg, falt, lalt, fvert, fsq, frdr, flt, al, dep, arr
    ) 
    VALUES (?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (ymd,frid,gnd) DO UPDATE SET lcontact=?, lalt=?, llat=?, llon=?, lspeed=?, lrdr=?, lhdg=? ? ? ? ?
    '''

    uvert=""
    usq=""
    uarr=""
    udep=""

    exist_vert = 'vert' in df.columns
    exist_sq = 'squark' in df.columns
    # Execute the insert statement for each row in DataFrame
    for index, row in df.iterrows():
        if exist_vert and row['vert'] !='':
            uvert=f", lvert= '{row['vert']}'" 
         
        if exist_sq and row['sq'] !='':
            usq=f", lsq= '{row['sq']}'"
        if row['arr'] !='' :
            uarr=f", arr= '{row['arr']}'"
        if row['dep']!='':
            udep=f", dep= '{row['dep']}'"
        
        print(uvert,usq,udep,uarr)
        try:
            cursor.execute(insert_stmt, (
                row['ymd'], row['frid'], row['ts'], row['ts'], row['hex'], row['reg'], row['ac'], row['cs'], row['iso'],
                row['lat'], row['lon'], row['lat'], row['lon'], row['gnd'], row['speed'], row['hdg'], row['alt'],
                row['alt'], '0', '0', row['source'], row['flight'], row['al'], row['dep'],row['arr'],
                row['ts'],row['alt'],row['lat'], row['lon'],row['speed'],row['source'], row['hdg'], uvert, usq, uarr, udep
            ))
        except sqlite3.Error as er:
            print( "SQlite Error: ", er)
            return False
        #     # print("Error %s", err.sqlite_errorcode)
            # print(err.sqlite_errorname)
      #  print(cursor.lastrowid)
    # Commit the transaction
    conn.commit()
    return 
def round_5 ( v ):
    return round(v,5)
def mysql_fr24( df ):
    fr24db="k53371_fp.fr24"


    # db_mysql_str = 'mysql+pymysql://k53371_dump:We7632r!@nclink.hvnt.de/k53371_fp'
    db_mysql_str = 'mysql+pymysql://acarsd:We7632r!@10.0.0.178/k53371_fp'
    engine = create_engine(db_mysql_str, echo=True)#,echo='debug')
    # @event.listens_for(engine, "connect", insert=True)
    # try:
    #     df = pd.read_sql('Select max(lcontact) as lcontact from k53371_fp.fr24;', con=db_connection)
    # except SQLAlchemyError  as err:
    #     # print(err)
    #     print(f"Ein Fehler ist aufgetreten: {err}")
    #     return false
    
    # max= df.iloc[0].lcontact or  0
    # ymd = datetime.now().strftime("%Y-%m-%d")
    # print(max)
    # print(ymd)

    # #print(df)
    # df.fillna("",inplace=True)
    # df['id']=""
    # vsql=""
    # i=0
    # # print(df)
    
    # df.drop('timestamp',axis=1, inplace=True)
    # df.drop('fnear',axis=1, inplace=True)
    # df.drop('lnear',axis=1, inplace=True)
    # df.drop('vspeed',axis=1, inplace=True)
    # df.drop('squawk',axis=1, inplace=True)
    # print(df)
    print(df.head(3))

    sql=f"INSERT INTO {fr24db} "
    sql +=" (`frid`,`ymd`,`fcontact`,`hex`,`reg`,`ac`,`cs`,`iso`,`flon`,`flat`,`falt`,`fhdg`,`fspeed`,`fvert`,`fsq`,`gnd`,`flt`,`al`,`dep`,`arr`,`frdr`) "
    #sql +=" VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) as nv "
    sql +=" VALUES %s as nv "
    sql +=" ON DUPLICATE KEY UPDATE lcontact=nv.fcontact, llon=nv.flon, llat=nv.flat, lalt=nv.falt, lhdg=nv.fhdg, lspeed=nv.fspeed, lvert=nv.fvert, lsq=nv.fsq, lrdr=nv.frdr"

    print(sql)
    
    
    
    vsql=[]
    dtx=df
    #dtx=df.head(3)
    
    s=io.StringIO()
    #dtx.to_csv(s,header=False,index=False,quoting=csv.QUOTE_ALL)

    # print (s.getvalue().split('\n'))
    for index, row in dtx.iterrows():
         
         #row.to_csv(s        ,header=False,index=False,quoting=csv.QUOTE_ALL,sep='|')
         x=row.to_list()
        #  print (type(x))
         #print ("Row"+ s.getvalue())
         vsql.append ( '("' + '","'.join(map(str,x)) +'")' )
        #  stmt= insert("fr24").values(row)
        #  print(stmt)
        #  print ( x.join("','"))

    
    # print('\n'.join(vsql))
    sx= ','.join(vsql)
    sf= sql % ','.join(vsql)
    #print(type(sx))
    vdata=[]
    vdata.append(sx)
#    try:
    #sx='SELECT * from fr24'
    #cursor = engine.raw_connection() 
    # print("SX:", sf)
    cursor = engine.connect() 
    #c= cursor.cursor()
    status=cursor.execute(text(sf))
    print(status)
    cursor.commit()
    cursor.close()
    # except SQLAlchemyError  as err:
    #      print(err)
    #      print(f"Ein Fehler ist aufgetreten: {err}")


    # cursor.executemany(sql,dtx.values.tolist())
    # dtx=df.head(3)
    # print(dtx.values.tolist())
    
    # for index, row in df.iterrows():
    #     # print(row['frid'],row['cs'])
    #     # print(row.values.tolist())
    #     # print("------------------")
    #     row = ', '.join([f"'{str(item)}'" if isinstance(item, str) else str(item) for item in row]) 
        
    #     sql = "INSERT INTO {fr24db}"
    #     # sql += 
    #     sql +=" (`id`,`frid`,`ymd`,`addr`,`fcontact`,`lcontact`,`hex`,`reg`,`ac`,`cs`,`iso`,`flon`,`flat`,`llon`,`llat`,`falt`,`lalt`,`fhdg`,`lhdg`,`fspeed`,`lspeed`,`fvert`,`lvert`,`fsq`,`lsq`,`gnd`,`flt`,`al`,`dep`,`arr`,`frdr` ,`lrdr`,`fnear`,`lnear`) "
    #     sql += " VALUES (%s) as nv"
    #     #v=row.to_string(index=False)
    #     vsql=vsql+row+"\n"
    #     #vsql.append(row)
    #     #print( index)
    #     if ( index % 10 ==0 ):
    #         print(index)
    #         insert_stmt= f"{sql} ",vsql
    #         #sql=vsql
    #         print(insert_stmt)
    #         exit()
        #print (row)
    #sql +=
    # row=df.iloc[0]
    # row=row.to_string(index=False)
    # row=row.strip().replace('\n',"','").replace('\t','')

    

    # print(vsql[1:3].join("\n"))
    #  " ON DUPLICATE KEY UPDATE  lcontact=nv.lcontact, llat =nv.llat, llon=nv.llon, lalt=nv.lalt,lhdg=nv.lhdg,lspeed=nv.lspeed,lvert=nv.lvert,lsq=nv.lsq,lrdr=nv.lrdr, lnear=k53371_fp.nb(nv.llat,nv.llon),dep=nv.dep,arr=nv.arr";
    #print(vsql);
    
# # # Query to check the inserted data
# cursor.execute("SELECT frid,gnd,cs FROM fr24 where gnd=1 order by cs")
# rows = cursor.fetchall()

# # # Print the results
# # print("\nInserted data:")
# for row in rows:
#     print(row)

# # Close the cursor and connection
# cursor.close()
# conn.close()

# exit()

current_date = datetime.now().strftime("%Y-%m")
db_filename = f"fr24-{current_date}.db"


#df=pd.read_parquet('/home/ubuntu/fr24_20240925-073209.parq')
df=pd.read_parquet('/home/ubuntu/fr24_20240925-150504.parq')
df['gnd']=df['gnd'].astype(int)

df['lat']=df['lat'].apply(round_5)
df['lon']=df['lon'].apply(round_5)
df1=df[['frid','ymd','ts','hex','reg','ac','cs','iso','lon','lat','alt','hdg','speed','vspeed','squawk','gnd','flight','al','dep','arr','source']]
# Remove GRND from dataset
#df1=df1.drop(df1[(df['ac']=='GRND')].index)
print(df)
# df.drop('timestamp',axis=1, inplace=True)
# df.drop('fnear',axis=1, inplace=True)
# df.drop('lnear',axis=1, inplace=True)
# df.drop('vspeed',axis=1, inplace=True)
# df.drop('squawk',axis=1, inplace=True)
print(df1)

#mysql_fr24(df1)


# print(df.values.tolist())
# insert_sqlite(df, db_filename)
# current_date = datetime.now().strftime("%Y-%m")
# db_filename = f"fr24-{current_date}.db"

# cnx=sqlite3.connect(db_filename)
# df = pd.read_sql_query("SELECT * FROM fr24", cnx)

# print(df)
#mysql_fr24(db_filename)
exit();
