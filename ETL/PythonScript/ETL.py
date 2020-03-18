import cx_Oracle
import os
import pandas as pd
import shutil
import sys
import tkinter as tk

root = tk.Tk()

canvas1 = tk.Canvas(root, width = 600, height = 400)
canvas1.pack()

label1 = tk.Label(root, text='Running ETL...')
canvas1.create_window(150, 150, window=label1)



sourceFilePath = 'D:\\Projects\\OneDrive_eGeneration_Limited\\16. Navana\\ETL\\Data'
extFilePath = 'D:\\Projects\\OneDrive_eGeneration_Limited\\16. Navana\\ExternalFile'

def getFileName(path):
    for filename in os.listdir(path):
        if filename.endswith(".csv"):
            return filename



def moveFileToExternal():
    if len(os.listdir(extFilePath)) == 0:
        print("External File Directory Empty!!")
    elif len(os.listdir(extFilePath)) > 0:
        for filename in os.listdir(extFilePath):
            if filename.endswith(".csv"):
                print("Deleting file...")
                os.unlink(filename)

    srcFileName = getFileName(sourceFilePath)
    print(srcFileName)
    shutil.move(sourceFilePath+ '/' + srcFileName, extFilePath)
    print("***File Moved To External File Directory!***")
    


def loadEXT_LND():
    dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='oracle')
    con = cx_Oracle.connect(user='etl', password='etl', dsn = dsn_tns)

    cur = con.cursor()

    cur.execute('select status from process_log where prc_id = 2')
    res = cur.fetchone()
    print(res)
    if res[0] == 'N':
        print("***Executing LOAD into LND_NAVANA***")
        cur.execute('insert into LND_NAVANA select * from EXT_NAVANA')
        print("Done Loading")
        cur.execute('commit')

    cur.close()
    con.close()




def loadLND_STG():
    dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='oracle')
    con = cx_Oracle.connect(user='etl', password='etl', dsn = dsn_tns)

    cur = con.cursor()

    cur.execute('select status from process_log where prc_id = 2')
    res = cur.fetchone()
    print(res)
    if res[0] == 'Y':
        print("***Loading Data into: STG_NAVANA***")
        cur.execute(
            """MERGE INTO STG_NAVANA STG
                USING LND_NAVANA LND 
                ON (STG.n_id = LND.n_id)
                WHEN MATCHED THEN
                    UPDATE SET  
                    STG.prtclr = LND.prtclr,
                    STG.yr = LND.yr, 
                    STG.mon = LND.mon, 
                    STG.dsc = LND.dsc,
                    STG.uom = LND.uom, 
                    STG.qty = LND.qty, 
                    STG.dt = LND.dt, 
                    STG.tgt_qty = LND.tgt_qty
                WHEN NOT MATCHED THEN
                    INSERT (STG.n_id, STG.prtclr, STG.yr, STG.mon, STG.dsc, STG.uom, STG.qty, STG.dt, STG.tgt_qty)
                    VALUES (LND.n_id, LND.prtclr, LND.yr, LND.mon, LND.dsc, LND.uom, LND.qty, LND.dt, LND.tgt_qty)""")

        print("**Done Loading**")
        cur.execute('commit')

    cur.close()
    con.close()


def loadSTG_DW():
    dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='oracle')
    con = cx_Oracle.connect(user='etl', password='etl', dsn = dsn_tns)

    cur = con.cursor()

    cur.execute('select status from process_log where prc_id = 3')
    res = cur.fetchone()
    print(res)
    if res[0] == 'Y':
        print("***Loading Data into: DW_NAVANA***")
        cur.execute('insert into DW_NAVANA select * from STG_NAVANA ')

        print("**Done Loading**")
        cur.execute('commit')

    cur.close()
    con.close()



#print("Source File: ", getFileName(sourceFilePath))


moveFileToExternal()
loadEXT_LND()
loadLND_STG()
loadSTG_DW()



def close_window ():
    root.destroy()

button = tk.Button(text = "Quit", command = close_window)
button.pack()
root.mainloop()





