# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
PROGRAM NAME:     etl_iabs_bdt.py                                  
LOCATION:                      
PROGRAMMER(S):    Jason Yelverton                                
DESCRIPTION:      IABS Extract/Transformation/Load program to load legacy Q historical BDT data.

LANGUAGE/VERSION: Python/2.7.10                                          
INITIATION DATE:  x/x/2016                                                
INPUT FILE(S):    xxxxxxxxxxxxxxxxxxxxxxxxxxx (extract from Qwest focus database)
LOCATION:         
                                                                         
OUTPUT:           ORACLE DWBS001P                                
                  Table names IABS_BDT_*
                                                                         
EXTERNAL CALLS:                                         
                                                                         
LOCATION:         /home/caimsown/etl
       
                                                                         
Copyright 2016, CenturyLink All Rights Reserved. Unpublished and          
Confidential Property of CenturyLink.                                          
                                                                         
CONFIDENTIAL: Disclose and distribute solely to CenturyLink employees
having a need to know.
=========================================================================
                 R E V I S I O N      H I S T O R Y                      
=========================================================================
PROGRAMMER:                                   DATE:        
VALIDATOR:                                    DATE:                      
REASON:  

"""

import datetime
startTM=datetime.datetime.now()
import cx_Oracle
import sys
import ConfigParser
import os
import platform
import copy
import csv

###IMPORT COMMON/SHARED UTILITIES
from etl_caims_cabs_utility import  process_insert_many_table, writelog, db_record_exists, \
                                   createTableTypeDict,setDictFields, clean_focus_input

settings = ConfigParser.ConfigParser()
settings.read('settings.ini')
settings.sections()

"SET ORACLE CONNECTION"
con=cx_Oracle.connect(settings.get('OracleSettings','OraCAIMSUser'),settings.get('OracleSettings','OraCAIMSPw'),settings.get('OracleSettings','OraCAIMSSvc'))
schema=settings.get('OracleSettings','OraCAIMSUser')
"CONSTANTS"

if str(platform.system()) == 'Windows': 
    OUTPUT_DIR = settings.get('GlobalSettings','WINDOWS_LOG_DIR');
else:
    OUTPUT_DIR = settings.get('GlobalSettings','LINUX_LOG_DIR');    
 

#SET FILE NAME AND PATH
#--inputPath comes from settings.ini
#--file name comes from command line parameter
#COMMAND LINE EXECUTION Example:
#python etl_caims_cabs_bdt.py PBCL.CY.XRU0102O.CABS.Oct14.txt

try:
    fileCycle=sys.argv[1] 
except:
   raise Exception("ERROR: No cycle passed to " + str(sys.argv[0]))
    
if fileCycle.rstrip(' ') == "":
    raise Exception("ERROR: File name passed to " + str(sys.argv[0]) +" is empty.")
else:
    print "fileCycle value is:" + str(fileCycle)

ROOTRECfileNm="IABSBIL_SEG_ROOTREC_"+fileCycle+".csv"
BALDUEfileNm="IABSBIL_SEG_BALDUE_"+fileCycle+".csv"
BALDTLfileNm="IABSBIL_SEG_BALDTL_"+fileCycle+".csv"
DISPDTLfileNm="IABSBIL_SEG_DISPDTL_"+fileCycle+".csv"
PMNTADJfileNm="IABSBIL_SEG_PMNTADJ_"+fileCycle+".csv"
ADJMTDTLfileNm="IABSBIL_SEG_ADJMTDTL_"+fileCycle+".csv"
CRNT1fileNm="IABSBIL_SEG_CRNT1_"+fileCycle+".csv"
CRNT2fileNm="IABSBIL_SEG_CRNT2_"+fileCycle+".csv"


if str(platform.system()) == 'Windows':
    inputPath=settings.get('IABS_BDTSettings','WINDOWS_IABS_BDT_inDir') 
else:
    inputPath=settings.get('IABS_BDTSettings','LINUX_IABS_BDT_inDir') 


#####THIS CAN BE PUT IN A LIST
ROOTREC_FILENM_AND_PATH=os.path.join(inputPath,ROOTRECfileNm)
BALDUE_FILENM_AND_PATH=os.path.join(inputPath,BALDUEfileNm)
BALDTL_FILENM_AND_PATH=os.path.join(inputPath,BALDTLfileNm)
DISPDTL_FILENM_AND_PATH=os.path.join(inputPath,DISPDTLfileNm)
PMNTADJ_FILENM_AND_PATH=os.path.join(inputPath,PMNTADJfileNm)
ADJMTDTL_FILENM_AND_PATH=os.path.join(inputPath,ADJMTDTLfileNm)
CRNT1_FILENM_AND_PATH=os.path.join(inputPath,CRNT1fileNm)
CRNT2_FILENM_AND_PATH=os.path.join(inputPath,CRNT2fileNm)

if os.path.isfile(ROOTREC_FILENM_AND_PATH):
    print ("ROOTREC file:"+ROOTREC_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+ROOTREC_FILENM_AND_PATH)

if os.path.isfile(BALDUE_FILENM_AND_PATH):
    print ("BALDUE file:"+BALDUE_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+ROOTREC_FILENM_AND_PATH)
    
if os.path.isfile(BALDTL_FILENM_AND_PATH):
    print ("BALDTL file:"+BALDTL_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+ROOTREC_FILENM_AND_PATH)
    
if os.path.isfile(DISPDTL_FILENM_AND_PATH):
    print ("DISPDTL file:"+DISPDTL_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+DISPDTL_FILENM_AND_PATH)
    
if os.path.isfile(PMNTADJ_FILENM_AND_PATH):
    print ("PMNTADJ file:"+PMNTADJ_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+PMNTADJ_FILENM_AND_PATH)    

if os.path.isfile(ADJMTDTL_FILENM_AND_PATH):
    print ("ADJMTDTL file:"+ADJMTDTL_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+ADJMTDTL_FILENM_AND_PATH)    

if os.path.isfile(CRNT1_FILENM_AND_PATH):
    print ("CRNT1 file:"+CRNT1_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+CRNT1_FILENM_AND_PATH)    

if os.path.isfile(CRNT2_FILENM_AND_PATH):
    print ("CRNT2 file:"+CRNT2_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+CRNT2_FILENM_AND_PATH)    
    
root_rec=False
 
"GLOBAL VARIABLES"
global IABS_BDT_IABSBIL_tbl, IABS_BDT_IABSBIL_DEFN_DICT
global IABS_BDT_BALDUE_tbl, IABS_BDT_BALDUE_DEFN_DICT
global IABS_BDT_BALDTL_tbl, IABS_BDT_BALDTL_DEFN_DICT
global IABS_BDT_DISPDTL_tbl, IABS_BDT_DISPDTL_DEFN_DICT
global IABS_BDT_PMNTADJ_tbl, IABS_BDT_PMNTADJ_DEFN_DICT
global IABS_BDT_ADJMTDTL_tbl, IABS_BDT_ADJMTDTL_DEFN_DICT
global IABS_BDT_CRNT1_tbl, IABS_BDT_CRNT1_DEFN_DICT
global IABS_BDT_CRNT2_tbl, IABS_BDT_CRNT2_DEFN_DICT

badKey=False
dup_ban_lock=False

iabsbil_key_concat=''       #ACNA|BAN|EOB_DATE                          :   ID (IABSBIL_ID)
baldue_key_concat=''        #IABSBIL_ID|INV_DATE|ASTATE|PREVBAL         :   ID (BALDUE_ID)
baldtl_key_concat=''        #BALDUE_ID|DINV_DATE|DSTATE                 :   ID (BALDTL_ID)
dispdtl_key_concat=''       #BALDTL_ID|DSP_AUDIT_NO                     :   ID (DISPDTL_ID)
pmntadj_key_concat=''       #BALDTL_ID|AINV_REF|AINV_DATE|APSTATE|SERIAL_NO  :   ID (PMNTADJ_ID) **PORA is not in the MFD as a key but load job matches on it
adjmtdtl_key_concat=''      #PMNTADJ_ID|ADJMT_SER_NO                    :   ID (ADJMNTDTL_ID)
crnt1_key_concat=''         #BALDTL_ID|INVDT1|SUBSTATE                  :   ID (CRNT1_ID)
crnt2_key_concat=''         #CRNT1_ID|ACC_TYP_IND|FAC_TYP_IND           :   ID (CRNT2_ID)

#Variables to hold sequence values for all table inserts
iabs_bdt_iabsbil_sq=0
iabs_bdt_baldue_sq=0
iabs_bdt_baldtl_sq=0
iabs_bdt_dispdtl_sq=0
iabs_bdt_pmntadj_sq=0
iabs_bdt_adjmtdtl_sq=0
iabs_bdt_crnt1_sq=0
iabs_bdt_crnt2_sq=0

record_counts={}
dup_record_counts={}

STAR_LINE='*********************************************************************************************'  

# Initialize Unique keys - KV - where Key is concatenated unique fields and value is sequence id #
# Child tables will have parent id
iabsbilKeys={}
baldueKeys={}
baldtlKeys={}
dispdtlKeys={}
pmntadjKeys={}
adjmtdtlKeys={}
crnt1Keys={}
crnt2Keys={}
##############################################
iabs_bdt_iabsbil_records={}
iabs_bdt_baldue_records={}
iabs_bdt_baldtl_records={}
iabs_bdt_dispdtl_records={}
iabs_bdt_pmntadj_records={}
iabs_bdt_adjmtdtl_records={}
iabs_bdt_crnt1_records={}
iabs_bdt_crnt2_records={}

iabsbil_insert_cnt=0
baldue_insert_cnt=0
baldtl_insert_cnt=0
dispdtl_insert_cnt=0
pmntadj_insert_cnt=0
adjmtdtl_insert_cnt=0
crnt1_insert_cnt=0
crnt2_insert_cnt=0

results={}
cntr=0
inscnt=0
rows=[]
rootrecHdrLst=[]
rootrecDtlLst=[]
baldueHdrLst=[]
baldueDtlLst=[]
baldtlHdrLst=[]
baldtlDtlLst=[]
dispdtlHdrLst=[]
dispdtlDtlLst=[]
pmntadjHdrLst=[]
pmntadjDtlLst=[]
adjmtdtlHdrLst=[]
adjmtdtlDtlLst=[]
crnt1HdrLst=[]
crnt1DtlLst=[]
crnt2HdrLst=[]
crnt2DtlLst=[]

insql=""

"TRANSLATERS"

def init():
    global output_log
    global IABS_BDT_IABSBIL_tbl, IABS_BDT_IABSBIL_DEFN_DICT
    global IABS_BDT_BALDUE_tbl, IABS_BDT_BALDUE_DEFN_DICT
    global IABS_BDT_BALDTL_tbl, IABS_BDT_BALDTL_DEFN_DICT
    global IABS_BDT_DISPDTL_tbl, IABS_BDT_DISPDTL_DEFN_DICT
    global IABS_BDT_PMNTADJ_tbl, IABS_BDT_PMNTADJ_DEFN_DICT
    global IABS_BDT_ADJMTDTL_tbl, IABS_BDT_ADJMTDTL_DEFN_DICT
    global IABS_BDT_CRNT1_tbl, IABS_BDT_CRNT1_DEFN_DICT
    global IABS_BDT_CRNT2_tbl, IABS_BDT_CRNT2_DEFN_DICT

    "  CREATE LOG FILE WITH CYCLE DATE FROM HEADER AND RUN TIME OF THIS JOB"
    
    log_file=os.path.join(OUTPUT_DIR,"IABS_LOG"+startTM.strftime("%Y%m%d_@%H%M") +".txt")
         
    output_log = open(log_file, "w")
    output_log.write("-CSR CAIMS ETL PROCESS-")

    "Set initial values for all sequences"
    initialize_sequences()
    
    IABS_BDT_IABSBIL_DEFN_DICT=createTableTypeDict('IABS_BDT_IABSBIL',con,schema,output_log) 
    IABS_BDT_IABSBIL_tbl=setDictFields('IABS_BDT_IABSBIL', IABS_BDT_IABSBIL_DEFN_DICT) 
    
    IABS_BDT_BALDUE_DEFN_DICT=createTableTypeDict('IABS_BDT_BALDUE',con,schema,output_log) 
    IABS_BDT_BALDUE_tbl=setDictFields('IABS_BDT_BALDUE', IABS_BDT_BALDUE_DEFN_DICT)
    
    IABS_BDT_BALDTL_DEFN_DICT=createTableTypeDict('IABS_BDT_BALDTL',con,schema,output_log) 
    IABS_BDT_BALDTL_tbl=setDictFields('IABS_BDT_BALDTL', IABS_BDT_BALDTL_DEFN_DICT)    
    
    IABS_BDT_DISPDTL_DEFN_DICT=createTableTypeDict('IABS_BDT_DISPDTL',con,schema,output_log) 
    IABS_BDT_DISPDTL_tbl=setDictFields('IABS_BDT_DISPDTL', IABS_BDT_DISPDTL_DEFN_DICT)
    
    IABS_BDT_PMNTADJ_DEFN_DICT=createTableTypeDict('IABS_BDT_PMNTADJ',con,schema,output_log) 
    IABS_BDT_PMNTADJ_tbl=setDictFields('IABS_BDT_PMNTADJ', IABS_BDT_PMNTADJ_DEFN_DICT) 

    IABS_BDT_ADJMTDTL_DEFN_DICT=createTableTypeDict('IABS_BDT_ADJMTDTL',con,schema,output_log) 
    IABS_BDT_ADJMTDTL_tbl=setDictFields('IABS_BDT_ADJMTDTL', IABS_BDT_ADJMTDTL_DEFN_DICT)
    
    IABS_BDT_CRNT1_DEFN_DICT=createTableTypeDict('IABS_BDT_CRNT1',con,schema,output_log) 
    IABS_BDT_CRNT1_tbl=setDictFields('IABS_BDT_CRNT1', IABS_BDT_CRNT1_DEFN_DICT) 
 
    IABS_BDT_CRNT2_DEFN_DICT=createTableTypeDict('IABS_BDT_CRNT2',con,schema,output_log) 
    IABS_BDT_CRNT2_tbl=setDictFields('IABS_BDT_CRNT2', IABS_BDT_CRNT2_DEFN_DICT) 
    

 
    
def main():

    global output_log
    
    process_ROOTREC_segment()   
    process_inserts()
    process_BALDUE_segment()
    process_inserts()
    process_BALDTL_segment()
    process_inserts()
    process_DISPDTL_segment()
    process_inserts()
    process_PMNTADJ_segment()
    process_inserts()
    process_ADJMTDTL_segment()
    process_inserts()
    process_CRNT1_segment()
    process_inserts()
    process_CRNT2_segment()
    #if dup_record_counts['CRNT2'] and dup_record_counts['CRNT2'] > 0:
    #    print('CRNT2 Record dups...no insert')
    #else:
    #    process_inserts()
    process_inserts()

    reset_record_flags()
###############################################main end#########################################################
def process_ROOTREC_segment():
    print "Processing ROOTREC_segment"

    global output_log
    global banlock
    global output_log
    global ROOTREC_FILENM_AND_PATH
    global rootrecHdrLst, rootrecDtlLst, pmntadjHdrLst, pmntadjDtlLst
    global IABS_BDT_IABSBIL_tbl, IABS_BDT_IABSBIL_DEFN_DICT
    global rootrec_hdr_cnt    
    global ROOTREC_key_concat
    global iabs_bdt_iabsbil_records
    global iabs_bdt_iabsbil_sq
    global iabsbilKeys    
    
    rootrecFile = open(ROOTREC_FILENM_AND_PATH)    
    rootrec_input = csv.reader(rootrecFile)

    firstRecord=True
    
    for rootrow in rootrec_input:
#STEP 1: GET COLUMN HEADERS
        if(firstRecord == True):            
            for rootrecHdr in rootrow:
                rootrecHdrLst.append(rootrecHdr)
            rootrec_hdr_cnt=len(rootrecHdrLst) 
            firstRecord=False
        else:
#STEP 2: GET VALUES OF THE FIELDS 
            for rootrecdtl in rootrow:
                rootrecDtlLst.append(rootrecdtl)
#STEP 3: ITERATE OVER THE VALUES IN THE LIST AND POPULATE THE TABLE DICTIONARY WITH THE VALUES                
            for i in range(len(rootrecHdrLst)):
                IABS_BDT_IABSBIL_tbl[rootrecHdrLst[i]]=clean_focus_input(IABS_BDT_IABSBIL_DEFN_DICT,rootrecHdrLst[i],rootrecDtlLst[i])
#STEP 4: SET ID = CURRENT SEQUENCE                
            IABS_BDT_IABSBIL_tbl['ID']=iabs_bdt_iabsbil_sq
#STEP 5: ADD THE UNIQUE KEY TO THE KEY DICTIONARY
            #ROOTREC UNIQUE KEY - ACNA|BAN|EOB_DATE
#STEP 6: CHECK TO SEE IF RECORD ALREADY EXISTS 
### - USING THE CYCLE DATE READ THE DATABASE AND POPULATE A DICTIONARY WITH ALL OF THE UNIQUE ACNA|BAN|EOB_DATE TO PREVENT MULITPLE DATABASE CALLS            
            #if db_record_exists('IABS_BDT_IABSBIL',IABS_BDT_IABSBIL_tbl['ACNA'],IABS_BDT_IABSBIL_tbl['BAN'],IABS_BDT_IABSBIL_tbl['EOB_DATE'], con, schema, output_log):
            #    writelog("WARNING: DUPLICATE BAN - ACNA="+IABS_BDT_IABSBIL_tbl['ACNA']+", BAN="+IABS_BDT_IABSBIL_tbl['BAN']+", EOB_DATE="+IABS_BDT_IABSBIL_tbl['EOB_DATE'], output_log)
            #else:
            tmpKey=str(IABS_BDT_IABSBIL_tbl['ACNA']) + str(IABS_BDT_IABSBIL_tbl['BAN']) + str(IABS_BDT_IABSBIL_tbl['EOB_DATE'])
            if tmpKey in iabsbilKeys:          
                print "WARNING IABSBIL DUPLICATE - " + tmpKey
                count_record('IABSBIL',True)
            else:
                iabsbilKeys[tmpKey]=iabs_bdt_iabsbil_sq
#STEP 7: ADD A COPY OF THE RECORD TO THE LIST OF RECORDS TO INSERT            
                iabs_bdt_iabsbil_records[iabs_bdt_iabsbil_sq]= copy.deepcopy(IABS_BDT_IABSBIL_tbl)
                count_record('IABSBIL',False)
#STEP 8: INCREMENT THE SEQUENCE BY 1            
                iabs_bdt_iabsbil_sq+=1
            initialize_tbl(IABS_BDT_IABSBIL_tbl)
            rootrecDtlLst=[]
  
        
    rootrecFile.close() 



def process_BALDUE_segment():
    print "Processing BALDUE_segment"

    global output_log
    global banlock
    global output_log
    global BALDUE_FILENM_AND_PATH
    global baldueHdrLst, baldueDtlLst
    global IABS_BDT_BALDUE_tbl, IABS_BDT_BALDUE_DEFN_DICT
    global baldue_hdr_cnt
    global iabs_bdt_baldue_records
    global iabs_bdt_baldue_sq
    global iabsbilKeys    
    global baldueKeys 
    
    baldueFile = open(BALDUE_FILENM_AND_PATH)    
    baldue_input = csv.reader(baldueFile)

    firstRecord=True
     
    for rootrow in baldue_input:
        if(firstRecord == True):            
            for baldueHdr in rootrow:
                baldueHdrLst.append(baldueHdr)
            baldue_hdr_cnt=len(baldueHdrLst) 
            firstRecord=False
        else:
            for balduedtl in rootrow:
                baldueDtlLst.append(balduedtl)
            for i in range(len(baldueHdrLst)):                
                IABS_BDT_BALDUE_tbl[baldueHdrLst[i]]=clean_focus_input(IABS_BDT_BALDUE_DEFN_DICT,baldueHdrLst[i],baldueDtlLst[i])
                
            IABS_BDT_BALDUE_tbl['ID']=iabs_bdt_baldue_sq
            
            parentKey=getRootrecKey(IABS_BDT_BALDUE_tbl)
            IABS_BDT_BALDUE_tbl['IABSBIL_ID']=iabsbilKeys[parentKey]
            tmpKey=str(IABS_BDT_BALDUE_tbl['IABSBIL_ID']) + str(IABS_BDT_BALDUE_tbl['INV_DATE']) + str(IABS_BDT_BALDUE_tbl['ASTATE']) + str(IABS_BDT_BALDUE_tbl['PREVBAL'])
            if tmpKey in baldueKeys:
                print "WARNING BALDUE DUPLICATE - " + tmpKey
                count_record('BALDUE',True)                
            else:
                baldueKeys[tmpKey]=iabs_bdt_baldue_sq     
                iabs_bdt_baldue_records[iabs_bdt_baldue_sq]= copy.deepcopy(IABS_BDT_BALDUE_tbl)
                count_record('BALDUE',False)          
                iabs_bdt_baldue_sq+=1
               
            initialize_tbl(IABS_BDT_BALDUE_tbl)
            baldueDtlLst=[]
        
    baldueFile.close()    
    
    
def process_BALDTL_segment():
    print "Processing BALDTL_segment"

    global output_log
    global banlock
    global output_log
    global BALDTL_FILENM_AND_PATH
    global baldtlHdrLst, baldtlDtlLst
    global IABS_BDT_BALDTL_tbl, IABS_BDT_BALDTL_DEFN_DICT
    global baldtl_hdr_cnt
    global iabs_bdt_baldtl_records
    global iabs_bdt_baldtl_sq
    global baldtlKeys    
    global baldueKeys 
    
    baldtlFile = open(BALDTL_FILENM_AND_PATH)    
    baldtl_input = csv.reader(baldtlFile)

    firstRecord=True    
    
    for rootrow in baldtl_input:
        #GET COLUMN HEADERS
        if(firstRecord == True):            
            for baldtlHdr in rootrow:
                baldtlHdrLst.append(baldtlHdr)
            baldtl_hdr_cnt=len(baldtlHdrLst) 
            firstRecord=False
        else:
            for baldtldtl in rootrow:
                baldtlDtlLst.append(baldtldtl)
            for i in range(len(baldtlHdrLst)):                
                IABS_BDT_BALDTL_tbl[baldtlHdrLst[i]]=clean_focus_input(IABS_BDT_BALDTL_DEFN_DICT,baldtlHdrLst[i],baldtlDtlLst[i])
            
            IABS_BDT_BALDTL_tbl['ID']=iabs_bdt_baldtl_sq
            
            parentKey=getBaldueKey(IABS_BDT_BALDTL_tbl)
            IABS_BDT_BALDTL_tbl['BALDUE_ID']=baldueKeys[parentKey]
            tmpKey=str(IABS_BDT_BALDTL_tbl['BALDUE_ID']) + str(IABS_BDT_BALDTL_tbl['DINV_DATE']) + str(IABS_BDT_BALDTL_tbl['DSTATE'])
            if tmpKey in baldtlKeys:
                print "WARNING BALDTL DUPLICATE - " + tmpKey
                count_record('BALDTL',True)                
            else:
                baldtlKeys[tmpKey]=iabs_bdt_baldtl_sq            
                iabs_bdt_baldtl_records[iabs_bdt_baldtl_sq]= copy.deepcopy(IABS_BDT_BALDTL_tbl)
                count_record('BALDTL',False)
                iabs_bdt_baldtl_sq+=1
                
            initialize_tbl(IABS_BDT_BALDTL_tbl)
            baldtlDtlLst=[] 
        
    baldtlFile.close()    

def process_DISPDTL_segment():
    print "Processing DISPDTL_segment"
#BALDTL_ID|DSP_AUDIT_NO
    global output_log
    global banlock
    global output_log
    global DISPDTL_FILENM_AND_PATH
    global dispdtlHdrLst, dispdtlDtlLst
    global IABS_BDT_DISPDTL_tbl, IABS_BDT_DISPDTL_DEFN_DICT
    global dispdtl_hdr_cnt
    global iabs_bdt_dispdtl_records
    global iabs_bdt_dispdtl_sq
    global baldtlKeys    
    global dispdtlKeys 
    
    dispdtlFile = open(DISPDTL_FILENM_AND_PATH)    
    dispdtl_input = csv.reader(dispdtlFile)

    firstRecord=True
        
    for rootrow in dispdtl_input:
        if(firstRecord == True):            
            for dispdtlHdr in rootrow:
                dispdtlHdrLst.append(dispdtlHdr)
            dispdtl_hdr_cnt=len(dispdtlHdrLst) 
            firstRecord=False
        else:
            for dispdtldtl in rootrow:
                dispdtlDtlLst.append(dispdtldtl)
            for i in range(len(dispdtlHdrLst)):                
                IABS_BDT_DISPDTL_tbl[dispdtlHdrLst[i]]=clean_focus_input(IABS_BDT_DISPDTL_DEFN_DICT,dispdtlHdrLst[i],dispdtlDtlLst[i])

            IABS_BDT_DISPDTL_tbl['ID']=iabs_bdt_dispdtl_sq        
            parentKey=getBaldtlKey(IABS_BDT_DISPDTL_tbl)
            IABS_BDT_DISPDTL_tbl['BALDTL_ID']=baldtlKeys[parentKey]

            tmpKey=str(IABS_BDT_DISPDTL_tbl['BALDTL_ID']) + str(IABS_BDT_DISPDTL_tbl['DSP_AUDIT_NO'])
            if tmpKey in dispdtlKeys:
                print "WARNING DISPDTL DUPLICATE - " + tmpKey
                count_record('DISPDTL',True)                
            else:
                dispdtlKeys[tmpKey]=iabs_bdt_dispdtl_sq          
                iabs_bdt_dispdtl_records[iabs_bdt_dispdtl_sq]= copy.deepcopy(IABS_BDT_DISPDTL_tbl)
                count_record('DISPDTL',False)       
                iabs_bdt_dispdtl_sq+=1
                
            initialize_tbl(IABS_BDT_DISPDTL_tbl)
            dispdtlDtlLst=[]
        
    dispdtlFile.close()    
        

def process_PMNTADJ_segment():
    print "Processing PMNTADJ_segment"
#BALDTL_ID|AINV_REF|AINV_DATE|APSTATE|PORA
    global output_log
    global banlock
    global output_log
    global PMNTADJ_FILENM_AND_PATH
    global pmntadjHdrLst, pmntadjDtlLst
    global IABS_BDT_PMNTADJ_tbl, IABS_BDT_PMNTADJ_DEFN_DICT
    global pmntadj_hdr_cnt
    global iabs_bdt_pmntadj_records
    global iabs_bdt_pmntadj_sq
    global baldtlKeys    
    global pmntadjKeys 
    
    pmntadjFile = open(PMNTADJ_FILENM_AND_PATH)    
    pmntadj_input = csv.reader(pmntadjFile)

    firstRecord=True
     
    for rootrow in pmntadj_input:
        if(firstRecord == True):            
            for pmntadjHdr in rootrow:
                pmntadjHdrLst.append(pmntadjHdr)
            pmntadj_hdr_cnt=len(pmntadjHdrLst) 
            firstRecord=False
        else:
            for pmntadjdtl in rootrow:
                pmntadjDtlLst.append(pmntadjdtl)
            for i in range(len(pmntadjHdrLst)):                
                IABS_BDT_PMNTADJ_tbl[pmntadjHdrLst[i]]=clean_focus_input(IABS_BDT_PMNTADJ_DEFN_DICT,pmntadjHdrLst[i],pmntadjDtlLst[i])

            IABS_BDT_PMNTADJ_tbl['ID']=iabs_bdt_pmntadj_sq           
            parentKey=getBaldtlKey(IABS_BDT_PMNTADJ_tbl)
            IABS_BDT_PMNTADJ_tbl['BALDTL_ID']=baldtlKeys[parentKey]
            tmpKey=str(IABS_BDT_PMNTADJ_tbl['BALDTL_ID']) + str(IABS_BDT_PMNTADJ_tbl['AINV_REF']) + str(IABS_BDT_PMNTADJ_tbl['AINV_DATE']) + str(IABS_BDT_PMNTADJ_tbl['APSTATE']) + str(IABS_BDT_PMNTADJ_tbl['SERIAL_NO']) 
            #if tmpKey in pmntadjKeys:          
            #    print "WARNING PMNTADJKEY DUPLICATE - " + tmpKey
            #    count_record('PMNTADJ',True)  
            #else:
            pmntadjKeys[tmpKey]=iabs_bdt_pmntadj_sq
            iabs_bdt_pmntadj_records[iabs_bdt_pmntadj_sq]= copy.deepcopy(IABS_BDT_PMNTADJ_tbl)
            count_record('PMNTADJ',False) 
            iabs_bdt_pmntadj_sq+=1
                
            initialize_tbl(IABS_BDT_PMNTADJ_tbl)
            pmntadjDtlLst=[]
        
    pmntadjFile.close()  


def process_ADJMTDTL_segment():
    print "Processing ADJMTDTL_segment"    
#PMNTADJ_ID|ADJMT_SER_NO
    global output_log
    global banlock
    global output_log
    global ADJMTDTL_FILENM_AND_PATH
    global adjmtdtlHdrLst, adjmtdtlDtlLst
    global IABS_BDT_ADJMTDTL_tbl, IABS_BDT_ADJMTDTL_DEFN_DICT
    global adjmtdtl_hdr_cnt
    global iabs_bdt_adjmtdtl_records
    global iabs_bdt_adjmtdtl_sq
    global pmntadjKeys    
    global adjmtdtlKeys 
    
    adjmtdtlFile = open(ADJMTDTL_FILENM_AND_PATH)    
    adjmtdtl_input = csv.reader(adjmtdtlFile)

    firstRecord=True
    
 
    for rootrow in adjmtdtl_input:
        if(firstRecord == True):            
            for adjmtdtlHdr in rootrow:
                adjmtdtlHdrLst.append(adjmtdtlHdr)
            adjmtdtl_hdr_cnt=len(adjmtdtlHdrLst) 
            firstRecord=False
        else:
            for adjmtdtldtl in rootrow:
                adjmtdtlDtlLst.append(adjmtdtldtl)
            for i in range(len(adjmtdtlHdrLst)):                
                IABS_BDT_ADJMTDTL_tbl[adjmtdtlHdrLst[i]]=clean_focus_input(IABS_BDT_ADJMTDTL_DEFN_DICT,adjmtdtlHdrLst[i],adjmtdtlDtlLst[i])

            IABS_BDT_ADJMTDTL_tbl['ID']=iabs_bdt_adjmtdtl_sq           
            parentKey=getPmntadjKey(IABS_BDT_ADJMTDTL_tbl)
            IABS_BDT_ADJMTDTL_tbl['PMNTADJ_ID']=pmntadjKeys[parentKey]
            tmpKey=str(IABS_BDT_ADJMTDTL_tbl['PMNTADJ_ID']) + str(IABS_BDT_ADJMTDTL_tbl['ADJMT_SER_NO'])
            if tmpKey in adjmtdtlKeys:          
                print "WARNING ADJMTDTLKEY DUPLICATE - " + tmpKey
                count_record('ADJMTDTL',True) 
            else:
                adjmtdtlKeys[tmpKey]=iabs_bdt_adjmtdtl_sq
                iabs_bdt_adjmtdtl_records[iabs_bdt_adjmtdtl_sq]= copy.deepcopy(IABS_BDT_ADJMTDTL_tbl)
                count_record('ADJMTDTL',False) 
                iabs_bdt_adjmtdtl_sq+=1
                
            initialize_tbl(IABS_BDT_ADJMTDTL_tbl)
            adjmtdtlDtlLst=[]
        
    adjmtdtlFile.close()  

    
def process_CRNT1_segment():
    print "Processing CRNT1_segment"
#BALDTL_ID|INVDT1|SUBSTATE
    global output_log
    global banlock
    global output_log
    global CRNT1_FILENM_AND_PATH
    global crnt1HdrLst, crnt1DtlLst
    global IABS_BDT_CRNT1_tbl, IABS_BDT_CRNT1_DEFN_DICT
    global crnt1_hdr_cnt
    global iabs_bdt_crnt1_records
    global iabs_bdt_crnt1_sq
    global baldtlKeys    
    global crnt1Keys 
    
    crnt1File = open(CRNT1_FILENM_AND_PATH)    
    crnt1_input = csv.reader(crnt1File)

    firstRecord=True
    
    for rootrow in crnt1_input:
        if(firstRecord == True):            
            for crnt1Hdr in rootrow:
                crnt1HdrLst.append(crnt1Hdr)
            crnt1_hdr_cnt=len(crnt1HdrLst) 
            firstRecord=False
        else:
            for crnt1dtl in rootrow:
                crnt1DtlLst.append(crnt1dtl)
            for i in range(len(crnt1HdrLst)):                
                IABS_BDT_CRNT1_tbl[crnt1HdrLst[i]]=clean_focus_input(IABS_BDT_CRNT1_DEFN_DICT,crnt1HdrLst[i],crnt1DtlLst[i])

            IABS_BDT_CRNT1_tbl['ID']=iabs_bdt_crnt1_sq           
            parentKey=getBaldtlKey(IABS_BDT_CRNT1_tbl)
            IABS_BDT_CRNT1_tbl['BALDTL_ID']=baldtlKeys[parentKey]
            tmpKey=str(IABS_BDT_CRNT1_tbl['BALDTL_ID']) + str(IABS_BDT_CRNT1_tbl['INVDT1']) + str(IABS_BDT_CRNT1_tbl['SUBSTATE'])

            if tmpKey in crnt1Keys:          
                print "WARNING CRNT1KEY DUPLICATE - " + tmpKey
                count_record('CRNT1',True) 
            else:
                crnt1Keys[tmpKey]=iabs_bdt_crnt1_sq
                iabs_bdt_crnt1_records[iabs_bdt_crnt1_sq]= copy.deepcopy(IABS_BDT_CRNT1_tbl)
                count_record('CRNT1',False)
                iabs_bdt_crnt1_sq+=1
                
            initialize_tbl(IABS_BDT_CRNT1_tbl)
            crnt1DtlLst=[]
        
    crnt1File.close()  
    
def process_CRNT2_segment():
    print "Processing CRNT2_segment"
#CRNT1_ID|ACC_TYP_IND|FAC_TYP_IND
    global output_log
    global banlock
    global output_log
    global CRNT2_FILENM_AND_PATH
    global crnt2HdrLst, crnt2DtlLst
    global IABS_BDT_CRNT2_tbl, IABS_BDT_CRNT2_DEFN_DICT
    global crnt2_hdr_cnt
    global iabs_bdt_crnt2_records
    global iabs_bdt_crnt2_sq
    global crnt1Keys    
    global crnt2Keys 
    
    crnt2File = open(CRNT2_FILENM_AND_PATH)    
    crnt2_input = csv.reader(crnt2File)

    firstRecord=True
 
    for rootrow in crnt2_input:
        if(firstRecord == True):            
            for crnt2Hdr in rootrow:
                crnt2HdrLst.append(crnt2Hdr)
            crnt2_hdr_cnt=len(crnt2HdrLst) 
            firstRecord=False
        else:
            for crnt2dtl in rootrow:
                crnt2DtlLst.append(crnt2dtl)
            for i in range(len(crnt2HdrLst)):                
                IABS_BDT_CRNT2_tbl[crnt2HdrLst[i]]=clean_focus_input(IABS_BDT_CRNT2_DEFN_DICT,crnt2HdrLst[i],crnt2DtlLst[i])

            IABS_BDT_CRNT2_tbl['ID']=iabs_bdt_crnt2_sq           
            parentKey=getCrnt1Key(IABS_BDT_CRNT2_tbl)
            if parentKey in crnt1Keys:
                IABS_BDT_CRNT2_tbl['CRNT1_ID']=crnt1Keys[parentKey]
                print('-----CRNT1_ID FOUND-----')
                tmpKey=str(IABS_BDT_CRNT2_tbl['CRNT1_ID']) + str(IABS_BDT_CRNT2_tbl['ACC_TYP_IND']) + str(IABS_BDT_CRNT2_tbl['FAC_TYP_IND'])

                if tmpKey in crnt2Keys:          
                    print "WARNING CRNT2KEY DUPLICATE - " + tmpKey
                    count_record('CRNT2',True)
                else:
                    crnt2Keys[tmpKey]=iabs_bdt_crnt2_sq
                    iabs_bdt_crnt2_records[iabs_bdt_crnt2_sq]= copy.deepcopy(IABS_BDT_CRNT2_tbl)
                    count_record('CRNT2',True)
                    iabs_bdt_crnt2_sq+=1
            else:
                print ('WARNING ----- PARENT KEY NOT FOUND - ' + parentKey)
                print ('EOB_DATE - '  + IABS_BDT_CRNT2_tbl['EOB_DATE'] + '|| ACNA - '  + IABS_BDT_CRNT2_tbl['ACNA'] + '|| BAN - '  + IABS_BDT_CRNT2_tbl['BAN'] + '|| INV_DATE - '  + IABS_BDT_CRNT2_tbl['INV_DATE'] + '|| ASTATE - '  + IABS_BDT_CRNT2_tbl['ASTATE'] + '|| PREVBAL - '  + IABS_BDT_CRNT2_tbl['PREVBAL'] + '|| DINV_DATE - '  + IABS_BDT_CRNT2_tbl['DINV_DATE'] + '|| DSTATE - '  + IABS_BDT_CRNT2_tbl['DSTATE'] + '|| INVDT1 - '  + IABS_BDT_CRNT2_tbl['INVDT1'] + '|| SUBSTATE - '  + IABS_BDT_CRNT2_tbl['SUBSTATE'])
                count_record('CRNT2_NOT_FOUND',True)
            
                
            initialize_tbl(IABS_BDT_CRNT2_tbl)
            crnt2DtlLst=[]
        
    crnt2File.close()  


    
def reset_record_flags():
    "These flags are used to check if a record already exists for the current acna/ban/eob_date"
    "The first time a record is processed, if the count == 0, then an insert will be performed,"
    "else an update."
    print "Resetting Flags"

def fix_date(datestring):
    return datestring[:4]+datestring[5:7]+datestring[8:10]
    
def fix_null(fieldstr, fieldtype):
    tmpstr=fieldstr
    if (fieldtype=='STRING'):
        if (str(fieldstr).rstrip(' ') == ''):
            tmpstr='nl'
    elif (fieldtype=='NUMBER'):
        if (str(fieldstr).rstrip(' ') == ''):
            tmpstr='0'
        
    return tmpstr
    
def initialize_sequences():
    global iabs_bdt_iabsbil_sq
    global iabs_bdt_baldue_sq
    global iabs_bdt_baldtl_sq
    global iabs_bdt_dispdtl_sq
    global iabs_bdt_pmntadj_sq
    global iabs_bdt_adjmtdtl_sq
    global iabs_bdt_crnt1_sq
    global iabs_bdt_crnt2_sq

    iabs_bdt_iabsbil_sq = get_new_sequence("SQ_IABS_BDT_IABSBIL") - 10000000
    iabs_bdt_baldue_sq = get_new_sequence("SQ_IABS_BDT_BALDUE") - 10000000
    iabs_bdt_baldtl_sq = get_new_sequence("SQ_IABS_BDT_BALDTL") - 10000000
    iabs_bdt_dispdtl_sq = get_new_sequence("SQ_IABS_BDT_DISPDTL") - 10000000
    iabs_bdt_pmntadj_sq = get_new_sequence("SQ_IABS_BDT_PMNTADJ") - 10000000
    iabs_bdt_adjmtdtl_sq = get_new_sequence("SQ_IABS_BDT_ADJMTDTL") - 10000000
    iabs_bdt_crnt1_sq = get_new_sequence("SQ_IABS_BDT_CRNT1") - 10000000
    iabs_bdt_crnt2_sq = get_new_sequence("SQ_IABS_BDT_CRNT2") - 10000000

    writelog("Sequences Set", output_log)
     
####END INITIALIZE PROCEDURES
     
def get_new_sequence(sequence_name):    
    selCur = con.cursor()
    sqlStmt="SELECT " + sequence_name +".nextval FROM dual"
    selCur.execute(sqlStmt)
    seqId = selCur.fetchone()[0]        
    selCur.close()
    return seqId
    
def count_record(currec,dupRec):
    global record_counts
    global dup_record_counts

    if dupRec:
        if str(currec).rstrip(' ') in dup_record_counts:            
            dup_record_counts[str(currec).rstrip(' ')]+=1
        else:
            dup_record_counts[str(currec).rstrip(' ')]=1
    else:
        if str(currec).rstrip(' ') in record_counts:
            record_counts[str(currec).rstrip(' ')]+=1
        else:
            record_counts[str(currec).rstrip(' ')]=1 

   
def process_write_program_stats():
    global record_counts
    global dup_record_counts
    global output_log
    writelog("\n",output_log)
    
    idCnt=0
    writelog("**",output_log)
    writelog("**Processed record IDs and counts**",output_log)
    writelog("record_id, count",output_log)
    
    keylist = record_counts.keys()
    keylist.sort()
    for key in keylist:
#        print "%s: %s" % (key, record_counts[key])   
        writelog(str(key)+", "+str(record_counts[key]),output_log)   
        idCnt+=record_counts[key]       
#    for key, value in record_counts.items():
#        writelog(str(key)+", "+str(value))   
#        idCnt+=value
    writelog("\n  Total count: "+str(idCnt),output_log)
    writelog(" ",output_log)
    writelog("**",output_log)
    unkCnt=0
    writelog( "There are "+str(len(dup_record_counts))+" duplicate record types not processed: ",output_log)    
    writelog("**DUPLICATE record IDs and counts**",output_log)
    writelog("record_id, count",output_log)

    keylist = dup_record_counts.keys()
    keylist.sort()
    for key in keylist:
#        print "%s: %s" % (key, record_counts[key])   
        writelog(str(key)+", "+str(dup_record_counts[key]),output_log)   
        unkCnt+=dup_record_counts[key]  

    writelog("\n  Total count: "+str(unkCnt),output_log)    
    writelog("**",output_log)    
    
   # writelog("TOTAL ACNA/EOB_DATE/BAN's processed:"+str(CSR_KEY_cnt),output_log)
    writelog(" ",output_log)
    #writelog("Total input records read from input file:"+str(idCnt+unkCnt),output_log)
    writelog(" ",output_log)


def process_ERROR_END(msg):
    global output_log
    writelog("ERROR:"+msg,output_log)
#    debug("ERROR:"+msg)
    con.commit()
    con.close()
    process_close_files()
#    raise Exception("ERROR:"+msg)
    
    
def process_close_files():
    global output_log    
   
    output_log.close()
   
def initialize_tbl(tbl):
    for key,value in tbl.items() :            
        tbl[key]=''
    
def endProg(msg):
    global output_log
    
    ###print "updating sequences"
    ############################update_sequences()
    con.commit()
    con.close()
    
    process_write_program_stats()
     
    endTM=datetime.datetime.now()
    print "\nTotal run time:  %s" % (endTM-startTM)
    writelog("\nTotal run time:  %s" % (endTM-startTM),output_log)
     

    writelog("\n"+msg,output_log)
     
    process_close_files()

def process_inserts():

    global iabs_bdt_iabsbil_records, iabs_bdt_baldue_records, iabs_bdt_baldtl_records, iabs_bdt_dispdtl_records
    global iabs_bdt_pmntadj_records, iabs_bdt_adjmtdtl_records, iabs_bdt_crnt1_records, iabs_bdt_crnt2_records
    global IABS_BDT_IABSBIL_DEFN_DICT, IABS_BDT_BALDUE_DEFN_DICT
    global IABS_BDT_IABSBIL_tbl, IABS_BDT_IABSBIL_DEFN_DICT
    global IABS_BDT_BALDUE_tbl, IABS_BDT_BALDUE_DEFN_DICT
    global IABS_BDT_BALDTL_tbl, IABS_BDT_BALDTL_DEFN_DICT
    global IABS_BDT_DISPDTL_tbl, IABS_BDT_DISPDTL_DEFN_DICT
    global IABS_BDT_PMNTADJ_tbl, IABS_BDT_PMNTADJ_DEFN_DICT
    global IABS_BDT_ADJMTDTL_tbl, IABS_BDT_ADJMTDTL_DEFN_DICT
    global IABS_BDT_CRNT1_tbl, IABS_BDT_CRNT1_DEFN_DICT
    global IABS_BDT_CRNT2_tbl, IABS_BDT_CRNT2_DEFN_DICT
    global con
    global output_log
    global schema
    global iabsbil_insert_cnt, baldue_insert_cnt, baldtl_insert_cnt, dispdtl_insert_cnt
    global pmntadj_insert_cnt, adjmtdtl_insert_cnt, crnt1_insert_cnt, crnt2_insert_cnt
    global cntr, rows, insql, results, inscnt
 
    results={}
    selCur = con.cursor()

    cntr=0
    rows=[]
    insql=""
    inscnt=0
    
    if len(iabs_bdt_iabsbil_records) > 0:
        for key, value in iabs_bdt_iabsbil_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table('IABS_BDT_IABSBIL', value, IABS_BDT_IABSBIL_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
        
    if len(iabs_bdt_baldue_records) > 0:
        for key, value in iabs_bdt_baldue_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table('IABS_BDT_BALDUE', value, IABS_BDT_BALDUE_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
    
    if len(iabs_bdt_baldtl_records) > 0:
        for key, value in iabs_bdt_baldtl_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table('IABS_BDT_BALDTL', value, IABS_BDT_BALDTL_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
        
    if len(iabs_bdt_dispdtl_records) > 0:
        for key, value in iabs_bdt_dispdtl_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table('IABS_BDT_DISPDTL', value, IABS_BDT_DISPDTL_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()        
    
    if len(iabs_bdt_pmntadj_records) > 0:
        for key, value in iabs_bdt_pmntadj_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table('IABS_BDT_PMNTADJ', value, IABS_BDT_PMNTADJ_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()   
    
    if len(iabs_bdt_adjmtdtl_records) > 0:
        for key, value in iabs_bdt_adjmtdtl_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table('IABS_BDT_ADJMTDTL', value, IABS_BDT_ADJMTDTL_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals() 
    
    if len(iabs_bdt_crnt1_records) > 0:
        for key, value in iabs_bdt_crnt1_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table('IABS_BDT_CRNT1', value, IABS_BDT_CRNT1_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals() 
    
    if len(iabs_bdt_crnt2_records) > 0:
        for key, value in iabs_bdt_crnt2_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table('IABS_BDT_CRNT2', value, IABS_BDT_CRNT2_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals() 
        
        
        
    selCur.close()
    
#Update insert counters    
    iabsbil_insert_cnt+=len(iabs_bdt_iabsbil_records)
    baldue_insert_cnt+=len(iabs_bdt_baldue_records)
    baldtl_insert_cnt+=len(iabs_bdt_baldtl_records)
    dispdtl_insert_cnt+=len(iabs_bdt_dispdtl_records)
    pmntadj_insert_cnt+=len(iabs_bdt_pmntadj_records)
    adjmtdtl_insert_cnt+=len(iabs_bdt_adjmtdtl_records)
    crnt1_insert_cnt+=len(iabs_bdt_crnt1_records)
    crnt2_insert_cnt+=len(iabs_bdt_crnt2_records)

#Reset records    
    iabs_bdt_iabsbil_records={}
    iabs_bdt_baldue_records={}    
    iabs_bdt_iabsbil_records={}
    iabs_bdt_baldue_records={}
    iabs_bdt_baldtl_records={}
    iabs_bdt_dispdtl_records={}
    iabs_bdt_pmntadj_records={}
    iabs_bdt_adjmtdtl_records={}
    iabs_bdt_crnt1_records={}
    iabs_bdt_crnt2_records={}
    
    
def prepareInsert(val):
    global insql,results,cntr,rows
    if cntr==0:
        insql=str(results['firstPart']) + " values " + str(results['bindVars'])
        cntr+=1
    tmpSecond=tuple(results['secondPart'])            
    rows.append(tmpSecond)
    #rows.append(val)
def resetVals():
    global cntr, rows, insql
    cntr=0
    rows=[]
    insql=""
    


def getRootrecKey(tblRec):
    
    global IABS_BDT_IABSBIL_DEFN_DICT
    #ACNA|BAN|EOB_DATE
    acna = clean_focus_input(IABS_BDT_IABSBIL_DEFN_DICT,'ACNA',tblRec['ACNA'])
    ban = clean_focus_input(IABS_BDT_IABSBIL_DEFN_DICT,'BAN',tblRec['BAN'])
    eob_date = clean_focus_input(IABS_BDT_IABSBIL_DEFN_DICT,'EOB_DATE',tblRec['EOB_DATE'])
    
    return str(acna) + str(ban) + str(eob_date)    
    
def getBaldueKey(tblRec):
    
    global IABS_BDT_BALDUE_DEFN_DICT
    #IABSBIL_ID|INV_DATE|ASTATE|PREVBAL
    inv_date = clean_focus_input(IABS_BDT_BALDUE_DEFN_DICT,'INV_DATE',tblRec['INV_DATE'])
    astate = clean_focus_input(IABS_BDT_BALDUE_DEFN_DICT,'ASTATE',tblRec['ASTATE'])
    prevbal = clean_focus_input(IABS_BDT_BALDUE_DEFN_DICT,'PREVBAL',tblRec['PREVBAL'])
    rootreckey = getRootrecKey(tblRec)
    iabsbil_id = iabsbilKeys[rootreckey]
    
    return str(iabsbil_id) + str(inv_date) + str(astate) + str(prevbal)   

def getBaldtlKey(tblRec):
    
    global IABS_BDT_BALDTL_DEFN_DICT
    #BALDUE_ID|DINV_DATE|DSTATE
    dinv_date = clean_focus_input(IABS_BDT_BALDTL_DEFN_DICT, 'DINV_DATE', tblRec['DINV_DATE'])
    dstate = clean_focus_input(IABS_BDT_BALDTL_DEFN_DICT, 'DSTATE', tblRec['DSTATE'])
    balduekey = getBaldueKey(tblRec)
    baldue_id = baldueKeys[balduekey]
    
    return str(baldue_id) + str(dinv_date) + str(dstate)
    
def getPmntadjKey(tblRec):

    global IABS_BDT_PMNTADJ_DEFN_DICT
    #BALDTL_ID|AINV_REF|AINV_DATE|APSTATE 
    ainv_ref = clean_focus_input(IABS_BDT_PMNTADJ_DEFN_DICT, 'AINV_REF', tblRec['AINV_REF'])
    ainv_date = clean_focus_input(IABS_BDT_PMNTADJ_DEFN_DICT, 'AINV_DATE', tblRec['AINV_DATE'])
    apstate = clean_focus_input(IABS_BDT_PMNTADJ_DEFN_DICT, 'APSTATE', tblRec['APSTATE'])
    serial_no = clean_focus_input(IABS_BDT_PMNTADJ_DEFN_DICT, 'SERIAL_NO', tblRec['ADJMT_SER_NO'])
    baldtlkey = getBaldtlKey(tblRec)
    baldtl_id = baldtlKeys[baldtlkey]
    
    return str(baldtl_id) + str(ainv_ref) + str(ainv_date) + str(apstate) + str(serial_no)
    
def getCrnt1Key(tblRec):
    
    global IABS_BDT_CRNT1_DEFN_DICT
    #BALDTL_ID|INVDT1|SUBSTATE 
    invdt1 = clean_focus_input(IABS_BDT_CRNT1_DEFN_DICT, 'INVDT1', tblRec['INVDT1'])
    substate = clean_focus_input(IABS_BDT_CRNT1_DEFN_DICT, 'SUBSTATE', tblRec['SUBSTATE'])
    baldtlkey = getBaldtlKey(tblRec)
    baldtl_id = baldtlKeys[baldtlkey]
    
    return str(baldtl_id) + str(invdt1) + str(substate)

"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
"BEGIN Program logic"
"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


print "Starting Program"
init()
main()
endProg("-END OF PROGRAM-")
