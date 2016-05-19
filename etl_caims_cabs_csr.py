# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
PROGRAM NAME:     etl_caims_cabs_csr.py                                  
LOCATION:                      
PROGRAMMER(S):    Dan VandeRiet, Jason Yelverton                                
DESCRIPTION:      CAIMS Extract/Transformation/Load program for CSR records.
                  
REPLACES:         Legacy CTL program xxxxxxxxx - LOAD xxxxxxxxxx FOCUS DATABASE.
                                                                         
LANGUAGE/VERSION: Python/2.7.10                                          
INITIATION DATE:  x/x/2016                                                
INPUT FILE(S):    xxxxxxxxxxxxxxxxxxxxxxxxxxx (build from GDG)
LOCATION:         MARION MAINFRAME           
                                                                         
OUTPUT:           ORACLE DWBS001P                                
                  Table names CAIMS_CSR_*
                                                                         
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


###IMPORT COMMON/SHARED UTILITIES
from etl_caims_cabs_utility import  process_insert_many_table, writelog, db_record_exists, \
                                   createTableTypeDict,setDictFields,convertnumber, invalid_acna_chars  


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
fileNm=""
try:
    fileNm=sys.argv[1] 
except:
   raise Exception("ERROR: No file name passed to " + str(sys.argv[0]))
    
if fileNm.rstrip(' ') == "":
    raise Exception("ERROR: File name passed to " + str(sys.argv[0]) +" is empty.")
else:
    print "fileNm value is:" + str(fileNm)



if str(platform.system()) == 'Windows':
    inputPath=settings.get('CSRSettings','WINDOWS_CSR_inDir') 
else:
    inputPath=settings.get('CSRSettings','LINUX_CSR_inDir') 

IP_FILENM_AND_PATH =os.path.join(inputPath,fileNm)

if os.path.isfile(IP_FILENM_AND_PATH):
    print ("Input file:"+IP_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+IP_FILENM_AND_PATH)


usoccnt = 0
usoc_cnt = 0
ucode = ''
Level = ''

root_rec=False
csr_tape_val=False
CSR_CKT_exists=False 
 
"GLOBAL VARIABLES"
record_id='987654'
prev_record_id='456789'  
badKey=False
grplock=False
dup_ban_lock=False
current_abbd_rec_key={'ACNA':'x','EOB_DATE':'1','BAN':'x'}
current_abbd_rec_key_concat=''                     #EOB_DATE|ACNA|BAN       :    ID (BCCBSPLID)
current_ckt_key_concat=''                          #BCCBSPL_ID|PSM|CIRCUIT  :    ID (CKT_ID)
current_loc_key_concat=''                          #CKT_ID|LOC              :    ID (LOC_ID)
current_cktusoc_key_concat=''                      #CKT_ID|CUSOC            :    ID (CKTUSOC_ID)
#current_cktusoc_key_concat=''                      #CKT_ID|CUSOC|CUSOCQTY   :    ID (CKTUSOC_ID)
current_usoc_key_concat=''                         #LOC_ID|USOC             :    ID (USOC_ID)
prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}   #EOB_DATE,ACNA,BAN key   
 
#Variables to hold sequence values for all table inserts
csr_actlrec_sq=0
csr_bccbspl_sq=0
csr_billrec_sq=0
csr_cfid_sq=0
csr_ckt_sq=0
csr_cktusoc_sq=0
csr_cosfid_sq=0
csr_cufid_sq=0
csr_lfid_sq=0
csr_loc_sq=0
csr_ufid_sq=0
csr_usoc_sq=0

 
record_counts={}
unknown_record_counts={}
CSR_REC='40' 
STAR_LINE='*********************************************************************************************'  
   
# Initialize Unique keys - KV - where Key is concatenated unique fields and value is sequence id #
# Child tables will have parent id
bccbsplKeys={}
billrecKeys={}
actlrecKeys={}
cktKeys={}
locKeys={}
cktusocKeys={}
usocKeys={}
##############################################
csr_bccbspl_records={}
csr_actlrec_records={}
csr_billrec_records={}
csr_ckt_records={}
csr_loc_records={}
csr_cfid_records={}
csr_cosfid_records={}
csr_cufid_records={}
csr_ufid_records={}
csr_lfid_records={}
csr_usoc_records={}
csr_cktusoc_records={}

bccbspl_insert_cnt=0
actlrec_insert_cnt=0
billrec_insert_cnt=0
ckt_insert_cnt=0
loc_insert_cnt=0
cfid_insert_cnt=0
cosfid_insert_cnt=0
cufid_insert_cnt=0
ufid_insert_cnt=0
lfid_insert_cnt=0
usoc_insert_cnt=0
cktusoc_insert_cnt=0

results={}
cntr=0
inscnt=0
rows=[]
insql=""

"TRANSLATERS"


# ufidCols=ID,USOC_ID,UFID,UFID_DATA

#actlrecCols=ID, BCCBSPL_ID, ACTL_NUM, CUST_NAME, ACTL, ACTLADDR1, ACTLADDR2, ACTLADDR3, INPUT_RECORDS

# bccbsplCols=ID, ACNA, BAN, EOB_DATE, BILL_DATE, NLATA, ICSC_OFC, FGRP, TAPE, BAN_TAR, TAX_EXMPT, HOLD_BILL, PCNTR, CCNA, MCN, FINAL_IND, MKT_IND, FRMT_IND, EOBDATECC, BILLDATECC, UNB_TAR_IND, MAN_BAN_TYPE, INPUT_RECORDS    
       
    
def init():
    global output_log
    global record_id
    global prev_record_id
    global csr_input 
#    global output_log
    "OPEN FILES"
    "   CABS INPUT FILE"
    csr_input = open(IP_FILENM_AND_PATH, "r")
    
    "PROCESS HEADER LINE"
    "  -want to get bill cycle info for output log file "
    "  -this will make the log file names more sensical " 
#    READ HEADER LINE    
    headerLine=csr_input.readline()
    record_id=headerLine[:6]
    cycl_yy=headerLine[6:8]
    cycl_mmdd=headerLine[8:12]
    cycl_time=headerLine[12:21].replace('\n','')

    "  CREATE LOG FILE WITH CYCLE DATE FROM HEADER AND RUN TIME OF THIS JOB"
    
    log_file=os.path.join(OUTPUT_DIR,"CSR_Cycle"+str(cycl_yy)+str(cycl_mmdd)+str(cycl_time)+"_"+startTM.strftime("%Y%m%d_@%H%M") +".txt")
         
    output_log = open(log_file, "w")
    output_log.write("-CSR CAIMS ETL PROCESS-")
    
    if record_id not in settings.get('CSRSettings','CSRHDR'):
        process_ERROR_END("The first record in the input file was not a "+str(settings.get('CSRSettings','CSRHDR')).rstrip(' ')+" record")

    writelog("Process "+sys.argv[0],output_log)
    writelog("   started execution at: " + str(startTM),output_log)
    writelog(STAR_LINE,output_log)
    writelog(" ",output_log)
    writelog("Input file: "+str(csr_input),output_log)
    writelog("Log file: "+str(output_log),output_log)
    
    "Write header record informatio only"
    writelog("Header Record Info:",output_log)
    writelog("     Record ID: "+record_id+" YY: "+cycl_yy+", MMDD: "+cycl_mmdd+", TIME: "+str(cycl_time),output_log)
    
    count_record(record_id,False)
    del headerLine,cycl_yy,cycl_mmdd,cycl_time
    
    "Get initial values for all sequences"
    initialize_sequences()
    
def main():
    
    global record_type
    global line
    global Level
    global record_counts
    global unknown_record_counts
    global output_log
    "Counters"

    global inputLineCnt, CSR_KEY_cnt

    "TABLE Dictionaries - for each segment"
    global TMP_TEST_EXMANY_tbl,  TMP_TEST_EXMANY_DEFN_DICT
    global CSR_BCCBSPL_tbl,  CSR_BCCBSPL_DEFN_DICT
    global CSR_BILLREC_tbl, CSR_BILLREC_DEFN_DICT
    global CSR_ACTLREC_tbl, CSR_ACTLREC_DEFN_DICT
    global CSR_CKT_tbl, CSR_CKT_DEFN_DICT
    global CSR_LOC_tbl, CSR_LOC_DEFN_DICT
    global CSR_LFID_tbl, CSR_LFID_DEFN_DICT
    global CSR_COSFID_tbl, CSR_COSFID_DEFN_DICT
    global CSR_UFID_tbl, CSR_UFID_DEFN_DICT
    global CSR_USOC_tbl, CSR_USOC_DEFN_DICT
    global CSR_CUFID_tbl, CSR_CUFID_DEFN_DICT
    global CSR_CFID_tbl, CSR_CFID_DEFN_DICT
    global CSR_CKTUSOC_tbl, CSR_CKTUSOC_DEFN_DICT
# 
    "text files"
    global csr_input
    global output_log
    
    global csr_bccbspl_records  
    global CSR_column_names
    global record_id
    global prev_record_id
    global current_abbd_rec_key 
    global prev_abbd_rec_key   
    global goodKey      
    global current_abbd       #{ACNA|BAN|EOB_DATE:ID}
    global current_psm_ckt   #{BCCBSPL_ID|PSM|CKT:ID} -- PID current_abbd[]
    global current_loc       #{CKT_ID|LOC:ID}
    global grplock
    global dup_ban_lock
    
#DECLARE TABLE ARRAYS AND DICTIONARIES
    
#    CSR_BCCBSPL_tbl=collections.OrderedDict() 
    prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}
    
    CSR_BCCBSPL_DEFN_DICT=createTableTypeDict('CAIMS_CSR_BCCBSPL',con,schema,output_log) 
    CSR_BCCBSPL_tbl=setDictFields('CAIMS_CSR_BCCBSPL', CSR_BCCBSPL_DEFN_DICT) 

    CSR_BILLREC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_BILLREC',con,schema,output_log)
    CSR_BILLREC_tbl=setDictFields('CAIMS_CSR_BILLREC', CSR_BILLREC_DEFN_DICT)

    CSR_ACTLREC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_ACTLREC',con,schema,output_log)
    CSR_ACTLREC_tbl=setDictFields('CAIMS_CSR_ACTLREC', CSR_ACTLREC_DEFN_DICT)

    CSR_CKT_DEFN_DICT=createTableTypeDict('CAIMS_CSR_CKT',con,schema,output_log)    
    CSR_CKT_tbl=setDictFields('CAIMS_CSR_CKT', CSR_CKT_DEFN_DICT)

    CSR_LOC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_LOC',con,schema,output_log)    
    CSR_LOC_tbl=setDictFields('CAIMS_CSR_LOC', CSR_LOC_DEFN_DICT)

    CSR_LFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_LFID',con,schema,output_log) 
    CSR_LFID_tbl=setDictFields('CAIMS_CSR_LFID', CSR_LFID_DEFN_DICT)

    CSR_COSFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_COSFID',con,schema,output_log) 
    CSR_COSFID_tbl=setDictFields('CAIMS_CSR_COSFID', CSR_COSFID_DEFN_DICT)

    CSR_UFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_UFID',con,schema,output_log) 
    CSR_UFID_tbl=setDictFields('CAIMS_CSR_UFID', CSR_UFID_DEFN_DICT)

    CSR_USOC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_USOC',con,schema,output_log) 
    CSR_USOC_tbl=setDictFields('CAIMS_CSR_USOC', CSR_USOC_DEFN_DICT)
        
    CSR_CUFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_CUFID',con,schema,output_log) 
    CSR_CUFID_tbl=setDictFields('CAIMS_CSR_CUFID', CSR_CUFID_DEFN_DICT)

    CSR_CFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_CFID',con,schema,output_log) 
    CSR_CFID_tbl=setDictFields('CAIMS_CSR_CFID', CSR_CFID_DEFN_DICT)
    
    CSR_CKTUSOC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_CKTUSOC',con,schema,output_log) 
    CSR_CKTUSOC_tbl=setDictFields('CAIMS_CSR_CKTUSOC', CSR_CFID_DEFN_DICT)   
   
    "COUNTERS"
    inputLineCnt=0
    CSR_KEY_cnt=0
    status_cnt=0
    exmanyCnt=0
    "KEY"
    
    Level=' '
    "LOOP THROUGH INPUT CABS TEXT FILE"
    for line in csr_input:
      
        "Count each line"
        inputLineCnt += 1
        status_cnt+=1
        if status_cnt>999:
            print str(inputLineCnt)+" lines completed processing...."+str(datetime.datetime.now())
            status_cnt=0
        record_type=line[:2]
               
         
        if len(line) > 231:
            record_id=line[225:231]
        else:
            record_id=line[:6]


        "Process by record_id"
     
        "Header rec (first rec) processed in init()"
          
        if record_type == CSR_REC:
            "START-MAIN PROCESS LOOP"
            "GET KEY OF NEXT RECORD"
            current_abbd_rec_key=process_getkey()
            if badKey:
                count_record("BAD_ABD_KEY",True)
                writelog("WARNING: BAD INPUT DATA.  ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'],output_log)
            elif dup_ban_lock and record_id != '010100':
                count_record("DUP_BAN",True)
            elif grplock and record_id != '010100':
                count_record("BAD_FGRP",True)
            else:
                if current_abbd_rec_key != prev_abbd_rec_key:
                    CSR_KEY_cnt+=1
                    exmanyCnt+=1
                    if exmanyCnt >= 5:
                        process_inserts()
                        exmanyCnt=0
                    reset_record_flags()
                    
                process_csr_records()
   
            "set previous key for comparison in next iteration"   
            prev_abbd_rec_key=current_abbd_rec_key
            
        elif record_id in settings.get('CSRSettings','CSRFTR'):
            "FOOTER RECORD"
            log_footer_rec_info()     
            
        else:
            "ERROR:Unidentified Record"
            writelog("ERROR: Not sure what type of record this is:",output_log)
            writelog(line,output_log)
         
         
        prev_record_id=record_id
        "END-MAIN PROCESS LOOP"    
    #END of For Loop
       
    process_inserts()
#    update_sequences()                    
    reset_record_flags()
###############################################main end#########################################################
 


def log_footer_rec_info(): 
    global record_id
    global Level
    global output_log
    
    BAN_cnt=line[6:12]
    REC_cnt=line[13:22]
    writelog(" ",output_log)
    writelog("Footer Record Info:",output_log)
    writelog("     Record ID "+record_id+" BAN Count: "+BAN_cnt+" RECORD CNT: "+REC_cnt,output_log)
    writelog("The total number of lines counted in input file is : "+ str(inputLineCnt),output_log)
    writelog(" ",output_log)
    
    count_record(record_id,False)

def process_csr_records():
    global record_id
    global Level
    global tstx
    global verno
    global tsty
    global uact

 
     
    dtst=line[224:225]  
    unknownRecord=False
    if record_id == '010100':                                  
        process_TYP0101_HEADREC()                              
    elif record_id == '050500':                                
        process_ROOTREC_TYP0505()
    elif record_id in ('051000','051100'):                     
        process_ROOTREC_CHEKFID()
    elif record_id == '100500':                                
        process_BILLREC_BILLTO()                               
    elif record_id == '101000':                                
        process_ACTLREC_BILLTO()                               
    elif record_id == '101500':
        process_ROOTREC_UPROOT()
    elif record_id == '150500' and dtst !='D':
        process_TYP1505_CKT_LOC()       
    elif record_id in ('150600','151001') and dtst !='D':
        process_FIDDRVR()     
    elif record_id == '151000' and dtst !='D':
        process_USOCDRVR_36()
    elif record_id == '151600' and dtst !='D':
        process_USOCDRVR_TAX()    
    elif record_id == '151700' and dtst !='D':
        process_USOCDRVR_PLN()  
    elif record_id in ('152000','152001'):
        process_TYP1520() 
    elif record_id =='152100':    
        process_TYP1521()
    else:  #UNKNOWN RECORDS
#        debug("unknown record id:"+record_id)
        unknownRecord=True
    
    count_record(record_id,unknownRecord)

        
def process_getkey():
    global badKey
    global blankACNA
    global badCharsInACNA
 
    tmpACNA=line[6:11].rstrip(' ')
    tmpBAN=line[17:30].rstrip(' ')
    tmpEOB_DATE=line[11:17].rstrip(' ')
    newACNA='nl'
    badKey=False
    blankACNA=False
    badCharsInACNA=False
       
    if tmpACNA=='':
        newACNA='nl'
        blankACNA=True
    elif invalid_acna_chars(tmpACNA):
        newACNA='nl'
        badCharsInACNA=True
    else:
        newACNA=tmpACNA
    
    if tmpBAN == '' or tmpEOB_DATE == '':
        badKey=True
        
    return { 'ACNA': newACNA, 'EOB_DATE':tmpEOB_DATE, 'BAN':tmpBAN}
     
    
def reset_record_flags():
    "These flags are used to check if a record already exists for the current acna/ban/eob_date"
    "The first time a record is processed, if the count == 0, then an insert will be performed,"
    "else an update."
    global csr_tape_val   
    csr_tape_val=''    
    
    global root_rec
    global adjmtdtl_rec
    global Level
    global CSR_CKT_exists
    CSR_CKT_exists=False 
    Level=''    
    root_rec=False
   
    
def process_TYP0101_HEADREC():
    global verno
    global Level
    global output_log
    global current_abbd_rec_key
    global bccbsplKeys
    global dup_ban_lock
     
#   
    tmp_current_abbd_rec_key_concat = current_abbd_rec_key['ACNA'].rstrip(' ')  + current_abbd_rec_key['BAN'].rstrip(' ')  + current_abbd_rec_key['EOB_DATE'].rstrip(' ')
    if tmp_current_abbd_rec_key_concat in bccbsplKeys or db_record_exists('CAIMS_CSR_BCCBSPL', current_abbd_rec_key['ACNA'],current_abbd_rec_key['BAN'],current_abbd_rec_key['EOB_DATE'], con, schema, output_log):
        dup_ban_lock=True
        writelog("WARNING: DUPLICATE BAN - ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'], output_log)
#   
    else:
        dup_ban_lock=False
    initialize_tbl('CSR_BCCBSPL_tbl')
#    initialize_BCCBSPL_tbl()
#    FIXFORM X-225 X5 X6 X13 X31 X15 X6 VERSION_NBR/2 X1 X4 X3 X3 X2 X3 X4
#    FIXFORM HOLD_BILL/1
#    FIXFORM FINAL_IND/1 MKT_IND/2 FRMT_IND/2 X108 MAN_BAN_TYPE/1

    verno=line[82:84] 
    CSR_BCCBSPL_tbl['HOLD_BILL']=line[104:105 ] 
    CSR_BCCBSPL_tbl['FINAL_IND']=line[105:106] 
    CSR_BCCBSPL_tbl['MKT_IND']=line[106:108] 
    CSR_BCCBSPL_tbl['FRMT_IND']=line[108:110] 
    CSR_BCCBSPL_tbl['MAN_BAN_TYPE']=line[219:220] 
    
    writelog("** BOS VERSION NUMBER IS "+verno+" ** ",output_log)
    writelog("CASE HEADREC HOLD_BILL = "+line[104:105],output_log)
    writelog("**--------------------------**",output_log)

    #Reset Variables at this level?????
    Level=' '    
    
    
    
def process_ROOTREC_TYP0505(): 
    "050500"
#    debug("Root Record:"+record_id)
    global root_rec
    global Level
    global record_id
    global output_log
    global banlock
    global grplock
    global current_abbd_rec_key
    global current_abbd_rec_key_concat
    global csr_bccbspl_records
    global csr_bccbspl_sq
    global bccbsplKeys

    Level = 'F'
    banlock='N'
   
    "record_id doesn't need populated"
  
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 TNPA/A3 X-3 BAN/A13 X-5 FGRP/1
#FIXFORM X4 X8 X1 X120
#FIXFORM BILLDATECC/8 X-6 BILL_DATE/6 X10
#FIXFORM ICSC_OFC/4 X1 NLATA/3 X5 BAN_TAR/4 X2 TAX_EXMPT/9
#FIXFORM X2 UNB_TAR_IND/1

    CSR_BCCBSPL_tbl['FGRP']=line[25:26] 
    if CSR_BCCBSPL_tbl['FGRP'] in ('S','W'):
        grplock=False
        CSR_BCCBSPL_tbl['BILLDATECC']=line[159:167]  
        CSR_BCCBSPL_tbl['BILL_DATE']=line[161:167]    
        #note ICSC_OFC ix only 3 byte in MFD, 4 in FIXFORM ABOVE
        CSR_BCCBSPL_tbl['ICSC_OFC']=line[177:180]
        
        CSR_BCCBSPL_tbl['NLATA']=line[182:185]
        CSR_BCCBSPL_tbl['BAN_TAR']=line[190:194]
        CSR_BCCBSPL_tbl['TAX_EXMPT']=line[196:205] 
        CSR_BCCBSPL_tbl['UNB_TAR_IND']=line[207:208]
        CSR_BCCBSPL_tbl['EOBDATECC']=CSR_BCCBSPL_tbl['EOB_DATE']
        CSR_BCCBSPL_tbl['INPUT_RECORDS']=str(record_id)
    
        current_abbd_rec_key_concat=current_abbd_rec_key['ACNA'].rstrip(' ')  + current_abbd_rec_key['BAN'].rstrip(' ')  + current_abbd_rec_key['EOB_DATE'].rstrip(' ')
        bccbsplKeys[current_abbd_rec_key_concat]=csr_bccbspl_sq
        CSR_BCCBSPL_tbl['ID']=csr_bccbspl_sq
        csr_bccbspl_records[csr_bccbspl_sq]= copy.deepcopy(CSR_BCCBSPL_tbl)
        csr_bccbspl_sq += 1            
    
        #writelog("insert csr_bccbspl",output_log)
     
        root_rec=True
    
    else:
        grplock=True
    
        
def process_ROOTREC_CHEKFID():
    "051000,051100"
    global record_id
    global output_log
    global current_abbd_rec_key_concat
    global bccbsplKeys

#    FIXFORM X-225 X5 X6 X13 X31 X32 X32 X32 X32
#    FIXFORM BILLFID/4 BILLFID_DATA/28
    
    if line[189:193] == 'CCNA':
        csr_bccbspl_records[bccbsplKeys[current_abbd_rec_key_concat]]['CCNA']=line[193:196]
        csr_bccbspl_records[bccbsplKeys[current_abbd_rec_key_concat]]['INPUT_RECORDS']+=","+str(record_id)
        #writelog("update csr_bccbspl CCNA",output_log)
        
    elif line[189:192] == 'MCN':
        csr_bccbspl_records[bccbsplKeys[current_abbd_rec_key_concat]]['MCN']=line[193:221]
        csr_bccbspl_records[bccbsplKeys[current_abbd_rec_key_concat]]['INPUT_RECORDS']+=","+str(record_id)
        #writelog("update csr_bccbspl MCN",output_log)
        
    else:
        #continue on, skip
        pass

   
def process_ROOTREC_UPROOT():
    global record_id
    global output_log
    global current_abbd_rec_key_concat
    global bccbsplKeys
    global csr_bccbspl_records
    "ROOTREC  -  101500"    
#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 TAPE/A1 X128       
 
    if root_rec==True:
        if line[61:62].rstrip(' ') != '':
            csr_bccbspl_records[bccbsplKeys[current_abbd_rec_key_concat]]['TAPE']=line[61:62]
            csr_bccbspl_records[bccbsplKeys[current_abbd_rec_key_concat]]['INPUT_RECORDS']+=","+str(record_id)

        else:
            writelog("No TAPE value on input data for ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'],output_log)
                 ##ONCE WE HIT THIS RECORD WE CAN PURGE/WRITE OUT the ROOT RECORD:
###        process_insert_table("CAIMS_CSR_BCCBSPL", CSR_BCCBSPL_tbl, CSR_BCCBSPL_DEFN_DICT,con,output_log)
        #writelog("insert csr_bccbspl",output_log)
        
    else:
        process_ERROR_END("ERROR: Encountered UPROOT record (record id "+record_id+") but no root record has been created.")

 

def process_BILLREC_BILLTO():   
    global record_id
    global output_log
    global current_abbd_rec_key_concat
    global csr_billrec_records
    global csr_billrec_sq
    global bccbsplKeys
    global billrecKeys
    
    "100500"
#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X35 BILLNAME/A32
#    FIXFORM BADDR1/A32 BADDR2/A32 BADDR3/A32 BADDR4/A32

    initialize_tbl('CSR_BILLREC_tbl')    
      
    CSR_BILLREC_tbl['BILLNAME']=line[65:97]
    CSR_BILLREC_tbl['BADDR1']=line[97:129]
    CSR_BILLREC_tbl['BADDR2']=line[129:161]
    CSR_BILLREC_tbl['BADDR3']=line[161:193]
    CSR_BILLREC_tbl['BADDR4']=line[193:225]
    CSR_BILLREC_tbl['INPUT_RECORDS']=str(record_id)
###    process_insert_table("CAIMS_CSR_BILLREC", CSR_BILLREC_tbl, CSR_BILLREC_DEFN_DICT,con,output_log)
    parentId=bccbsplKeys[current_abbd_rec_key_concat]           #Assign the parent id    
    bill_key_concat=str(parentId) + CSR_BILLREC_tbl['BILLNAME']       #Set new billrec key
    billrecKeys[bill_key_concat]=csr_billrec_sq
    CSR_BILLREC_tbl['ID']=csr_billrec_sq
    CSR_BILLREC_tbl['BCCBSPL_ID']=parentId
    csr_billrec_records[csr_billrec_sq] = copy.deepcopy(CSR_BILLREC_tbl)
    csr_billrec_sq += 1
    #writelog("insert csr_billrec",output_log)

def process_ACTLREC_BILLTO(): 
    global record_id
    global output_log
    global current_abbd_rec_key_concat
    global csr_actlrec_records
    global csr_actlrec_sq
    global bccbsplKeys
    global actlrecKeys
    "ACTLREC  -  101000"    
    
    initialize_tbl('CSR_ACTLREC_tbl')
    CSR_ACTLREC_tbl['ACTL_NUM']=line[61:65]
    
    CSR_ACTLREC_tbl['ACTL']=line[65:76]
    CSR_ACTLREC_tbl['ACTLADDR1']=line[76:106]
    CSR_ACTLREC_tbl['ACTLADDR2']=line[106:136]
    CSR_ACTLREC_tbl['ACTLADDR3']=line[136:166]
    CSR_ACTLREC_tbl['CUST_NAME']=line[167:197]
    #CSR_ACTLREC_tbl['FGRP']=line[217:218]
    CSR_ACTLREC_tbl['INPUT_RECORDS']=str(record_id)
    
###    process_insert_table("CAIMS_CSR_ACTLREC", CSR_ACTLREC_tbl, CSR_ACTLREC_DEFN_DICT,con,output_log)
    parentId=bccbsplKeys[current_abbd_rec_key_concat]           #Assign the parent id
    
    actlkey_concat=str(parentId) + CSR_ACTLREC_tbl['ACTL_NUM']       #Set new actl key
    actlrecKeys[actlkey_concat]=csr_actlrec_sq
    CSR_ACTLREC_tbl['ID']=csr_actlrec_sq
    CSR_ACTLREC_tbl['BCCBSPL_ID']=parentId
    csr_actlrec_records[csr_actlrec_sq] = copy.deepcopy(CSR_ACTLREC_tbl)
    csr_actlrec_sq += 1
    #writelog("insert csr_actlrec",output_log)
 

def process_TYP1505_CKT_LOC():
    global Level
    global usoccnt
    global record_id
    global output_log
    global CSR_CKT_exists
    global csr_ckt_sq
    global csr_loc_sq
    global current_abbd_rec_key_concat
    global current_ckt_key_concat
    global current_loc_key_concat
    global csr_ckt_records
    global csr_loc_records
    global bccbsplKeys
    global cktKeys
    global locKeys
    
    "150500"
#    FIXFORM X-159 TFID/4 X1 QTEST/120 X6 X21 ACTVY_IND/1
        
    tfid=line[72:76].rstrip(' ')
    qtest=line[77:197]
    actvy_ind=line[224:225]
    
    if actvy_ind=='D':
        pass  #SKIP record
    if any(s in qtest for s in ('/NC','/PIU','/ASG','/LCC','/BAND','/TAR' '/RTAR','/LLN',
                            '/TLI','/PICX','/HML','/HTG','/TN','/TER','/STN','/SFN',  
                            '/SFG','/CKR','/GSZ','/NHN','/PTN','/SBN','/LSO','/FSO',  
                            '/NCI','/ICO','/SGN','/DES','/HSO','/CFA','/XR','/SN','/LSOC')):  
        pass   #SKIP record
    else: #CREATE CKTSEG
        if tfid in ('CLS','CLF','CLT','CLM'):

            initialize_tbl('CSR_CKT_tbl')
            ##FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
            #FIXFORM CDINSTCC/8 X3 FID/A4 X1 PSM/16 X-16 CIRCUIT/A53 X67
            #FIXFORM X1 CKT_LTP/A4 X1 CKT_MAN_BAN_UNITS/Z7.4 X6
            #FIXFORM CDACTCC/8 CACT/A1
            Level = 'C'
            
            CSR_CKT_tbl['CDINSTCC']=line[61:69]
            CSR_CKT_tbl['FID']=tfid
            CSR_CKT_tbl['PSM']=line[77:93]
            CSR_CKT_tbl['CIRCUIT']=line[77:130]
            CSR_CKT_tbl['CKT_LTP']=line[198:202]
            CSR_CKT_tbl['CKT_MAN_BAN_UNITS']=convertnumber(line[203:210],4)
            CSR_CKT_tbl['CDACTCC']=line[216:224]
            CSR_CKT_tbl['CACT']=line[224:225]
            CSR_CKT_tbl['INPUT_RECORDS']=str(record_id) 
###            process_insert_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)

            parentId=bccbsplKeys[current_abbd_rec_key_concat]                   #Assign the parent id    
            ckt_key_concat=str(parentId) + CSR_CKT_tbl['PSM'] + CSR_CKT_tbl['CIRCUIT']   #Set new circuit key
            current_ckt_key_concat=ckt_key_concat
            cktKeys[ckt_key_concat]=csr_ckt_sq
            
            CSR_CKT_tbl['ID']=csr_ckt_sq
            CSR_CKT_tbl['BCCBSPL_ID']=parentId
            csr_ckt_records[csr_ckt_sq] = copy.deepcopy(CSR_CKT_tbl)
            csr_ckt_sq += 1
            CSR_CKT_exists=True
            
            #writelog("insert csr_ckt",output_log)
            
        elif tfid in  ('CKL','CKLT'):
            initialize_tbl('CSR_LOC_tbl')
            Level = 'L'
            #FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
            #FIXFORM LDINSTCC/8 X3 CKLFID/A4 X1 LOC_DATA/A60 X60
            #FIXFORM X1 LOC_LTP/A4 X1 LOC_MAN_BAN_UNITS/Z7.4 X6
            #FIXFORM LDACTCC/8 LACT/A1
            #below:It would ignore the first two characters and get the 3rd and 4th.
            #TLOC = EDIT(LOC_DATA,'$$99$') 

            usoccnt=0

            CSR_LOC_tbl['LDINSTCC']=line[61:69]
            CSR_LOC_tbl['CKLFID']=line[72:76]
            CSR_LOC_tbl['LOC_DATA']=line[77:137]
            tloc=line[79:81]
            CSR_LOC_tbl['LOC_LTP']=line[198:202]
            CSR_LOC_tbl['LOC_MAN_BAN_UNITS']=line[203:210]
            CSR_LOC_tbl['LDACTCC']=line[216:224]
            CSR_LOC_tbl['LACT']=line[224:225]
            
            
            if tloc == '1-':
                CSR_LOC_tbl['LOC'] ='1'
            elif tloc == '2-':
                CSR_LOC_tbl['LOC'] ='2'
            elif tloc == '3-':
                CSR_LOC_tbl['LOC'] ='3'
            elif tloc == '4-':
                CSR_LOC_tbl['LOC'] ='4'
            elif tloc == '5-':
                CSR_LOC_tbl['LOC'] ='5'
            elif tloc == '6-':
                CSR_LOC_tbl['LOC'] ='6'
            elif tloc == '7-':
                CSR_LOC_tbl['LOC'] ='7'
            elif tloc == '8-':
                CSR_LOC_tbl['LOC'] ='8'
            elif tloc == '9-':
                CSR_LOC_tbl['LOC'] ='9'
            else:
                CSR_LOC_tbl['LOC'] =tloc 
                
            CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
            
            #process_insert_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
            parentId=cktKeys[current_ckt_key_concat]                   #Assign the parent id    
            loc_key_concat=str(parentId) + CSR_LOC_tbl['LOC']   #Set new loc key
            current_loc_key_concat=loc_key_concat
            locKeys[loc_key_concat]=csr_loc_sq
            CSR_LOC_tbl['ID']=csr_loc_sq
            CSR_LOC_tbl['CKT_ID']=parentId
            csr_loc_records[csr_loc_sq] = copy.deepcopy(CSR_LOC_tbl)
            csr_loc_sq += 1
            #writelog("insert csr_loc",output_log)
        else:
            pass #SKIP record
            
       
def process_FIDDRVR():
    "150600, 151001 records"
#    -************************************************************
#    -* PROCESS '401506' AND '401511' RECORDS. THESE ARE FLOATING FIDS
#    -* ONLY.  LEFT-HANDED FIDS ARE PROCESSED IN THE TYP1505 CASE
#    -************************************************************
    global Level
    global tfid
    global record_id
    global output_log
    global current_usoc_key_concat
    global current_cktusoc_key_concat
    global usocKeys
    global cktusocKeys
    global csr_cufid_records
    global csr_cufid_sq
    global csr_ufid_records
    global csr_ufid_sq
    
    "150600, 151001"
    #FIXFORM X-159 TFID/4 X121 X6 X21 ACTVY_IND/1    
    
    tfid=line[72:76] 
    actvy_ind=line[224:225]
    
    if actvy_ind == 'D':
        pass
    elif (Level == 'F' and tfid.rstrip(' ') == 'ASG' ) \
        or (Level == 'LU' and tfid.rstrip(' ') == 'ASG') \
        or (Level == 'CU' and tfid.rstrip(' ') == 'ASG') \
        or (Level == 'CO' and tfid.rstrip(' ') == 'ASG') \
        or (Level == 'L' and tfid.rstrip(' ') == 'ASG'):
            pass
    elif Level == 'C':
        process_CKTFIDRVR()
        
    elif Level == 'CU':
        #FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X42
        #FIXFORM CUFID/A4 X1 CUFID_DATA/36 X85
        #FIXFORM FGRP/1 X26
        initialize_tbl('CSR_CUFID_tbl')
#        initialize_CUFID_tbl()
#        debug("CSR_CUFID_tbl:"+record_id)
        CSR_CUFID_tbl['CUFID']=tfid
        CSR_CUFID_tbl['CUFID_DATA']=line[77:113]
        #CSR_CUFID_tbl['FGRP']=line[198:199]
        CSR_CUFID_tbl['INPUT_RECORDS']=str(record_id)
        
        CSR_CUFID_tbl['ID']=csr_cufid_sq
        CSR_CUFID_tbl['CKTUSOC_ID']=cktusocKeys[current_cktusoc_key_concat]  #parent id is id from CAIMS_CSR_CKTUSOC
        csr_cufid_records[csr_cufid_sq]= copy.deepcopy(CSR_CUFID_tbl)
        csr_cufid_sq += 1 
        
###        process_insert_table("CAIMS_CSR_CUFID", CSR_CUFID_tbl, CSR_CUFID_DEFN_DICT,con,output_log)
        
    elif Level == 'CO':
        process_COSFID()
    elif Level == 'L':
        process_LOCFIDRVR()
    elif Level == 'LU':
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X42
#        FIXFORM UFID/A4 X1 UFID_DATA/A36 X85
#        FIXFORM FGRP/1 X26
               
        ufid=line[72:76]
        if ufid == 'ASG':
            pass #go to top
        else:
            initialize_tbl('CSR_UFID_tbl') 
            CSR_UFID_tbl['UFID']=ufid
            CSR_UFID_tbl['UFID_DATA']=line[77:113]
            #CSR_UFID_tbl['FGRP']=line[198:199]
            CSR_UFID_tbl['INPUT_RECORDS']=str(record_id)
            
            CSR_UFID_tbl['ID']=csr_ufid_sq
            CSR_UFID_tbl['USOC_ID']=usocKeys[current_usoc_key_concat]  #parent id is id from CAIMS_CSR_CKT
            csr_ufid_records[csr_ufid_sq]= copy.deepcopy(CSR_UFID_tbl)
            csr_ufid_sq += 1 
            
            #writelog("insert csr_ufid",output_log)
###            process_insert_table("CAIMS_CSR_UFID", CSR_UFID_tbl, CSR_UFID_DEFN_DICT,con,output_log)   
    else:
        pass #go to top

def process_LOCFIDRVR():
    global tfid, Level
    global record_id
    global output_log
    global current_loc_key_concat    
    global locKeys
    global csr_loc_records

    if tfid == 'LSO ':                      
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LSO/7
        csr_loc_records[locKeys[current_loc_key_concat]]['LSO']=line[77:84]
        #writelog("update csr_loc",output_log)  
        
    elif tfid == 'FSO ':  
        #FIXFORM X-225 X5 X6 X13 X42 X4 X1 FSO/7
        csr_loc_records[locKeys[current_loc_key_concat]]['FSO']=line[77:84]
        #writelog("update csr_loc",output_log)  
        
    elif tfid == 'NCI ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 NCI/13
        csr_loc_records[locKeys[current_loc_key_concat]]['NCI']=line[77:90]
        #writelog("update csr_loc",output_log)
        
    elif tfid == 'NC  ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LNCODE/4
        csr_loc_records[locKeys[current_loc_key_concat]]['LNCODE']=line[77:81]
        #writelog("update csr_loc",output_log)  
        
    elif tfid == 'ICO ': 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 ICO/4
        csr_loc_records[locKeys[current_loc_key_concat]]['ICO']=line[77:81]
        #writelog("update csr_loc",output_log)
        
    elif tfid == 'SGN ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SGN/3
        csr_loc_records[locKeys[current_loc_key_concat]]['SGN']=line[77:80]
        #writelog("update csr_loc",output_log)
        
    elif tfid == 'TAR ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LTAR/4
        csr_loc_records[locKeys[current_loc_key_concat]]['LTAR']=line[77:81]
        #writelog("update csr_loc",output_log)
        
    elif tfid == 'RTAR': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LRTAR/4
        csr_loc_records[locKeys[current_loc_key_concat]]['LRTAR']=line[77:81]
        #writelog("update csr_loc",output_log)
        
    elif tfid == 'DES ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LDES/36
        csr_loc_records[locKeys[current_loc_key_concat]]['LDES']=line[77:113]
        #writelog("update csr_loc",output_log)
        
    elif tfid == 'HSO ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 HSO/7
        csr_loc_records[locKeys[current_loc_key_concat]]['HSO']=line[77:84]
        #writelog("update csr_loc",output_log)
        
    elif tfid == 'CFA ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 CFA/40
        csr_loc_records[locKeys[current_loc_key_concat]]['CFA']=line[77:117]
        #writelog("update csr_loc",output_log)
        
    elif tfid == 'XR  ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 XR/4
        csr_loc_records[locKeys[current_loc_key_concat]]['XR']=line[77:81]
        #writelog("update csr_loc",output_log)  
    elif tfid == 'SN  ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SN/30
        csr_loc_records[locKeys[current_loc_key_concat]]['SN']=line[77:107]
        #writelog("update csr_loc",output_log)
    else:  
        process_LOCFID()               
 

def process_LOCFID():
    global record_id
    global output_log
    global current_loc_key_concat    
    global locKeys    
    global csr_lfid_records
    global csr_lfid_sq

#     -* THIS CASE READS THE  FLOATED FID AND LOADS THE CORRESPONDING
#     -* FIELD IN THE LFIDSEG SEGMENT OF THE DATABASE.
#     -************************************************************ 
#     FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X42
#     FIXFORM LOCFID/A4 X1 LF_DATA/A36 X85
#     FIXFORM FGRP/1 X26  
    
    initialize_tbl('CSR_LFID_tbl')
#    initialize_LFID_tbl()
    
    CSR_LFID_tbl['LOCFID']=line[72:76]
    CSR_LFID_tbl['LOCFID_DATA']=line[77:113]
#    CSR_LFID_tbl['FGRP']=line[198:199]
    CSR_LFID_tbl['INPUT_RECORDS']=str(record_id)
    
    CSR_LFID_tbl['ID']=csr_lfid_sq
    CSR_LFID_tbl['LOC_ID']=locKeys[current_loc_key_concat]  #parent id is id from CAIMS_CSR_LOC
    csr_lfid_records[csr_lfid_sq]= copy.deepcopy(CSR_LFID_tbl)
    csr_lfid_sq += 1    
    
    #writelog("insert lfid_tbl",output_log)  
 
def process_COSFID():   
    global tfid, Level,record_id
    global output_log
    global current_ckt_key_concat
    global cktKeys
    global csr_ckt_records
    global csr_cosfid_records
    global csr_cosfid_sq

#-************************************************************
#-* PROCESS '401511' RECORDS FROM CASE FIDDRVR WHEN Level EQUALS 'CO'.
#-* THIS CASE READS THE FLOATED FID AND LOADS THE COSFID & COSFID_DATA
#-* FIELD IN COSFID   SEGMENT OF THE DATABASE.
#-************************************************************
  
    initialize_tbl('CSR_COSFID_tbl')    
#                FIXFORM X-225 X5 X6 X13 X42
#                FIXFORM COSFID/A4 X1 COSFID_DATA/36 X85
#                FIXFORM X1 X26
    CSR_COSFID_tbl['COSFID']=line[72:76]
    CSR_COSFID_tbl['COSFID_DATA']=line[77:113]
    CSR_COSFID_tbl['INPUT_RECORDS']=str(record_id)
    
    CSR_COSFID_tbl['ID']=csr_cosfid_sq
    CSR_COSFID_tbl['CKT_ID']=cktKeys[current_ckt_key_concat]  #parent id is id from CAIMS_CSR_CKT
    csr_cosfid_records[csr_cosfid_sq]= copy.deepcopy(CSR_COSFID_tbl)
    csr_cosfid_sq += 1 
    
    #writelog("update csr_cosfid",output_log)  

def process_CKTFIDRVR():
    global Level
    global tfid
    global  record_id
    global output_log
    global current_ckt_key_concat
    global cktKeys
    global csr_ckt_records
    global csr_cfid_records
    global csr_cfid_sq
    
    if tfid == 'NC  ': 
         #FIXFORM X-225 X5 X6 X13 X42 X4 X1 NCODE/4
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['NCODE']=line[77:81]
        #writelog("update csr_ckt",output_log)
         
    elif tfid == 'PIU ': 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 PIU/3
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['PIU']=line[77:80]
        #writelog("update csr_ckt",output_log) 
        
    elif tfid == 'ASG ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 ASG/6
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['ASG']=line[77:83]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'LCC ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LCC/3
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['LCC']=line[77:80]
        #writelog("update csr_ckt",output_log)  
         
    elif tfid == 'BAND': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 BAND/1
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['BAND']=line[77:78]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'TAR ': 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 TAR/4
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['TAR']=line[77:81]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'RTAR': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 RTAR/4
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['RTAR']=line[77:81]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'LLN ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LLN/12
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['LLN']=line[77:89]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'TLI ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 TLI/12
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['TLI']=line[77:89]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'PICX': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 PICX/3
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['PICX']=line[77:80]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'HML ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 HML/40
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['HML']=line[77:117]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'HTG ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 HTG/40
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['HTG']=line[77:117]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'TN  ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 TN/12
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['TN']=line[77:89]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'TER ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 TER/4
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['TER']=line[77:81]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'STN ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 STN/24
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['STN']=line[77:91]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'SFN ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SFN/4
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['SFN']=line[77:81]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'SFG ': 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SFG/6
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['SFG']=line[77:83] 
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'CKR ': 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 CKR/36
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['CKR']=line[77:113]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'GSZ ': 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 GSZ/3
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['GSZ']=line[77:80]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'NHN ': 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 NHN/12
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['NHN']=line[77:89]
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'PTN ': 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 PTN/12
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['PTN']=line[77:89] 
        #writelog("update csr_ckt",output_log)  
        
    elif tfid == 'SBN ': 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 SBN/40
        csr_ckt_records[cktKeys[current_ckt_key_concat]]['SBN']=line[77:117]
        #writelog("update csr_ckt",output_log)  
        
    else:
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X42
#        FIXFORM CFID/A4 X1 FID_DATA/36
        if Level == 'C':
            initialize_tbl('CSR_CFID_tbl')
            CSR_CFID_tbl['CFID']=line[72:76]
            CSR_CFID_tbl['FID_DATA']=line[77:113]
            CSR_CFID_tbl['INPUT_RECORDS']=str(record_id)
 
            CSR_CFID_tbl['ID']=csr_cfid_sq
            CSR_CFID_tbl['CKT_ID']=cktKeys[current_ckt_key_concat]  #parent id is id from CAIMS_CSR_CKT
            csr_cfid_records[csr_cfid_sq]= copy.deepcopy(CSR_CFID_tbl)
            csr_cfid_sq += 1            
            #writelog("insert csr_cfid",output_log)            

def process_USOCDRVR_36():

    "151000"
    global Level 
    global record_id
    
#    FIXFORM X-225 X5 X6 X13 X31
#    FIXFORM X8 X6 X2 TUSOC/5 X121 X6 X7
#    FIXFORM X8 X1

    if Level in ('L','LU'):
        process_LOCUSOC_36()
    else:
#    FIXFORM X-225 X5 X6 X13 X31
#    FIXFORM X8 X6 X2 TUSOC/5 X121 X6 X7
#    FIXFORM X8 X1
        tusoc=line[77:82]
        if tusoc.rstrip(' ') in ( 'UDP','U3P','U6P'):
            process_CKTUSOC_36()
        else:
            process_COSUSOC_36()
     
def process_CKTUSOC_36():    
    global Level
    global record_id
    global cuact
    global output_log
    global current_ckt_key_concat
    global cktKeys
    global csr_ckt_records
    global current_cktusoc_key_concat
    global cktusocKeys
    global csr_cktusoc_records
    global csr_cktusoc_sq

#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
#    FIXFORM CUDINSTCC/8 CUSOCQTY/Z6 CURES1/2 CUSOC/5 X121
#    FIXFORM CURES2/6 X7 CUDACTCC/8 CUACT/1

    if Level in ('L', 'LU'):
        process_LOCUSOC_36()
    else:
    #    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
    #    FIXFORM CUDINSTCC/8 CUSOCQTY/Z5 CUSOC/5 X137
    #    FIXFORM CUDACTCC/8 CUACT/1
            
        Level = 'CU'
        cuact=line[224:225]               
        
        if cuact == 'D':
            pass
        else:
            initialize_tbl('CSR_CKTUSOC_tbl')
        
        CSR_CKTUSOC_tbl['CUDINSTCC']=line[61:69]
        CSR_CKTUSOC_tbl['CUSOCQTY']=convertnumber(line[69:74],0)
        CSR_CKTUSOC_tbl['CUSOC']=line[74:79]
        CSR_CKTUSOC_tbl['CUDACTCC']=line[216:224]
            
        CSR_CKTUSOC_tbl['CUACT']=cuact
        CSR_CKTUSOC_tbl['INPUT_RECORDS']=str(record_id) 
        
            parentId=cktKeys[current_ckt_key_concat]
            cktusoc_key_concat=str(parentId) + CSR_CKTUSOC_tbl['CUSOC']
            current_cktusoc_key_concat=cktusoc_key_concat

            CSR_CKTUSOC_tbl['ID']=csr_cktusoc_sq
            CSR_CKTUSOC_tbl['CKT_ID']=parentId
            csr_cktusoc_records[csr_cktusoc_sq]= copy.deepcopy(CSR_CKTUSOC_tbl)
            csr_cktusoc_sq += 1  
            #writelog("insert csr_cktusoc",output_log)  
    
def process_COSUSOC_36():    
    global Level, record_id
    global output_log
    global CSR_CKT_exists
    global current_ckt_key_concat
    global cktKeys
    global csr_ckt_records
    
#    FIXFORM X-225 X5 X6 X13 X31
#    FIXFORM COSDINSTCC/8 COSUSOCQTY/Z6 COSRES1/2 COS_USOC/5 X121
#    FIXFORM COSRES2/6 X7 COSDACTCC/8 COSACT/1
    if Level in ('L','LU'):
         process_LOCUSOC_36()  
    else:
        Level = 'CO'
        if line[224:225] == 'D':
            pass
        else:
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COSDINSTCC']=line[61:69]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COSUSOCQTY']=convertnumber(line[69:75],0)
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COSRES1']=line[75:77]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_USOC']=line[77:82]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COSRES2']=line[203:209]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COSDACTCC']=line[216:224]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COSACT']=line[224:225]
            
            #writelog("update csr_ckt",output_log)

def process_LOCUSOC_36():     
    global Level, usoccnt, uact
    global record_ID
    global output_log
    global usoccnt,ucode,usoc_cnt, Level
    global current_loc_key_concat    
    global current_usoc_key_concat
    global locKeys
    global usocKeys
    global csr_usoc_records
    global csr_usoc_sq

 #USOC
    if Level in ('L','LU'):
        
    usoccnt = usoccnt + 1
    usoc_cnt = usoccnt
    ucode = '  '
    Level = 'LU'
 
#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
#    FIXFORM UDINSTCC/8 QUSOC/Z6 USOCRES1/2 USOC/5 X121
#    FIXFORM USOCRES2/6 X7 UDACTCC/8 UACT/A1
    uact=line[224:225]

    if uact == 'D':
        pass #GOTO TOP
    else:     
            initialize_tbl('CSR_USOC_tbl')
            
        CSR_USOC_tbl['UDINSTCC']=line[61:69]
        CSR_USOC_tbl['QUSOC']=convertnumber(line[69:75],0)
        CSR_USOC_tbl['USOCRES1']=line[75:76]
        CSR_USOC_tbl['USOC']=line[77:82]
        CSR_USOC_tbl['USOC_CNT']=usoc_cnt
        CSR_USOC_tbl['USOCRES2']=line[203:209]
        CSR_USOC_tbl['UDACTCC']=line[216:224]
        CSR_USOC_tbl['UACT']=uact
        CSR_USOC_tbl['INPUT_RECORDS']=str(record_id) 

            CSR_USOC_tbl['ID']=csr_usoc_sq
            parentId=locKeys[current_loc_key_concat]                #Assign the parent id -- id from CAIMS_CSR_LOC
            usoc_key_concat=str(parentId) + CSR_USOC_tbl['USOC']
            usocKeys[usoc_key_concat]=csr_usoc_sq                   #Add Usoc Key for ufid segment parent
            current_usoc_key_concat=usoc_key_concat
            CSR_USOC_tbl['LOC_ID']=locKeys[current_loc_key_concat]
            csr_usoc_records[csr_usoc_sq]= copy.deepcopy(CSR_USOC_tbl)
            csr_usoc_sq += 1             
            
            #writelog("insert csr_usoc",output_log)
            
            
def process_USOCDRVR_TAX():
    "151600"
    global x_tbl
    global Level,record_id
    global usoc_cnt, ucode, cuact, cusocqty, uact, Level, circuit,cosact
    global output_log
    global cktKeys, usocKeys, cktusocKeys
    global current_ckt_key_concat, current_usoc_key_concat, current_cktusoc_key_concat
    global csr_ckt_records, csr_usoc_records, csr_cktusoc_records
    
#        FIXFORM X-225 X5 X6 X13 X31 X11 TUSOC/5 X121 X9
#        FIXFORM X3 X2 X4 X1 X8
    if Level in ('L','LU'):
#        GOTO LOCUSOC_TAX  logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES THE 1516 LOCATION Level USOCS FROM CASE CKTUSOC
#        -*   IF Level EQUALS 'L' OR 'LU'
#        -************************************************************
        ucode = '  '
        Level = 'LU'
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM USOC/5 X121 USO_FEDTAX/1 USO_STTAX/1 USO_CITYTAX/1
#        FIXFORM USO_CNTYTAX/1 USO_STSLSTAX/1 USO_LSLSTAX/1 USO_SURTAX/1
#        FIXFORM USO_FRANTAX/1 USO_OTHERTAX/1 X3 X2 X4 X1 X8
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC']=line[72:77]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USO_FEDTAX']=line[198:199]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USO_STTAX']=line[199:200]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USO_CITYTAX']=line[200:201]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USO_CNTYTAX']=line[201:202]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USO_STSLSTAX']=line[202:203]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USO_LSLSTAX']=line[203:204]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USO_SURTAX']=line[204:205]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USO_FRANTAX']=line[205:206]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USO_OTHERTAX']=line[206:207]

        #writelog("update csr_usoc",output_log)  
   
    elif tusoc.rstrip(' ') in ('UDP','U3P','U6P'):
#        GOTO CKTUSOC_TAX  logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES CIRCUIT Level USOC RECORDS 401516. AND
#        -*  UPDATES THE CKTUSOC SEGMENT. SET Level TO 'CU'.
#        -*  CHECK TO SEE IF THIS IS A CIRCUIT Level RECORD OR LOCATION Level
#        -************************************************************
        Level = 'CU'
        #cuact = cuact
        #cusocqty = cusocqty
#        SEGMENT=CKTUSOC
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM CUSOC/5 X121 CU_FEDTAX/1 CU_STTAX/1 CU_CITYTAX/1
#        FIXFORM CU_CNTYTAX/1 CU_STSLSTAX/1 CU_LSLSTAX/1 CU_SURTAX/1
#        FIXFORM CU_FRANTAX/1 CU_OTHERTAX/1 X3 X2 X4 X1 X8
        csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CUSOC']=line[72:77]
        csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_FEDTAX']=line[198:199]
        csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_STTAX']=line[199:200]
        csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_CITYTAX']=line[200:201]
        csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_CNTYTAX']=line[201:202]
        csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_STSLSTAX']=line[202:203]
        csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_LSLSTAX']=line[203:204]
        csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_SURTAX']=line[204:205]
        csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_FRANTAX']=line[205:206]
        csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_OTHERTAX']=line[206:207]
    
    else:
#        GOTO COSUSOC_TAX   logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES CIRCUIT Level USOC RECORDS 401516. AND
#        -*  UPDATES THE CKTUSOC SEGMENT. SET Level TO 'CO'.
#        -*  CHECK TO SEE IF THIS IS A CIRCUIT Level RECORD OR LOCATION Level
#        -************************************************************
#SEGMENT=CKTSEG
        Level = 'CO'
        circuit = circuit
        cosact = cosact
#        FIXFORM X-225 X5 X6 X13 X31 X11
#        FIXFORM COS_USOC/5 X121 COS_FEDTAX/1 COS_STTAX/1 COS_CITYTAX/1
#        FIXFORM COS_CNTYTAX/1 COS_STSLSTAX/1 COS_LSLSTAX/1 COS_SURTAX/1
#        FIXFORM COS_FRANTAX/1 COS_OTHERTAX/1 X3 X2 X4 X1 X8
        if cosact == 'D':
            pass #GOTO TOP
        else:
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_USOC']=line[72:77]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_FEDTAX']=line[198:199]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_STTAX']=line[199:200]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_CITYTAX']=line[200:201]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_CNTYTAX']=line[201:202]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_STSLSTAX']=line[202:203]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_LSLSTAX']=line[203:204]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_SURTAX']=line[204:205]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_FRANTAX']=line[205:206]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_OTHERTAX']=line[206:207]        

            #writelog("update csr_ckt ",output_log)  


def process_USOCDRVR_PLN():    
    "151700"
    global x_tbl
    global output_log
    global current_abbd_rec_key   
    global Level, record_id
    global usoccnt,usoc_cnt,ucode
    global cktKeys, usocKeys, cktusocKeys
    global current_ckt_key_concat, current_usoc_key_concat, current_cktusoc_key_concat
    global csr_ckt_records, csr_usoc_records, csr_cktusoc_records
#    FIXFORM X-225 X5 X6 X13 X31 X11 TUSOC/5 X26 X5 X5 X8 X8 X3
#    FIXFORM X1 X5 X2 X1 X5 X26 X8 X1 X44    
    TUSOC=line[72:77]
    
    if Level in ('L','LU'):
#        GOTO LOCUSOC_PLN  logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES THE 1517 LOCATION Level USOCS FROM CASE CKTUSOC
#        -*   IF Level EQUALS 'L' OR 'LU'
#        -************************************************************
#  SEGMENT=USOCSEG
    
        usoc_cnt = usoc_cnt 
#        QUSOC = QUSOC 
        ucode = '  ' 
        Level = 'LU' 
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM USOC/5 PLAN_ID/26 RQRD_PL_QTY/5 VAR_PL_QTY/5
#        FIXFORM TRM_STRT_DAT/8 TRM_END_DAT/8 LNGTH_OF_TRM/3 PL_TYP_INDR/1
#        FIXFORM DSCNT_PCTGE/Z5.2 SYS_CPCTY/2 ADD_DIS_PL_IND/1
#        FIXFORM SPL_OFR_PCTGE/Z5.2 SPL_OFR_IDENT/26 S_OFR_END_DAT/8
#        FIXFORM PL_TAR_TYP_INDR/1 X44
        
        if uact == 'D':
            pass #GOTO TOP
        else:
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC']=line[72:77]
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['PLAN_ID']=line[77:103]
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['RQRD_PL_QTY']=convertnumber(line[103:108],0)
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['VAR_PL_QTY']=convertnumber(line[108:113],0)
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['TRM_STRT_DAT']=line[113:121]
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['TRM_END_DAT']=line[121:129]
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['LNGTH_OF_TRM']=convertnumber(line[129:132],0)
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['PL_TYP_INDR']=convertnumber(line[132:133],0)
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['DSCNT_PCTGE']=convertnumber(line[133:138],2)
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['SYS_CPCTY']=convertnumber(line[138:140],0)
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['ADD_DIS_PL_IND']=line[140:141]
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['SPL_OFR_PCTGE']=convertnumber(line[141:146],2)
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['SPL_OFR_IDENT']=line[146:172]
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['S_OFR_END_DAT']=line[172:180]
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['PL_TAR_TYP_INDR']=line[180:181]
#            csr_usoc_records[usocKeys[current_usoc_key_concat]]['INPUT_RECORDS']+=","+str(record_id)
#            process_update_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT,con,output_log)
            #writelog("update csr_usoc 151700 ",output_log)  


    elif TUSOC.rstrip(' ') in ('UDP', 'U3P','U6P'):
#        GOTO CKTUSOC_PLN  logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES CIRCUIT Level USOC RECORDS 401517. AND
#        -*  UPDATES THE CKTUSOC SEGMENT. SET Level TO 'CU'.
#        -*  CHECK TO SEE IF THIS IS A CIRCUIT Level RECORD OR LOCATION Level
#        -************************************************************
#SEGMENT=CKTUSOC
        Level = 'CU' 
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM CUSOC/5 CU_PLAN_ID/26 CU_RQRD_PL_QTY/5 CU_VAR_PL_QTY/5
#        FIXFORM CU_TRM_STRT_DAT/8 CU_TRM_END_DAT/8 CU_LNGTH_OF_TRM/3
#        FIXFORM CU_PL_TYP_INDR/1 CU_DSCNT_PCTGE/Z5.2 CU_SYS_CPCTY/2
#        FIXFORM CU_ADD_DIS_PL_IND/1 CU_SPL_OFR_PCTGE/Z5.2 CU_SPL_OFR_IDENT/26
#        FIXFORM CU_S_OFR_END_DAT/8 CU_PL_TAR_TYP_INDR/1 X44
        if cuact == 'D':
            pass  #GOTO TOP
        else:
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CUSOC']=line[72:77]
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_PLAN_ID']=line[77:103]
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_RQRD_PL_QTY']=convertnumber(line[103:108],0)
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_VAR_PL_QTY']=convertnumber(line[108:113],0)
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_TRM_STRT_DAT']=line[113:121]
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_TRM_END_DAT']=line[121:129]
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_LNGTH_OF_TRM']=convertnumber(line[129:132],0)
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_PL_TYP_INDR']=line[132:133]
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_DSCNT_PCTGE']=convertnumber(line[133:138],2)
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_SYS_CPCTY']=convertnumber(line[138:140],0)
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_ADD_DIS_PL_IND']=line[140:141]
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_SPL_OFR_PCTGE']=convertnumber(line[141:146],2)
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_SPL_OFR_IDENT']=line[146:172]
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_S_OFR_END_DAT']=line[172:180]
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['CU_PL_TAR_TYP_INDR']=line[180:181]
            csr_cktusoc_records[cktusocKeys[current_cktusoc_key_concat]]['INPUT_RECORDS']+=","+str(record_id)
 
            #writelog("update csr_cktusoc 151700 ",output_log)  
#            
    else:
#        GOTO COSUSOC_PLN logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES CIRCUIT Level USOC RECORDS 401517. AND
#        -*  UPDATES THE CKTUSOC SEGMENT. SET Level TO 'CO'.
#        -*  CHECK TO SEE IF THIS IS A CIRCUIT Level RECORD OR LOCATION Level
#        -************************************************************
 
#        SEGMENT=CKTSEG 
        Level = 'CO' 
#        circuit = circuit 
#        cosact = cosact 
#        FIXFORM X-225 X5 X6 X13 X31 X11
#        FIXFORM COS_USOC/5 CO_PLAN_ID/26 CO_RQRD_PL_QTY/5 CO_VAR_PL_QTY/5
#        FIXFORM CO_TRM_STRT_DAT/8 CO_TRM_END_DAT/8 CO_LNGTH_OF_TRM/3
#        FIXFORM CO_PL_TYP_INDR/1 CO_DSCNT_PCTGE/Z5.2 CO_SYS_CPCTY/2
#        FIXFORM CO_ADD_DIS_PL_IND/1 CO_SPL_OFR_PCTGE/Z5.2 CO_SPL_OFR_IDENT/26
#        FIXFORM CO_S_OFR_END_DAT/8 CO_PL_TAR_TYP_INDR/1 X44
        
        if cosact == 'D':
            pass  #GOTO TOP
        else:
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['COS_USOC']=line[72:77]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_PLAN_ID']=line[77:103]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_RQRD_PL_QTY']=convertnumber(line[103:108],0)
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_VAR_PL_QTY']=convertnumber(line[108:113],0)
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_TRM_STRT_DAT']=line[113:121]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_TRM_END_DAT']=line[121:129]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_LNGTH_OF_TRM']=convertnumber(line[129:132],0)
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_PL_TYP_INDR']=line[132:133]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_DSCNT_PCTGE']=convertnumber(line[133:138],2)
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_SYS_CPCTY']=convertnumber(line[138:140],0)
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_ADD_DIS_PL_IND']=line[140:141]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_SPL_OFR_PCTGE']=convertnumber(line[141:146],2)
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_SPL_OFR_IDENT']=line[146:172]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_S_OFR_END_DAT']=line[172:180]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['CO_PL_TAR_TYP_INDR']=line[180:181]
            csr_ckt_records[cktKeys[current_ckt_key_concat]]['INPUT_RECORDS']+=","+str(record_id)            

            #writelog("update csr_ckt 151700",output_log)  

 
def  process_TYP1520():
    "152000"
    global x_tbl
    global output_log
    global current_abbd_rec_key   
    global Level, record_id
    global usoccnt,usoc_cnt,ucode
    global usocKeys
    global current_usoc_key_concat
    global csr_usoc_records
#    FIXFORM X-225 X5 X6 X13 X31 X11 TUSOC/5 X26 X5 X5 X8 X8 X3
#    FIXFORM X1 X5 X2 X1 X5 X26 X8 X1 X44    
#    -************************************************************
#    -*  THIS CASE UPDATES THE USOCSEG SEGMENT OF THE DATABASE WITH THE
#    -*   '401520' RECORDS FOR BOS VER GE 41
#    -************************************************************
#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X37
#    FIXFORM IRPCT/Z6.3 IRRATE/Z11.4 IRMRC/Z11.2 IAPCT/Z6.3
#    FIXFORM IARATE/Z11.4 IAMRC/Z11.2 MBIPFIX/A3 UFAC_CHG/A1 RATE_ZN_IR/1
#    FIXFORM USOC_RATE_SZN_IND_INT/1 X16
#    FIXFORM RAT_FCTR/Z8.7 USOC_PFBAND2/3 USOC_PFIIRIA/1 USOC_PFIIR/1
#    FIXFORM USOC_PFIIA/1 USOC_PFIIAIA/1 USOC_PFBAND1/3 RATE_ZN_IA/1
#    FIXFORM X16 LORATE/Z11.4 LOMRC/Z11.2 USO_O_LOC_PCT/3 X9
#    FIXFORM ACC_TYPE/1
#    FIXFORM LOPCT/Z6.3 RATE_ZN_LOC/1 MBIPVAR/3 X6
    
    
#    FIXFORM ON BCTFCSRI X61 IRIA_PCT/Z6.3 IRIARATE/Z11.4
#    FIXFORM IRIAMRC/Z11.2 IAIA_PCT/Z6.3 IAIARATE/Z11.4 IAIAMRC/Z11.2
#    FIXFORM X28 DSCT_FAC_LOC/Z5.4 X3 RATE_BAND/3 ALOC_UAMT/11
#    FIXFORM PCT_ORIG_USG/3 X1 SPDF_INTERST/Z5.4 SPDF_INTRAST/Z5.4
#    FIXFORM DISC_MONEY_INTERST/Z6.2
#    FIXFORM DISC_MONEY_INTRAST/Z6.2 DISC_MONEY_LOCAL/Z6.2 X21 X2
#    FIXFORM RATE_ZN_IRIA/1 RATE_ZN_IAIA/1
#    IF LEVEL NE 'L' OR 'LU' GOTO TOP;
#    COMPUTE
#    USOC_CNT = USOC_CNT;
#    UCODE = ' ';
#    QUSOC = QUSOC;
#    USOC = USOC;
#     LOC_ADL_UAMT = IF VERSION_NBR GE 39
#                    THEN (EDIT(ALOC_UAMT) * .01) ELSE 0.00;
#    MATCH QUSOC USOC USOC_CNT        7/X (USOCSEG  SEGMENT)
#       ON NOMATCH REJECT
#       ON MATCH COMPUTE
    
    if Level not in ('L','LU'):
         pass

    #FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X37
    #    FIXFORM IRPCT/Z6.3 IRRATE/Z11.4 IRMRC/Z11.2 IAPCT/Z6.3
    #    FIXFORM IARATE/Z11.4 IAMRC/Z11.2 MBIPFIX/A3 UFAC_CHG/A1 RATE_ZN_IR/1
    #    FIXFORM USOC_RATE_SZN_IND_INT/1 X16
    #    FIXFORM RAT_FCTR/Z8.7 USOC_PFBAND2/3 USOC_PFIIRIA/1 USOC_PFIIR/1
    #    FIXFORM USOC_PFIIA/1 USOC_PFIIAIA/1 USOC_PFBAND1/3 RATE_ZN_IA/1
    #    FIXFORM X16 LORATE/Z11.4 LOMRC/Z11.2 USO_O_LOC_PCT/3 X9
    #    FIXFORM ACC_TYPE/1
    #    FIXFORM LOPCT/Z6.3 RATE_ZN_LOC/1 MBIPVAR/3 X6
    elif record_id == '152000':         
         csr_usoc_records[usocKeys[current_usoc_key_concat]]
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['UCODE']='  '  #spaces   
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRPCT']=convertnumber(line[67:73] ,3)
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRRATE']=convertnumber(line[73:84],4)
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRMRC']=convertnumber(line[84:95],2)
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAPCT']=convertnumber(line[95:101],3)
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['IARATE']=convertnumber(line[101:112],4)
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAMRC']=convertnumber(line[112:123],2) 
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['MBIPFIX']=line[123:126]         
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['UFAC_CHG']=line[126:127]         
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_ZN_IR']=line[127:128]             
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_RATE_SZN_IND_INT']=line[128:129]              
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['RAT_FCTR']=convertnumber(line[145:153],7)              
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFBAND2']=line[153:156]
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFIIRIA']=line[156:157]
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFIIR']=line[157:158]
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFIIA']=line[158:159]
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFIIAIA']=line[159:160] 
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFBAND1']=line[160:163]     
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_ZN_IA']=line[163:164]                  
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['LORATE']=convertnumber(line[180:191],4)   
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['LOMRC']=convertnumber(line[191:202],2)
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['USO_O_LOC_PCT']=convertnumber(line[202:205],0)
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['ACC_TYPE']=line[214:215]  
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['LOPCT']=convertnumber(line[215:221],3)
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_ZN_LOC']=line[221:222]   
         csr_usoc_records[usocKeys[current_usoc_key_concat]]['MBIPVAR']=line[222:225]
         
         #writelog("update csr_usoc",output_log)  

    elif record_id =='152001' and prev_record_id == '152000':
#                            [61:67]       [67:78]    
#    FIXFORM ON BCTFCSRI X61 IRIA_PCT/Z6.3 IRIARATE/Z11.4
#            [78:89]       [89:95]       [95,106]       [106:117] 
#    FIXFORM IRIAMRC/Z11.2 IAIA_PCT/Z6.3 IAIARATE/Z11.4 IAIAMRC/Z11.2
#           [117:145] [145:150]         [150:153] [153:156]   [156:167]
#    FIXFORM X28      DSCT_FAC_LOC/Z5.4 X3        RATE_BAND/3 ALOC_UAMT/11   used in calc::::ALOC_UAMT/11)
#            [167:170]      [170:171] [171:176]         [176:182]
#    FIXFORM PCT_ORIG_USG/3 X1        SPDF_INTERST/Z5.4 SPDF_INTRAST/Z5.4
#            [182:186]   
#    FIXFORM DISC_MONEY_INTERST/Z6.2
#            [186:192]               [192:198]             [198:221]
#    FIXFORM DISC_MONEY_INTRAST/Z6.2 DISC_MONEY_LOCAL/Z6.2 X21 X2
#            [221:222]      [222:223]
#    FIXFORM RATE_ZN_IRIA/1 RATE_ZN_IAIA/1
    
#    IF LEVEL NE 'L' OR 'LU' GOTO TOP;
#    COMPUTE
#    USOC_CNT = USOC_CNT;
#    UCODE = ' ';
#    QUSOC = QUSOC;
#    USOC = USOC;
#     LOC_ADL_UAMT = IF VERSION_NBR GE 39
#                    THEN (EDIT(ALOC_UAMT) * .01) ELSE 0.00
        "152001"
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRIA_PCT']=convertnumber(line[61:67],3) 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRIARATE']=convertnumber(line[67:78],4) 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRIAMRC']=convertnumber(line[78:89],2)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAIA_PCT']=convertnumber(line[89:95],3) 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAIARATE']=convertnumber(line[95:106],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAIAMRC']=convertnumber(line[106:117],2)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['DSCT_FAC_LOC']=convertnumber(line[145:150],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_BAND']=line[153:156]
       
        #        Many of the below do not match
        #        Many of the below do not match
        aloc_uamt=line[156:167]      
        if aloc_uamt.rstrip(' ') =='':
            csr_usoc_records[usocKeys[current_usoc_key_concat]]['LOC_ADL_UAMT']='0.00'
        else:
            x=convertnumber(aloc_uamt,2)
            if str(x) =='0.00':
                csr_usoc_records[usocKeys[current_usoc_key_concat]]['LOC_ADL_UAMT']='0.00'
            else:
                y=float(x)*.01
                csr_usoc_records[usocKeys[current_usoc_key_concat]]['LOC_ADL_UAMT']=convertnumber(y,2)
 
            
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['PCT_ORIG_USG']=convertnumber(line[167:170],0) 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['SPDF_INTERST']=convertnumber(line[171:176],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['SPDF_INTRAST']=convertnumber(line[176:181],4) 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['DISC_MONEY_INTERST']=convertnumber(line[181:187],2)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['DISC_MONEY_INTRAST']=convertnumber(line[187:193],2) 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['DISC_MONEY_LOCAL']=convertnumber(line[193:200],2)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_ZN_IRIA']=line[222:223]      
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_ZN_IAIA']=line[223:224]   
        #csr_usoc_records[usocKeys[current_usoc_key_concat]]['INPUT_RECORDS']+=","+str(record_id)

        #writelog("insert csr_usoc",output_log)
    else:
#        error 01 record should always follow
        writelog("ERROR: a 152001 record should always follow a 152000.  Current record " \
        "is "+str(record_id) + " Previous record is "+str(prev_record_id),output_log)
    
def  process_TYP1521():
    "152100"
    global x_tbl
    global output_log
    global current_abbd_rec_key   
    global Level, record_id
    global usoccnt,usoc_cnt,ucode
    global a_pcfpct
    global usocKeys
    global current_usoc_key_concat
    global csr_usoc_records
 

    if Level not in ('L','LU'):
        pass
    elif record_id == '152100':                     #[61:67]
        
#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 QUSOC/Z6
#             [67:73]    [73:84]      [84:95]          [95:106]  
#    FIXFORM IRPCT/Z6.3 IRRATE/Z11.4 IR_U_ML_RT/Z11.4 IRMRC/Z11.2
#             [106:112]  [112:123] 
#    FIXFORM IAPCT/Z6.3 IARATE/Z11.4
#             [123:134]        [134:145]   [145:148]  [148:151]
#    FIXFORM IA_U_ML_RT/Z11.4 IAMRC/Z11.2 MBIPFIX/A3 MBIPVAR/A3
#             [151:152]   [152:153]    [153:154]  [154:155]    [155:157] 
#    FIXFORM UFAC_CHG/A1 RATE_ZN_IR/1 ACC_TYPE/1 RATE_ZN_IA/1 X2
#             [157:159] [159:162]   [162:169] [169:170]               [170:173] 
#    FIXFORM X2        PCFPCT_IA/3 X4 X3     USOC_RATE_SZN_IND_INT/1 X3
#             [173:181]     [181:203] [203:206]      [206:207]
#    FIXFORM RAT_FCTR/Z8.7 X22       USOC_PFBAND1/3 USOC_PFIIR/1
#             [207:208]    [208:209]      [209:210]      [210:213] 
#    FIXFORM USOC_PFIIA/1 USOC_PFIIAIA/1 USOC_PFIIRIA/1 USOC_PFBAND2/3
#             [213:216]  [216:219]
#    FIXFORM X3         A_PCFPCT/3        
            
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['QUSOC']=convertnumber(line[61:67],0)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRPCT']=convertnumber(line[67:73],3)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRRATE']=convertnumber(line[73:84],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IR_U_ML_RT']=convertnumber(line[84:95],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRMRC']=convertnumber(line[95:106],2)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAPCT']=convertnumber(line[106:112],3)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IARATE']=convertnumber(line[112:123],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IA_U_ML_RT']=convertnumber(line[123:134],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAMRC']=convertnumber(line[134:145],2)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['MBIPFIX']=line[145:148]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['MBIPVAR']=line[148:151]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['UCODE']='40'
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['UFAC_CHG']=line[151:152] 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_ZN_IR']=line[152:153]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['ACC_TYPE']=line[153:154]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_ZN_IA']=line[154:155]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['RAT_FCTR']=convertnumber(line[173:181],7)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFBAND1']=line[203:206]                    
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFIIR']=line[206:207]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFIIA']=line[207:208]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFIIAIA']=line[208:209]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFIIRIA']=line[209:210]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['USOC_PFBAND2']=line[210:213]
        a_pcfpct=line[216:219]  #used in 1521001 record
#        CSR_USOC_tbl['INPUT_RECORDS']+=","+str(record_id)
###        process_insert_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT,con,output_log)
        #writelog("update csr_usoc 152100",output_log)  
        
    elif record_id =='152101' and prev_record_id == '152100':
        "152101"       #     [61:67]       [67:78]        [78:89] 
#    FIXFORM ON BCTFCSRI X61 IRIA_PCT/Z6.3 IRIARATE/Z11.4 IRIA_U_ML_RT/Z11.4
#             [89:100]      [100:106]     [106:117]      [117:128] 
#    FIXFORM IRIAMRC/Z11.2 IAIA_PCT/Z6.3 IAIARATE/Z11.4 IAIA_U_ML_RT/Z11.4
#             [128:139]
#    FIXFORM IAIAMRC/Z11.2
#             [139:145]  [145:156]    [156:167]
#    FIXFORM LOPCT/Z6.3 LORATE/Z11.4 LO_U_ML_RT/Z11.4
#             [167:178]   [178:179]      [179:180]      [180:181]
#    FIXFORM LOMRC/Z11.2 RATE_ZN_IRIA/1 RATE_ZN_IAIA/1 RATE_ZN_LOC/1
#             [181:186]
#    FIXFORM DSCT_FAC_LOC/Z5.4
#             [186:193] [193:199]     [199:207] [207:212]         [212:217]         [217:225]  
#    FIXFORM X7        UNCOMP_MILE/6 X8        SPDF_INTERST/Z5.4 SPDF_INTRAST/Z5.4 X8
             
 
#    PFLEX_REC = IF A_PCFPCT NE '999' THEN PFLEX_REC + 1 ELSE PFLEX_REC;
#    IF PFLEX_REC GE 1 PERFORM ADD_PFLEX;
#    COMPUTE
#    USOC_CNT = USOC_CNT;
#    USOC = USOC;
 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRIA_PCT']=convertnumber(line[61:67],3)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRIARATE']=convertnumber(line[67:78],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRIA_U_ML_RT']=convertnumber(line[78:89],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IRIAMRC']=convertnumber(line[89:100],2)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAIA_PCT']=convertnumber(line[100:106],3)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAIARATE']=convertnumber(line[106:117],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAIA_U_ML_RT']=convertnumber(line[117:128],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['IAIAMRC']=convertnumber(line[128:139],2)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['LOPCT']=convertnumber(line[139:145],3)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['LORATE']=convertnumber(line[145:156],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['LO_U_ML_RT']=convertnumber(line[156:167],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['LOMRC']=convertnumber(line[167:178],2)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_ZN_IRIA']=line[178:179] 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_ZN_IAIA']=line[179:180] 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['RATE_ZN_LOC']=line[180:181]
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['DSCT_FAC_LOC']=convertnumber(line[181:186],4)
#        CSR_USOC_tbl['RATE_BAND']=line[x:x] ???????????????????????????????
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['SPDF_INTERST']=convertnumber(line[207:212],4)
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['SPDF_INTRAST']=convertnumber(line[212:217],4)
 
        csr_usoc_records[usocKeys[current_usoc_key_concat]]['AFLEX_PC_PCT']=a_pcfpct  #comes from 1521000 record
#        CSR_USOC_tbl['INPUT_RECORDS']+=","+str(record_id)

        #writelog("update csr_usoc 152101",output_log)  
    else:
#        error 01 record should always follow
        writelog("ERROR: a 152001 record should always follow a 152000.  Current record " \
        "is "+str(record_id) + " Previous record is "+str(prev_record_id),output_log)
    
#      
##
##INITIALIZATION PARAGRAPHS
def initialize_tbl(tbl):
    global current_abbd_rec_key
       
    if  tbl == 'CSR_BCCBSPL_tbl':
        CSR_BCCBSPL_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_BCCBSPL_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_BCCBSPL_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_BCCBSPL_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_BCCBSPL_tbl[key]=''        
    elif tbl == 'CSR_ACTLREC_tbl':
        for key,value in CSR_ACTLREC_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_ACTLREC_tbl[key]='' 
    elif tbl == 'CSR_CKT_tbl':
        for key,value in CSR_CKT_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_CKT_tbl[key]=''   
    elif tbl == 'CSR_LOC_tbl':
        for key,value in CSR_LOC_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_LOC_tbl[key]='' 
    elif tbl == 'CSR_LFID_tbl':
        for key,value in CSR_LFID_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_LFID_tbl[key]='' 
    elif tbl == 'CSR_COSFID_tbl':
        for key,value in CSR_COSFID_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_COSFID_tbl[key]='' 
    elif tbl == 'CSR_UFID_tbl':
        for key,value in CSR_UFID_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_UFID_tbl[key]=''
    elif tbl == 'CSR_USOC_tbl':
        for key,value in CSR_USOC_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_USOC_tbl[key]=''
    elif tbl == 'CSR_CUFID_tbl':
        for key,value in CSR_CUFID_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_CUFID_tbl[key]=''
    elif tbl == 'CSR_CFID_tbl':
        for key,value in CSR_CFID_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_CFID_tbl[key]=''
    elif tbl == 'CSR_BILLREC_tbl':
        for key,value in CSR_BILLREC_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_BILLREC_tbl[key]=''                
    else:
        process_ERROR_END("ERROR: No initialization code for "+tbl+" in the initialize_tbl method.")

    
def initialize_sequences():
    global csr_actlrec_sq
    global csr_bccbspl_sq
    global csr_billrec_sq
    global csr_cfid_sq
    global csr_ckt_sq
    global csr_cktusoc_sq
    global csr_cosfid_sq
    global csr_cufid_sq
    global csr_lfid_sq
    global csr_loc_sq
    global csr_ufid_sq
    global csr_usoc_sq
    csr_actlrec_sq = get_new_sequence("SQ_CSR_ACTLREC") - 10000000    
    csr_bccbspl_sq = get_new_sequence("SQ_CSR_BCCBSPL") - 10000000    
    csr_billrec_sq = get_new_sequence("SQ_CSR_BILLREC") - 10000000
    csr_cfid_sq = get_new_sequence("SQ_CSR_CFID") - 10000000
    csr_ckt_sq = get_new_sequence("SQ_CSR_CKT") - 10000000
    csr_cktusoc_sq = get_new_sequence("SQ_CSR_CKTUSOC") - 10000000
    csr_cosfid_sq = get_new_sequence("SQ_CSR_COSFID") - 10000000    
    csr_cufid_sq = get_new_sequence("SQ_CSR_CUFID") - 10000000
    csr_lfid_sq = get_new_sequence("SQ_CSR_LFID") - 10000000
    csr_loc_sq = get_new_sequence("SQ_CSR_LOC") - 10000000
    csr_ufid_sq = get_new_sequence("SQ_CSR_UFID") - 10000000
    csr_usoc_sq = get_new_sequence("SQ_CSR_USOC") - 10000000

    writelog("Sequences Set", output_log)
   
####END INITIALIZE PROCEDURES
    
def get_new_sequence(sequence_name):    
    selCur = con.cursor()
    sqlStmt="SELECT " + sequence_name +".nextval FROM dual"
    selCur.execute(sqlStmt)
    seqId = selCur.fetchone()[0]        
    selCur.close()
    return seqId
    
#def update_sequence(sequence_name, increment_value):
#      #alter sequence seq increment by (value);
#      #select seq.nextval from dual;
#      #alter sequence seq increment by (1);
#    if increment_value > 0:              
#        selCur = con.cursor()
#        sqlStmt="alter sequence " + sequence_name +" increment by " + str(increment_value)
#        selCur.execute(sqlStmt)
#        con.commit()
#        sqlStmt="SELECT " + sequence_name +".nextval FROM dual"
#        selCur.execute(sqlStmt)
#        con.commit()
#        sqlStmt="alter sequence " + sequence_name +" increment by 1"
#        selCur.execute(sqlStmt)
#        con.commit()
#        selCur.close()
#        writelog("Sequence " + sequence_name + " Updated", output_log)
#    else:
#        writelog("No records to update Sequence" + sequence_name, output_log)
    
def count_record(currec,unknownRec):
    global record_counts
    global unknown_record_counts

    if unknownRec:
        if str(currec).rstrip(' ') in unknown_record_counts:            
            unknown_record_counts[str(currec).rstrip(' ')]+=1
        else:
            unknown_record_counts[str(currec).rstrip(' ')]=1
    else:
        if str(currec).rstrip(' ') in record_counts:
            record_counts[str(currec).rstrip(' ')]+=1
        else:
            record_counts[str(currec).rstrip(' ')]=1 
    
 
def process_write_program_stats():
    global record_counts
    global unknown_record_counts
    global CSR_KEY_cnt
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
    writelog( "There are "+str(len(unknown_record_counts))+" different record ID types not processed: ",output_log)    
    writelog("**UNKNOWN record IDs and counts**",output_log)
    writelog("record_id, count",output_log)

    keylist = unknown_record_counts.keys()
    keylist.sort()
    for key in keylist:
#        print "%s: %s" % (key, record_counts[key])   
        writelog(str(key)+", "+str(unknown_record_counts[key]),output_log)   
        unkCnt+=unknown_record_counts[key]  
    
    
#    for key, value in unknown_record_counts.items():
#        writelog(str(key)+", "+str(value))
#        unkCnt+=value
    writelog("\n  Total count: "+str(unkCnt),output_log)    
    writelog("**",output_log)    
    
    writelog("TOTAL ACNA/EOB_DATE/BAN's processed:"+str(CSR_KEY_cnt),output_log)
    writelog(" ",output_log)
    writelog("Total input records read from input file:"+str(idCnt+unkCnt),output_log)
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
    global csr_input,  output_log
    global output_log
    
#    if DEBUGISON:
#        DEBUG_LOG.close()
        
    csr_input.close() 
    output_log.close()
   
    
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
    global csr_bccbspl_records, csr_actlrec_records, csr_billrec_records, csr_ckt_records, csr_loc_records, csr_cfid_records
    global csr_cosfid_records, csr_cufid_records, csr_ufid_records, csr_lfid_records, csr_usoc_records, csr_cktusoc_records
    global CSR_BCCBSPL_DEFN_DICT, CSR_ACTLREC_DEFN_DICT, CSR_BILLREC_DEFN_DICT, CSR_CKT_DEFN_DICT
    global TMP_TEST_EXMANY_DEFN_DICT
    global CSR_LOC_DEFN_DICT, CSR_CFID_DEFN_DICT, CSR_COSFID_DEFN_DICT, CSR_CUFID_DEFN_DICT
    global CSR_UFID_DEFN_DICT, CSR_LFID_DEFN_DICT, CSR_USOC_DEFN_DICT
    global con
    global output_log
    global schema
    global bccbspl_insert_cnt, actlrec_insert_cnt, billrec_insert_cnt, ckt_insert_cnt, loc_insert_cnt, cfid_insert_cnt
    global cosfid_insert_cnt, cufid_insert_cnt, ufid_insert_cnt, lfid_insert_cnt, usoc_insert_cnt, cktusoc_insert_cnt
    global cntr, rows, insql, results, inscnt
    
    results={}
    selCur = con.cursor()

    cntr=0
    rows=[]
    insql=""
    inscnt=0
    
    if len(csr_bccbspl_records) > 0:
        for key, value in csr_bccbspl_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table("CAIMS_CSR_BCCBSPL", value, CSR_BCCBSPL_DEFN_DICT,con,schema, 'SQ_CSR_BCCBSPL',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()

    if len(csr_actlrec_records) > 0:
        for key, value in csr_actlrec_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table("CAIMS_CSR_ACTLREC", value, CSR_ACTLREC_DEFN_DICT,con,schema, 'SQ_CSR_ACTLREC',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()        
    
    if len(csr_billrec_records) > 0:  
        for key, value in csr_billrec_records.items():       
            tmpResult=process_insert_many_table("CAIMS_CSR_BILLREC", value, CSR_BILLREC_DEFN_DICT,con,schema, 'SQ_CSR_BILLREC',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
        
    if len(csr_ckt_records) > 0:
        for key, value in csr_ckt_records.items():
            tmpResult=process_insert_many_table("CAIMS_CSR_CKT", value, CSR_CKT_DEFN_DICT,con,schema, 'SQ_CSR_CKT',output_log)                
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
    
    if len(csr_loc_records) > 0:    
        for key, value in csr_loc_records.items():
            tmpResult=process_insert_many_table("CAIMS_CSR_LOC", value, CSR_LOC_DEFN_DICT,con,schema, 'SQ_CSR_LOC',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
   
    if len(csr_cfid_records) > 0:     
        for key, value in csr_cfid_records.items():
            tmpResult=process_insert_many_table("CAIMS_CSR_CFID", value, CSR_CFID_DEFN_DICT,con,schema, 'SQ_CSR_CFID',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
    
    if len(csr_cosfid_records) > 0:
        for key, value in csr_cosfid_records.items():
            tmpResult=process_insert_many_table("CAIMS_CSR_COSFID", value, CSR_COSFID_DEFN_DICT,con,schema, 'SQ_CSR_COSFID',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
        
    if len(csr_cufid_records) > 0:        
        for key, value in csr_cufid_records.items():
            tmpResult=process_insert_many_table("CAIMS_CSR_CUFID", value, CSR_CUFID_DEFN_DICT,con,schema, 'SQ_CSR_CUFID',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
    
    if len(csr_ufid_records) > 0:
        for key, value in csr_ufid_records.items():
            tmpResult=process_insert_many_table("CAIMS_CSR_UFID", value, CSR_UFID_DEFN_DICT,con,schema, 'SQ_CSR_UFID',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()

    if len(csr_lfid_records) > 0:
        for key, value in csr_lfid_records.items():
            tmpResult=process_insert_many_table("CAIMS_CSR_LFID", value, CSR_LFID_DEFN_DICT,con,schema, 'SQ_CSR_LFID',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()

    if len(csr_usoc_records) > 0:           
        for key, value in csr_usoc_records.items():
            tmpResult=process_insert_many_table("CAIMS_CSR_USOC", value, CSR_USOC_DEFN_DICT,con,schema, 'SQ_CSR_USOC',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
     
    if len(csr_cktusoc_records) > 0:       
        for key, value in csr_cktusoc_records.items():
            tmpResult=process_insert_many_table("CAIMS_CSR_CKTUSOC", value, CSR_CKTUSOC_DEFN_DICT,con,schema, 'SQ_CSR_CKTUSOC',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()        

    selCur.close()
    
    bccbspl_insert_cnt+=len(csr_bccbspl_records)
    actlrec_insert_cnt+=len(csr_actlrec_records)
    billrec_insert_cnt+=len(csr_billrec_records)
    ckt_insert_cnt+=len(csr_ckt_records)
    loc_insert_cnt+=len(csr_loc_records)
    cfid_insert_cnt+=len(csr_cfid_records)
    cosfid_insert_cnt+=len(csr_cosfid_records)
    cufid_insert_cnt+=len(csr_cufid_records)
    ufid_insert_cnt+=len(csr_ufid_records)
    lfid_insert_cnt+=len(csr_lfid_records)
    usoc_insert_cnt+=len(csr_usoc_records)
    cktusoc_insert_cnt+=len(csr_cktusoc_records)
    
    csr_bccbspl_records={}
    csr_actlrec_records={}
    csr_billrec_records={}
    csr_ckt_records={}
    csr_loc_records={}
    csr_cfid_records={}
    csr_cosfid_records={}
    csr_cufid_records={}
    csr_ufid_records={}
    csr_lfid_records={}
    csr_usoc_records={}
    csr_cktusoc_records={}
    
    #writelog("Records Inserted", output_log)

#def update_sequences():
#    global bccbspl_insert_cnt, ckt_insert_cnt, actlrec_insert_cnt, billrec_insert_cnt
#    global loc_insert_cnt, cfid_insert_cnt, cufid_insert_cnt, cosfid_insert_cnt
#    global lfid_insert_cnt, ufid_insert_cnt
#
#    update_sequence("SQ_CSR_BCCBSPL", bccbspl_insert_cnt)
#    update_sequence("SQ_CSR_CKT", ckt_insert_cnt)
#    update_sequence("SQ_CSR_ACTLREC", actlrec_insert_cnt)    
#    update_sequence("SQ_CSR_BILLREC", billrec_insert_cnt)
#    update_sequence("SQ_CSR_LOC", loc_insert_cnt)
#    update_sequence("SQ_CSR_CFID", cfid_insert_cnt)
#    update_sequence("SQ_CSR_CUFID", cufid_insert_cnt)
#    update_sequence("SQ_CSR_COSFID", cosfid_insert_cnt)
#    update_sequence("SQ_CSR_LFID", lfid_insert_cnt)
#    update_sequence("SQ_CSR_UFID", ufid_insert_cnt)    
#    update_sequence("SQ_CSR_USOC", csr_usoc_sq)
#    update_sequence("SQ_CSR_CKTUSOC", csr_cktusoc_sq)

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


"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
"BEGIN Program logic"
"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


print "Starting Program"
init()
#try:
main()
#except Exception as e:
#    process_ERROR_END(e.message)
#else:
endProg("-END OF PROGRAM-")
