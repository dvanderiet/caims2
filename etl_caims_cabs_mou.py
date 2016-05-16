# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
PROGRAM NAME:     etl_caims_cabs_bdt.py                                  
LOCATION:                      
PROGRAMMER(S):    Jason Yelverton                                
DESCRIPTION:      CAIMS Extract/Transformation/Load program for Bill Data Tape
                  (BDT) records.
                  
REPLACES:         Legacy CTL program BC100FT0 - LOAD BCCBMOU FOCUS DATABASE.
                                                                         
LANGUAGE/VERSION: Python/2.7.10                                          
INITIATION DATE:  x/x/2016                                                
INPUT FILE(S):    PBCL.CY.XRU0102O.CABS.G0358V00.txt (build from GDG)
LOCATION:         MARION MAINFRAME           
                                                                         
OUTPUT:           ORACLE DWBS001P                                
                  Table names CAIMS_MOU_*
                                                                         
EXTERNAL CALLS:                                         
                                                                         
LOCATION:         pansco-pdm: /opt/IntrNet/p271app/rpt271/macros         
                                                                         
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
import platform
import os
import copy

###IMPORT COMMON/SHARED UTILITIES
#
from etl_caims_cabs_utility import  process_insert_many_table, translate_TACCNT_FGRP, writelog, \
                                    createTableTypeDict,setDictFields,convertnumber, invalid_acna_chars, db_record_exists


settings = ConfigParser.ConfigParser();
settings.read('settings.ini')
settings.sections()

"SET ORACLE CONNECTION"
con=cx_Oracle.connect(settings.get('OracleSettings','OraCAIMSUser'),settings.get('OracleSettings','OraCAIMSPw'),settings.get('OracleSettings','OraCAIMSSvc'))    
schema=settings.get('OracleSettings','OraCAIMSUser')
"CONSTANTS"

if str(platform.system()) == 'Windows': 
    output_dir = settings.get('GlobalSettings','WINDOWS_LOG_DIR');
else:
    output_dir = settings.get('GlobalSettings','LINUX_LOG_DIR');
    

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
    inputPath=settings.get('BDTSettings','WINDOWS_BDT_inDir') 
else:
    inputPath=settings.get('BDTSettings','LINUX_BDT_inDir') 

IP_FILENM_AND_PATH =os.path.join(inputPath,fileNm)

if os.path.isfile(IP_FILENM_AND_PATH):
    print ("Input file:"+IP_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+IP_FILENM_AND_PATH)


global mou_version
#mou_version = 'G0354'

"GLOBAL VARIABLES"    
global statpg_rec
global line
global dtl_rec
global current_parent_rec
global parent_1010_rec
global parent_3505_rec
global parent_3510_rec
global parent_3515_rec
global parent_3520_rec
global parent_3527_rec
global parent_3905_rec
global parent_3910_rec
global parent_3920_rec
global parent_3927_rec

global dup_ban_lock
global invalid_fgrp_lock
global badKey

global curr_3520_RE_part1
global curr_3520_RE_part2

global bctfmou_cnt
global dtlrec_cnt
global statpg_cnt

bctfmou_cnt=0
dtlrec_cnt=0
statpg_cnt=0

statpg_rec=False
dtl_rec=False
parent_1010_rec=False
parent_3505_rec=False
parent_3510_rec=False
parent_3515_rec=False
parent_3520_rec=False
parent_3527_rec=False
parent_3905_rec=False
parent_3910_rec=False
parent_3920_rec=False
parent_3927_rec=False

dup_ban_lock=False
invalid_fgrp_lock=False
 
record_id=''
badKey=False
current_abbd_rec_key={'ACNA':'x','EOB_DATE':'1','BAN':'x'}
prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}   #EOB_DATE,ACNA,BAN key
current_parent_rec=''
current_clli=''
current_TACCT_FGRP=''
current_ZCIC=''
curr_3520_RE_part1=''
curr_3520_RE_part2=''
#Create lists for each unique record
abbdLst=[]              #acna ban billdate 
abbdcLst=[]             #acna ban billdate clli
abbdcrecicjdLst=[]      #acna ban billdate clli re cic jursdn_cd (3505,3510,3515,3520 record types RATEREC)
abbdcrnLst=[]           #acna ban billdate clli record_num (3527 record types STATPG)

mou_bctfmou_records={}
mou_dtlrec_records={}
mou_statpg_records={}

results={}
exmanyCnt=0
 
record_counts={}
unknown_record_counts={}
BILL_REC='10' 
STAR_LINE='*********************************************************************************************'  

NEGATIVE_NUMS=['}','J','K','L','M','N','O','P','Q','R']
VALID_FGRP=['1','2','3','4','J','U']
VALID_RECORD_TYPES={'010100':'PARENT', '050500':'CHILD', '070500':'CHILD',                      \
                    '350500':'PARENT', '350501':'CHILD',                                        \
                    '351000':'PARENT',                                                          \
                    '351500':'PARENT',                                                          \
                    '352000':'PARENT', '352001':'CHILD', '352002':'CHILD',                      \
                    '352700':'PARENT', '352701':'CHILD', '352702':'CHILD', '352750':'CHILD',    \
                    '390500':'PARENT',                                                          \
                    '391000':'PARENT',                                                          \
                    '392000':'PARENT',                                                          \
                    '392700':'PARENT', '392701':'CHILD'}
   
"TRANSLATERS"


def init():
    global bdt_input 
    global output_log
    global record_id
    "OPEN FILES"
    "   CABS INPUT FILE"
    bdt_input = open(IP_FILENM_AND_PATH, "r");
    
    "PROCESS HEADER LINE"
    "  -want to get bill cycle info for output log file "
    "  -this will make the log file names more sensical " 
#    READ HEADER LINE    
    headerLine=bdt_input.readline()
    record_id=headerLine[:6]
    cycl_yy=headerLine[6:8]
    cycl_mmdd=headerLine[8:12]
    cycl_time=headerLine[12:21].replace('\n','')

    "  CREATE LOG FILE WITH CYCLE DATE FROM HEADER AND RUN TIME OF THIS JOB"
    log_file=os.path.join(output_dir,"BDT_Cycle"+str(cycl_yy)+str(cycl_mmdd)+str(cycl_time)+"_"+startTM.strftime("%Y%m%d_@%H%M") +".txt")
         
    output_log = open(log_file, "w");
    output_log.write("-BDT CAIMS ETL PROCESS-")
    
    if record_id not in settings.get('BDTSettings','BDTHDR'):
        process_ERROR_END("The first record in the input file was not a "+settings.get('BDTSettings','BDTHDR').rstrip(' ')+" record.")

    writelog("Process "+sys.argv[0], output_log)
    writelog("   started execution at: " + str(startTM), output_log)
    writelog(STAR_LINE, output_log)
    writelog(" ", output_log)
    writelog("Input file: "+str(bdt_input), output_log)
    writelog("Log file: "+str(output_log), output_log)
    
    "Write header record informatio only"
    writelog("Header Record Info:", output_log)
    writelog("     Record ID: "+record_id+" YY: "+cycl_yy+", MMDD: "+cycl_mmdd+", TIME: "+str(cycl_time), output_log)
    
    count_record(record_id,False)
    del headerLine,cycl_yy,cycl_mmdd,cycl_time
    


def main():    
    #BDT_config.initialize_BDT() 
    global record_type
    global line
    global record_counts
    global unknown_record_counts
    "Counters"

#    record_counts={'010100':0,'050500':0,'051000':0,'051200':0,'055000':0,'051300':0,'055100':0,'051301':0,'051400':0,'051500':0,'051600':0,'055200':0,'055300':0,'055400':0,'150500':0,'200500':0,'':0,'200501':0,'250500':0}
    
    global inputLineCnt
    global BDT_KEY_cnt

    "TABLE Dictionaries - for each segment"
    global MOU_BCTFMOU_tbl
    global MOU_BCTFMOU_DEFN_DICT
    global MOU_STATPG_tbl
    global MOU_STATPG_DEFN_DICT
    global MOU_DTLREC_tbl
    global MOU_DTLREC_DEFN_DICT
    
    
    "text files"
    global bdt_input
    global BDT_column_names
    global firstBillingRec    
    global record_id
    global prev_record_id
    global current_abbd_rec_key 
    global prev_abbd_rec_key   
    global goodKey
    global dup_ban_lock
    global invalid_fgrp_lock
    global current_parent_rec
    global output_log
    global mou_bctfmou_records, mou_dtlrec_records, mou_statpg_records
    
    firstBillingRec=True
#DECLARE TABLE ARRAYS AND DICTIONARIES
    MOU_BCTFMOU_DEFN_DICT=createTableTypeDict('CAIMS_MOU_BCTFMOU',con,schema,output_log)
    MOU_BCTFMOU_tbl=setDictFields('CAIMS_MOU_BCTFMOU', MOU_BCTFMOU_DEFN_DICT) 
    
    MOU_STATPG_DEFN_DICT=createTableTypeDict('CAIMS_MOU_STATPG',con,schema,output_log)
    MOU_STATPG_tbl=setDictFields('CAIMS_MOU_STATPG', MOU_STATPG_DEFN_DICT) 
    
    MOU_DTLREC_DEFN_DICT=createTableTypeDict('CAIMS_MOU_DTLREC',con,schema,output_log)
    MOU_DTLREC_tbl=setDictFields('CAIMS_MOU_DTLREC', MOU_DTLREC_DEFN_DICT) 

    "COUNTERS"
    inputLineCnt=1 #header record was read in init()
    BDT_KEY_cnt=0
    status_cnt=0
    exmanyCnt=0
    "KEY"
    
    prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}
    
    "LOOP THROUGH INPUT CABS TEXT FILE"
    for line in bdt_input:    
        
        "Count each line"
        inputLineCnt += 1 #Count each line
        status_cnt+=1
        if status_cnt>999:
            print str(inputLineCnt)+" lines completed processing...."+str(datetime.datetime.now())
            status_cnt=0               
        
        if len(line) > 231:
            record_id=line[225:231]
        else:
            record_id=line[:6]

        #if inputLineCnt > 5000:
        #    break
        "Process by record_id"    
        "Header rec (first rec) processed in init()"
        #If the new record is a valid parent we need to insert record of previos parent
        if record_id in VALID_RECORD_TYPES.keys() and VALID_RECORD_TYPES[record_id] == "PARENT" and dup_ban_lock == False:
            process_inserts(record_id)
             
        if line[:2] == BILL_REC:
            "START-MAIN PROCESS LOOP"
            "GET KEY OF NEXT RECORD"
            current_abbd_rec_key=process_getkey()
            
            if record_id not in VALID_RECORD_TYPES:
                count_record(record_id,True)
            elif dup_ban_lock and record_id != '010100':
                count_record("DUP_BAN",True)
            elif invalid_fgrp_lock  and record_id != '010100':
                count_record("BAD_FGRP",True)
            elif badKey:
                count_record("BAD_ABD_KEY",True)
                current_parent_rec = ''
                writelog("WARNING: BAD INPUT DATA.  ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'], output_log)
            else:
                if current_abbd_rec_key != prev_abbd_rec_key:
                    BDT_KEY_cnt+=1                    
                    exmanyCnt+=1
                    if exmanyCnt >= 5:
                        process_insertmany()
                        
                        exmanyCnt=0
                        mou_bctfmou_records={}
                        mou_dtlrec_records={}
                        mou_statpg_records={}
                    
                process_bill_records()
   
            "set previous key for comparison in next iteration"   
            prev_abbd_rec_key=current_abbd_rec_key
            
        elif record_id in settings.get('BDTSettings','BDTFTR'):
            process_inserts(record_id)
            "FOOTER RECORD"
            log_footer_rec_info()     
            
        else:
            "ERROR:Unidentified Record"
            writelog("ERROR: Not sure what type of record this is:", output_log)
            writelog(line, output_log)
         
         
        prev_record_id=record_id
        "END-MAIN PROCESS LOOP"    
    #END of For Loop
    process_insertmany()

def log_footer_rec_info():
    global record_id
    
    BAN_cnt=line[6:12]
    REC_cnt=line[13:22]
    writelog(" ", output_log)
    writelog("Footer Record Info:", output_log)
    writelog("     Record ID "+record_id+" BAN Count: "+BAN_cnt+" RECORD CNT: "+REC_cnt, output_log)
    writelog("The total number of lines counted in input file is : "+ str(inputLineCnt), output_log)
    writelog(" ", output_log)
    
    count_record(record_id,False)


def process_bill_records():
    global MOU_BCTFMOU_tbl
    global record_id
    global firstBillingRec    
    
    if firstBillingRec:
        "FIRST RECORD ONLY"
        #write BOS version number to log
        writelog("**--------------------------**", output_log)
        writelog("** BOS VERSION NUMBER IS "+line[82:84]+" ** ", output_log)
        writelog("**--------------------------**", output_log)
        firstBillingRec=False
        
#    rid0 = record_id[:2]
#    rid1 = record_id[2:4]
#    rid2 = record_id[4:6]
    unknownRecord=False
    if record_id == '010100':
        process_TYP010100_PARENT()
        set_flags(record_id)
# 
    elif record_id in ('050500','070500'):  #050500 and 070500 records update the 010100 record
        process_TYP010100_CHILD()
#        
    elif record_id == '350500':             
        process_TYP3505_PARENT()
        set_flags(record_id)
#
    elif record_id == '350501':        
        process_TYP3505_CHILD()
#
    elif record_id == '351000':
        process_TYP351000()
        set_flags(record_id)
#
    elif record_id == '351500':        
        process_TYP351500()
        set_flags(record_id)#
#
    elif record_id == '352000':        
        process_TYP3520_PARENT()
        set_flags(record_id)        
#
    elif record_id in ('352001','352002'):
        process_TYP3520_CHILD()        
#       
    elif record_id == '352700':  
        process_TYP3527_PARENT()
        set_flags(record_id)
#
    elif record_id in ('352701','352702','352750'):
        process_TYP3527_CHILD()
#
    elif record_id == '390500':             
        process_TYP390500()
        set_flags(record_id)
#
    elif record_id == '391000':
        process_TYP391000()
        set_flags(record_id)
#
    elif record_id == '392000':
        process_TYP392000()
        set_flags(record_id)
#  
    elif record_id == '392700':  
        process_TYP3927_PARENT()
        set_flags(record_id)
#
    elif record_id == '392701':
        process_TYP3927_CHILD()
#

#### 3525 Does nothing after BOS version 41 so don't process       
#    elif record_id[:4] == '3525':
#        #process_TYP352500() 
#        writelog(line[6:30]+"- 3525 - SUB_TOT_IND - "+line[96:97])        
####
    else:  #UNKNOWN RECORDS
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
     
     
def process_inserts(record_id):
    global MOU_BCTFMOU_tbl
    global MOU_BCTFMOU_DEFN_DICT
    global MOU_DTLREC_tbl
    global MOU_DTLREC_DEFN_DICT
    global MOU_STATPG_tbl
    global MOU_STATPG_DEFN_DICT    
    global current_parent_rec
    global bctfmou_cnt, dtlrec_cnt, statpg_cnt
    global mou_bctfmou_records, mou_dtlrec_records, mou_statpg_records
        
    if current_parent_rec=='010100':
        bctfmou_cnt+=1        
        mou_bctfmou_records[bctfmou_cnt]= copy.deepcopy(MOU_BCTFMOU_tbl)
        initialize_BCTFMOU_tbl()
        current_parent_rec=record_id
    
    elif current_parent_rec=='350500':
        dtlrec_cnt+=1        
        mou_dtlrec_records[dtlrec_cnt]= copy.deepcopy(MOU_DTLREC_tbl)
        initialize_DTLREC_tbl()
        current_parent_rec=record_id
    
    elif current_parent_rec=='351000':
        dtlrec_cnt+=1
        mou_dtlrec_records[dtlrec_cnt]= copy.deepcopy(MOU_DTLREC_tbl)
        initialize_DTLREC_tbl()
        current_parent_rec=record_id
    
    elif current_parent_rec=='351500':
        dtlrec_cnt+=1
        mou_dtlrec_records[dtlrec_cnt]= copy.deepcopy(MOU_DTLREC_tbl)
        initialize_DTLREC_tbl()
        current_parent_rec=record_id
    
    elif current_parent_rec=='352000':
        dtlrec_cnt+=1
        mou_dtlrec_records[dtlrec_cnt]= copy.deepcopy(MOU_DTLREC_tbl)
        initialize_DTLREC_tbl()
        current_parent_rec=record_id
        
    elif current_parent_rec=='352700':
        statpg_cnt+=1
        mou_statpg_records[statpg_cnt]= copy.deepcopy(MOU_STATPG_tbl)
        initialize_STATPG_tbl()
        current_parent_rec=record_id
        
    elif current_parent_rec=='390500':
        dtlrec_cnt+=1
        mou_dtlrec_records[dtlrec_cnt]= copy.deepcopy(MOU_DTLREC_tbl)        
        initialize_DTLREC_tbl()
        current_parent_rec=record_id
        
    elif current_parent_rec=='391000':
        dtlrec_cnt+=1
        mou_dtlrec_records[dtlrec_cnt]= copy.deepcopy(MOU_DTLREC_tbl)
        initialize_DTLREC_tbl()
        current_parent_rec=record_id
        
    elif current_parent_rec=='392000':
        dtlrec_cnt+=1
        mou_dtlrec_records[dtlrec_cnt]= copy.deepcopy(MOU_DTLREC_tbl)
        initialize_DTLREC_tbl()
        current_parent_rec=record_id
        
    elif current_parent_rec=='392700':
        statpg_cnt+=1
        mou_statpg_records[statpg_cnt]= copy.deepcopy(MOU_STATPG_tbl)
        initialize_STATPG_tbl()
        current_parent_rec=record_id
    else:
        current_parent_rec=record_id


def set_flags(current_record_type):
    global parent_1010_rec
    global parent_3505_rec
    global parent_3510_rec
    global parent_3515_rec
    global parent_3520_rec
    global parent_3527_rec
    global parent_3905_rec    
    global parent_3910_rec
    global parent_3920_rec
    global parent_3927_rec
    
    if current_record_type=='010100':        
        parent_3505_rec = False
        parent_3510_rec = False
        parent_3515_rec = False
        parent_3520_rec = False
        parent_3527_rec = False
        parent_3905_rec = False
        parent_3910_rec = False
        parent_3920_rec = False
        parent_3927_rec = False
    elif current_record_type=='350500':
        parent_1010_rec = False
        parent_3510_rec = False
        parent_3515_rec = False
        parent_3520_rec = False
        parent_3527_rec = False
        parent_3905_rec = False
        parent_3910_rec = False
        parent_3920_rec = False
        parent_3927_rec = False
    elif current_record_type=='351000':
        parent_1010_rec = False
        parent_3505_rec = False
        parent_3515_rec = False
        parent_3520_rec = False
        parent_3527_rec = False
        parent_3905_rec = False
        parent_3910_rec = False
        parent_3920_rec = False
        parent_3927_rec = False
    elif current_record_type=='351500':
        parent_1010_rec = False
        parent_3505_rec = False
        parent_3510_rec = False
        parent_3520_rec = False
        parent_3527_rec = False
        parent_3905_rec = False
        parent_3910_rec = False
        parent_3920_rec = False
        parent_3927_rec = False
    elif current_record_type=='352000':
        parent_1010_rec=False
        parent_3505_rec=False
        parent_3510_rec = False
        parent_3515_rec = False
        parent_3527_rec=False
        parent_3905_rec=False
        parent_3910_rec = False
        parent_3920_rec = False
        parent_3927_rec = False
    elif current_record_type=='352700':
        parent_1010_rec=False
        parent_3505_rec=False
        parent_3510_rec = False
        parent_3515_rec = False
        parent_3520_rec=False
        parent_3905_rec=False
        parent_3910_rec = False
        parent_3920_rec = False
        parent_3927_rec = False
    elif current_record_type=='390500':
        parent_1010_rec=False
        parent_3505_rec=False
        parent_3510_rec = False
        parent_3515_rec = False
        parent_3520_rec = False
        parent_3527_rec = False
        parent_3910_rec = False
        parent_3920_rec = False
        parent_3927_rec = False
    elif current_record_type=='391000':
        parent_1010_rec = False
        parent_3505_rec = False
        parent_3510_rec = False
        parent_3515_rec = False
        parent_3520_rec = False
        parent_3527_rec = False
        parent_3905_rec = False
        parent_3920_rec = False
        parent_3927_rec = False
    elif current_record_type=='392000':
        parent_1010_rec = False
        parent_3505_rec = False
        parent_3510_rec = False
        parent_3515_rec = False
        parent_3520_rec=False
        parent_3527_rec=False
        parent_3905_rec = False
        parent_3910_rec = False
        parent_3927_rec = False
    elif current_record_type=='392700':
        parent_1010_rec = False
        parent_3505_rec = False
        parent_3510_rec = False
        parent_3515_rec = False
        parent_3520_rec = False
        parent_3527_rec = False
        parent_3905_rec = False
        parent_3910_rec = False
        parent_3920_rec = False
    else:
        parent_1010_rec=False
        parent_3505_rec=False
        parent_3510_rec=False
        parent_3515_rec=False
        parent_3520_rec=False
        parent_3527_rec=False
        parent_3905_rec=False
        parent_3910_rec = False
        parent_3920_rec = False
        parent_3927_rec = False
    

def process_TYP010100_PARENT():
    global MOU_BCTFMOU_tbl
    global current_TACCT_FGRP
    global current_abbd_rec_key
    global current_ZCIC
    global dup_ban_lock
    global invalid_fgrp_lock
    global parent_1010_rec
    global line
    global current_parent_rec
    
    initialize_BCTFMOU_tbl()
    
    tacct=line[107:108]
    tfgrp=line[108:109]   
    current_TACCT_FGRP=translate_TACCNT_FGRP(tacct,tfgrp)
    
    if current_TACCT_FGRP not in VALID_FGRP: #checking for valid fgrp
        invalid_fgrp_lock=True
        writelog("WARNING: INVALID FGRP - ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'] + ", FGRP="+current_TACCT_FGRP, output_log)
        parent_1010_rec=False
        current_parent_rec=''
    elif current_abbd_rec_key in abbdLst or db_record_exists('CAIMS_MOU_BCTFMOU', current_abbd_rec_key['ACNA'],current_abbd_rec_key['BAN'],current_abbd_rec_key['EOB_DATE'], con, schema, output_log):
        dup_ban_lock=True
        writelog("WARNING: DUPLICATE BAN - ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'], output_log)
        parent_1010_rec=False
        current_parent_rec=''
    else:
        dup_ban_lock=False
        invalid_fgrp_lock=False
        abbdLst.append(current_abbd_rec_key)
        jdate=line[71:76]
    #    MOU_BCTFMOU_tbl['BANLOCK']='N'    #defaulted db value
        #MOU_BCTFMOU_tbl['VERSION_NBR']=line[82:84]    
        MOU_BCTFMOU_tbl['TLF']=line[97:98]
        MOU_BCTFMOU_tbl['NLATA']=line[98:101]
        MOU_BCTFMOU_tbl['HOLD_BILL']=line[105:106]
        #MOU_BCTFMOU_tbl['TACCNT']=tacct
        #MOU_BCTFMOU_tbl['TFGRP']=tfgrp  
        MOU_BCTFMOU_tbl['TACCNT_FGRP']=current_TACCT_FGRP
        MOU_BCTFMOU_tbl['BILL_DATE']=jdate
        #MOU_BCTFMOU_tbl['EOBDATEA']=MOU_BCTFMOU_tbl['EOB_DATE']
        MOU_BCTFMOU_tbl['EOBDATECC']=MOU_BCTFMOU_tbl['EOB_DATE']
        MOU_BCTFMOU_tbl['BILLDATECC']=MOU_BCTFMOU_tbl['BILL_DATE']
        MOU_BCTFMOU_tbl['CAIMS_REL']='B'
        MOU_BCTFMOU_tbl['MPB']=line[224:225]
        MOU_BCTFMOU_tbl['INPUT_RECORDS']=str(record_id)
        current_ZCIC=line[110:115]        
        
        parent_1010_rec=True
        current_parent_rec="010100"
        #read the 0505 record next       
 
    
def process_TYP010100_CHILD():    
    global MOU_BCTFMOU_tbl
    global record_id
    global line  

    if record_id=='050500':
        #    get bo_code
        MOU_BCTFMOU_tbl['ICSC_OFC']=line[211:214].rstrip(' ').lstrip(' ')
        MOU_BCTFMOU_tbl['INPUT_RECORDS']+="*"+str(record_id)
    elif record_id=='070500':        
        writelog("070500 RECORD", output_log)
    else:
        writelog("WARNING: INVALID 010100 CHILD - "+record_id, output_log)


def process_TYP3505_PARENT():
    global line
    global record_id
    global MOU_DTLREC_tbl    
    global curr_RE
    global parent_3505_rec
    global current_ZCIC
    global current_TACCT_FGRP
    global current_abbd_rec_key
    global current_parent_rec
    global abbdLst
    global abbdcLst
    global abbdcrecicjdLst
    
    curr_RE       = line[227:229] + line[118:120] + line[179:180] + line[123:124] + line[178:179] + line[180:181] + line[120:121] + line[221:222] + line[177:178] + ' ' +line[122:123] + line[207:208] + line[121:122] + line[176:177] + line[192:193]            
         
    if line[176:177] != '0':
        count_record("350500_IND12_SKIPPED",False)
        parent_3505_rec=False        
        current_parent_rec=''
    else:
        initialize_DTLREC_tbl()     
        MOU_DTLREC_tbl['CLLI']=line[61:72]
        MOU_DTLREC_tbl['RE']=curr_RE         
        current_record_num=line[108:118]
        if line[204:207]=='999':
            MOU_DTLREC_tbl['BIP']=' '
        else:
            MOU_DTLREC_tbl['BIP']=line[204:207]
            
        if line[182:185]=='999':
            MOU_DTLREC_tbl['SP_TRAN_PCT']=' '
        else:
            MOU_DTLREC_tbl['SP_TRAN_PCT']=line[182:185]

        if current_TACCT_FGRP == '1':
            MOU_DTLREC_tbl['CIC']=' '
            current_record_num=line[108:118]
        else:
            MOU_DTLREC_tbl['CIC']=current_ZCIC
            current_record_num=' '
        
        # RCDCC is a date field but the value is always defauled to ' ' because of BOS Version number
        # So just always leave RCDCC blank/null
        
        MOU_DTLREC_tbl['JRSDN_CD']=line[224:225]
        MOU_DTLREC_tbl['NAT_REG']='0'
        MOU_DTLREC_tbl['EO_TANDIND']=line[72:73]
        MOU_DTLREC_tbl['RATPT1']=line[73:84]
        MOU_DTLREC_tbl['RATPT1IND']=line[84:85]
        MOU_DTLREC_tbl['RATPT2']=line[85:96]
        MOU_DTLREC_tbl['RATPT2IND']=line[96:97]
        MOU_DTLREC_tbl['LTL_IDENTIF']=line[97:108]
        MOU_DTLREC_tbl['RECORD_NUM']=current_record_num
        MOU_DTLREC_tbl['AMILE']=line[124:127]
        MOU_DTLREC_tbl['QUANT']=convertnumber(line[127:138],0)
        MOU_DTLREC_tbl['RATE']=convertnumber(line[138:149],9)
        MOU_DTLREC_tbl['AMOUNT']=convertnumber(line[149:160],2)
        MOU_DTLREC_tbl['UBDFROMCC']=line[160:168]
        MOU_DTLREC_tbl['UBDTHRUCC']=line[168:176]
        #MOU_DTLREC_tbl['TSTP']=line[182:185] ##Set up top
        MOU_DTLREC_tbl['ST_ID']=line[186:188]
        MOU_DTLREC_tbl['ST_LV_CC']=line[188:192]
        #MOU_DTLREC_tbl['BIP']=line[204:207] ##Set up top
        MOU_DTLREC_tbl['PIU']=' '
        MOU_DTLREC_tbl['BAID_EO']=line[208:209]
        MOU_DTLREC_tbl['MPB_IND']=line[209:210]
        MOU_DTLREC_tbl['TANDM_CLLI']=line[210:221]
        MOU_DTLREC_tbl['JRSDN_CD']=line[224:225]
        MOU_DTLREC_tbl['INPUT_RECORDS']=str(record_id)
        MOU_DTLREC_tbl['QRCI']='5'
        parent_3505_rec=True
        current_parent_rec="350500"


def process_TYP3505_CHILD():
    global line
    global record_id
    global MOU_DTLREC_tbl
    global parent_3505_rec
    
    if parent_3505_rec != True:
        count_record("350501_NO_PARENT",False)
        writelog("Record - " + str(inputLineCnt) + " - 350501 - Has no 350500 parent", output_log)
    else:      
        MOU_DTLREC_tbl['RCI']=line[87:88]
        MOU_DTLREC_tbl['PARFROMDTCC']=line[88:96]
        MOU_DTLREC_tbl['PARTHRUDTCC']=line[96:104]
        MOU_DTLREC_tbl['IUDFROMCC']=line[104:112]
        MOU_DTLREC_tbl['IUDTHRUCC']=line[112:120]
        MOU_DTLREC_tbl['IBDCC']=line[120:128]
        MOU_DTLREC_tbl['EO_SW_ONR_ID']=line[142:146]
        MOU_DTLREC_tbl['LATA_WD_IND']=line[146:147]
        MOU_DTLREC_tbl['VOIP_USAGE_IND1']=line[147:148]
        MOU_DTLREC_tbl['INPUT_RECORDS']+="*"+str(record_id)

def process_TYP351000():
    global line
    global record_id
    global MOU_DTLREC_tbl    
    global current_abbd_rec_key    
    global curr_RE    
    global current_TACCT_FGRP
    global current_parent_rec
    global parent_3510_rec
    global abbdLst
    global abbdcLst
    global abbdcrecicjdLst
    
    curr_RE       = line[227:229] + line[82:84] + line[115:116] + line[85:86] + line[86:87] + line[87:88] + line[88:89] + line[94:95] + line[90:91] + line[91:92] + line[92:93] + line[93:94] + line[190:191] + line[95:96] + line[96:97]
    curr_jur_code = line[224:225]
    
    if line[95:96] != '0':
        count_record("351000_SKIPPED",False)
        parent_3510_rec=False
        current_parent_rec=''
    else:
        initialize_DTLREC_tbl()     
        curr_RE=line[227:229] + line[82:84] + line[115:116] + line[85:86] + line[86:87] + line[87:88] + line[88:89] + line[94:95] + line[90:91] + line[91:92] + line[92:93] + line[93:94] + line[190:191] + line[95:96] + line[96:97]
        current_record_num=line[72:82]        
        if current_TACCT_FGRP == '1':
            current_record_num=line[72:82]
        else:            
            current_record_num=' '
        currTSTP=line[208:211]
        if currTSTP == '999':
            currTSTP=' '        
        MOU_DTLREC_tbl['CLLI']=line[61:72]
        MOU_DTLREC_tbl['RECORD_NUM']=current_record_num
        MOU_DTLREC_tbl['RE']=curr_RE
        MOU_DTLREC_tbl['RCI']=line[97:98]
        MOU_DTLREC_tbl['PARFROMDTCC']=line[98:106]
        MOU_DTLREC_tbl['PARTHRUDTCC']=line[106:114]        
        MOU_DTLREC_tbl['LATA_WD_IND']=line[116:117]
        MOU_DTLREC_tbl['VOIP_USAGE_IND1']=line[117:118]        
        MOU_DTLREC_tbl['ST_ID']=line[139:141]
        MOU_DTLREC_tbl['QUANT']=convertnumber(line[141:152],0)
        MOU_DTLREC_tbl['RATE']=convertnumber(line[152:163],9)
        MOU_DTLREC_tbl['AMOUNT']=convertnumber(line[163:174],2)
        MOU_DTLREC_tbl['UBDFROMCC']=line[174:182]
        MOU_DTLREC_tbl['UBDTHRUCC']=line[182:190]
        MOU_DTLREC_tbl['EO_SW_ONR_ID']=line[191:195]        
        MOU_DTLREC_tbl['CIC']=line[203:208]
        #MOU_DTLREC_tbl['TSTP']=line[208:211]
        MOU_DTLREC_tbl['SP_TRAN_PCT']=currTSTP               
        MOU_DTLREC_tbl['ST_LV_CC']=line[211:215]        
        MOU_DTLREC_tbl['EO_TANDIND']=line[223:224]
        MOU_DTLREC_tbl['JRSDN_CD']=curr_jur_code
        MOU_DTLREC_tbl['INPUT_RECORDS']=str(record_id)
        
        #DEFAULTED VALUE COLUMNS        
        MOU_DTLREC_tbl['RCDCC']=''
        MOU_DTLREC_tbl['NAT_REG']='0'
        MOU_DTLREC_tbl['QRCI']='T'
        MOU_DTLREC_tbl['PIU']=' '
        MOU_DTLREC_tbl['AMILE']=' '
        MOU_DTLREC_tbl['TERM_CLLI']=' '
        MOU_DTLREC_tbl['TANDM_CLLI']=' '
        MOU_DTLREC_tbl['BIP']=' '
        MOU_DTLREC_tbl['LTL_IDENTIF']=' '
        MOU_DTLREC_tbl['TERM_CLLI']= inputLineCnt
          
        current_parent_rec="351000"

def process_TYP351500():
    global line
    global record_id
    global MOU_DTLREC_tbl    
    global parent_3515_rec
    global current_abbd_rec_key      
    global curr_RE    
    global current_parent_rec
    global current_TACCT_FGRP
    global abbdLst
    global abbdcLst
    global abbdcrecicjdLst
     
    if line[93:94] != '0':
        count_record("351500_SKIPPED",False)
        parent_3515_rec=False
        current_parent_rec=''
    else:
        initialize_DTLREC_tbl()
        curr_CLLI     = line[61:72]
        curr_RE=line[227:229] + line[82:84] + line[84:85] + line[85:86] + line[86:87] + ' ' + ' ' + line[89:90] + ' ' + ' ' + ' ' + ' ' + line[92:93] + line[93:94] + line[94:95]
        curr_CIC      = line[203:208]
        curr_jur_code = line[224:225]
        current_record_num=line[72:82]        
        if current_TACCT_FGRP == '1':
            current_record_num=line[72:82]
        else:            
            current_record_num=' '
        currTSTP=' '
        MOU_DTLREC_tbl['CLLI']=curr_CLLI
        MOU_DTLREC_tbl['RECORD_NUM']=current_record_num
        MOU_DTLREC_tbl['RE']=curr_RE
        MOU_DTLREC_tbl['RCI']=line[95:96]
        MOU_DTLREC_tbl['PARFROMDTCC']=line[96:104]
        MOU_DTLREC_tbl['PARTHRUDTCC']=line[104:112]
        MOU_DTLREC_tbl['ST_ID']=line[139:141]
        MOU_DTLREC_tbl['QUANT']=convertnumber(line[141:152],0)
        MOU_DTLREC_tbl['EO_SW_ONR_ID']=line[152:156]
        MOU_DTLREC_tbl['AMOUNT']=convertnumber(line[159:170],2)
        MOU_DTLREC_tbl['UBDFROMCC']=line[170:178]
        MOU_DTLREC_tbl['UBDTHRUCC']=line[178:186]
        MOU_DTLREC_tbl['RATE']=convertnumber(line[186:197],9)
        MOU_DTLREC_tbl['CIC']=curr_CIC
        MOU_DTLREC_tbl['SP_TRAN_PCT']=currTSTP
        MOU_DTLREC_tbl['ST_LV_CC']=line[211:215]
        MOU_DTLREC_tbl['VOIP_USAGE_IND1']=line[215:216]
        MOU_DTLREC_tbl['EO_TANDIND']=line[223:224]
        MOU_DTLREC_tbl['JRSDN_CD']=curr_jur_code
        MOU_DTLREC_tbl['INPUT_RECORDS']=str(record_id)
        
        #DEFAULTED VALUE COLUMNS
        MOU_DTLREC_tbl['RCDCC']=''
        MOU_DTLREC_tbl['NAT_REG']='0'
        MOU_DTLREC_tbl['QRCI']='F'
        MOU_DTLREC_tbl['PIU']=' '
        MOU_DTLREC_tbl['AMILE']=' '
        MOU_DTLREC_tbl['TERM_CLLI']=' '
        MOU_DTLREC_tbl['TANDM_CLLI']=' '
        MOU_DTLREC_tbl['BIP']=' '
        MOU_DTLREC_tbl['LTL_IDENTIF']=' '

        current_parent_rec="351500"

        
def process_TYP3520_PARENT():
    global line
    global record_id
    global MOU_DTLREC_tbl
    global parent_3520_rec
    global curr_3520_RE_part1
    global curr_3520_RE_part2
    global current_parent_rec
    global current_TACCT_FGRP
     
    if line[189:190] != '0':
        count_record("352000_SKIPPED",False)
        parent_3520_rec=False
        current_parent_rec=''
    else:
        initialize_DTLREC_tbl()
        curr_RE=line[227:229] + line[118:120] + line[191:192] + line[123:124] + line[222:223] + line[193:194] + line[192:193] + line[207:208] + line[190:191] + line[120:121] + line[122:123] + ' ' + line[121:122] + line[189:190] + line[205:206]
        curr_3520_RE_part1=line[227:229] + line[118:120] + line[191:192] + line[123:124] + line[222:223] + line[193:194] + line[192:193] + line[207:208] + line[190:191] + line[120:121] + line[122:123]
        curr_3520_RE_part2=line[121:122] + line[189:190] + line[205:206]
        current_record_num=line[108:118]                
        if current_TACCT_FGRP == '1':
            current_record_num=line[108:118]
        else:            
            current_record_num=' '
        tpiu=line[210:211] + line[211:212] + line[208:209]
        if tpiu == '999':
            MOU_DTLREC_tbl['PIU']=' '
        else:
            MOU_DTLREC_tbl['PIU']=tpiu
        #
        #MOU_DTLREC_tbl['OFFICE']=line[61:72]
        MOU_DTLREC_tbl['CLLI']=line[61:72]
        MOU_DTLREC_tbl['EO_TANDIND']=line[72:73]        
        MOU_DTLREC_tbl['LTL_IDENTIF']=line[97:108]
        MOU_DTLREC_tbl['RECORD_NUM']=current_record_num
        MOU_DTLREC_tbl['RE']=curr_RE
        MOU_DTLREC_tbl['AMILE']=line[124:127]
        MOU_DTLREC_tbl['QUANT']=convertnumber(line[127:138],0)
        MOU_DTLREC_tbl['VOIP_USAGE_IND1']=line[138:139]        
        MOU_DTLREC_tbl['RATE']=convertnumber(line[149:162],9)
        MOU_DTLREC_tbl['AMOUNT']=convertnumber(line[162:173],2)
        MOU_DTLREC_tbl['UBDFROMCC']=line[173:181]
        MOU_DTLREC_tbl['UBDTHRUCC']=line[181:189]
        MOU_DTLREC_tbl['ST_ID']=line[199:201]
        MOU_DTLREC_tbl['ST_LV_CC']=line[201:205]
        #MOU_DTLREC_tbl['PIU'] #calculated above
        MOU_DTLREC_tbl['JRSDN_CD']=line[224:225]
        MOU_DTLREC_tbl['INPUT_RECORDS']=str(record_id)
        
        #DEFAULTED VALUE COLUMNS
        MOU_DTLREC_tbl['RCDCC']=''
        MOU_DTLREC_tbl['QRCI']='2'       
        
        parent_3520_rec=True        
        current_parent_rec="352000"
        

def process_TYP3520_CHILD():
    global line
    global record_id
    global MOU_DTLREC_tbl
    global curr_RE    
    global current_ZCIC
    global current_parent_rec
    global parent_3520_rec
    global curr_3520_RE_part1
    global curr_3520_RE_part2
    
    if parent_3520_rec != True:
        count_record("352001_NO_PARENT",False)
        
    elif record_id=='352001':
        curr_RE= curr_3520_RE_part1 + line[106:107] + curr_3520_RE_part2       
        
        MOU_DTLREC_tbl['RE']=curr_RE
        MOU_DTLREC_tbl['RCI']=line[86:87]
        MOU_DTLREC_tbl['IUDFROMCC']=line[108:116]
        MOU_DTLREC_tbl['IUDTHRUCC']=line[116:124]
        MOU_DTLREC_tbl['IBDCC']=line[124:132]
        MOU_DTLREC_tbl['BAID_EO']=line[132:133]
        MOU_DTLREC_tbl['MPB_IND']=line[133:134]
        MOU_DTLREC_tbl['TANDM_CLLI']=line[189:200]
        MOU_DTLREC_tbl['CIC']=line[205:210]
        MOU_DTLREC_tbl['NAT_REG']=line[214:215]
        MOU_DTLREC_tbl['CNNCTN']=line[215:216]        
        MOU_DTLREC_tbl['TDM_TRNST_I']=line[223:224]
        MOU_DTLREC_tbl['INPUT_RECORDS']+="*"+str(record_id)        
        writelog("352001 record has " + MOU_DTLREC_tbl['INPUT_RECORDS'], output_log)
        
    elif record_id=='352002':
        MOU_DTLREC_tbl['EO_SW_ONR_ID']=line[84:88]
        MOU_DTLREC_tbl['TERM_CLLI']=line[88:99]        
        MOU_DTLREC_tbl['INPUT_RECORDS']+="*"+str(record_id)        
        writelog("352002 record has " + MOU_DTLREC_tbl['INPUT_RECORDS'], output_log)
        
    else:
        writelog("3520 Child record type not found", output_log)


def process_TYP3527_PARENT():
    global line
    global record_id
    global MOU_STATPG_tbl
    global parent_3527_rec
    global current_parent_rec
    
    #SUB_TOT_IND line[95:96]
    if line[95:96] != '0':
        count_record("352700_SKIPPED",False)
        parent_3527_rec=False        
        current_parent_rec=''        
    else:
        initialize_STATPG_tbl()
        
        MOU_STATPG_tbl['PCT_LO']='0'
        MOU_STATPG_tbl['CLLI']=line[61:72]
        #MOU_STATPG_tbl['OFFICE']=line[61:72]
        MOU_STATPG_tbl['REC_NUM']=line[72:82]
        MOU_STATPG_tbl['STATS_ELEM']=line[82:84]
        MOU_STATPG_tbl['RFB_CHG_IND']=line[84:85]
        MOU_STATPG_tbl['REC_MIN_IND']=line[85:86]
        MOU_STATPG_tbl['PIU_SRC_IND']=line[86:87]
        MOU_STATPG_tbl['NCTA_TT_IND']=line[87:88]
        MOU_STATPG_tbl['PCT_TRA_IND']=line[88:89]
        MOU_STATPG_tbl['NCTA_AP_IND']=line[89:90]
        MOU_STATPG_tbl['UNK_LAT_IND']=line[90:91]
        MOU_STATPG_tbl['NNJ_PRV_IND']=line[91:92]
        MOU_STATPG_tbl['INC_DIL_IND']=line[92:93]
        MOU_STATPG_tbl['CL_800_IND']=line[93:94]
        MOU_STATPG_tbl['DA_800_IND']=line[94:95]
        MOU_STATPG_tbl['SUB_TOT_IND']=line[95:96]
        MOU_STATPG_tbl['TER_CEL_IND']=line[96:97]
        MOU_STATPG_tbl['DIR_TANDIND']=line[99:100]
        MOU_STATPG_tbl['MULTI_JURIS']=line[100:101]
        MOU_STATPG_tbl['POT_TRAN_IND']=line[104:105]
        MOU_STATPG_tbl['VERT_FEATIND']=line[105:106]
        MOU_STATPG_tbl['ICEC_FACT']=convertnumber(line[106:112],5)
        MOU_STATPG_tbl['PCT_INTERST']=convertnumber(line[112:115],0)
        MOU_STATPG_tbl['PCT_TRATER']=convertnumber(line[115:118],0)
        MOU_STATPG_tbl['PCT_TRATRA']=convertnumber(line[118:121],0)
        MOU_STATPG_tbl['PCT_TERTER']=convertnumber(line[121:124],0)
        MOU_STATPG_tbl['EO_TAND_IND']=line[124:125]
        MOU_STATPG_tbl['FACT_#MESG']=convertnumber(line[125:136],0)
        MOU_STATPG_tbl['STATE']=line[136:138]
        MOU_STATPG_tbl['LOC_TRANSP']=line[138:140]
        MOU_STATPG_tbl['RECORD_MOU']=convertnumber(line[140:151],0)
        MOU_STATPG_tbl['NBR_MESG']=convertnumber(line[151:162],0)
        MOU_STATPG_tbl['ST_CO_CODE']=line[162:166]
        MOU_STATPG_tbl['FROMDATECC']=line[166:174]
        MOU_STATPG_tbl['THRUDATECC']=line[174:182]
        MOU_STATPG_tbl['ICEC_FAC_MIN']=convertnumber(line[182:190],5)
        MOU_STATPG_tbl['ATTEMPTS']=convertnumber(line[190:195],4)
        MOU_STATPG_tbl['MOU_ATTEMPT']=convertnumber(line[195:200],4)
        MOU_STATPG_tbl['TERM_ORG_RAT']=convertnumber(line[200:207],6)
        MOU_STATPG_tbl['FACTOR_MOU']=convertnumber(line[207:218],0)
        MOU_STATPG_tbl['BILL_ARRANG']=convertnumber(line[218:219],0)
        MOU_STATPG_tbl['NCTA_FACTOR']=convertnumber(line[219:224],4)
        MOU_STATPG_tbl['JURIS_IND']=line[224:225]
        MOU_STATPG_tbl['INPUT_RECORDS']=str(record_id)
        parent_3527_rec=True
        current_parent_rec="352700"


def process_TYP3527_CHILD():
    global line
    global record_id
    global MOU_STATPG_tbl
    global parent_3527_rec
    
    if parent_3527_rec != True:
          count_record("352701_NO_PARENT",False)
          writelog("Record - " + str(inputLineCnt) + " - 352701 - Has no 350500 parent", output_log)
#
    elif record_id=='352701':
        MOU_STATPG_tbl['TDM_TRNI_SP']=line[121:122]
        MOU_STATPG_tbl['PCT_LO']=line[123:126]
        MOU_STATPG_tbl['EXP_PIU']=convertnumber(line[157:162],2)
        MOU_STATPG_tbl['INPUT_RECORDS']+="*"+str(record_id)
#
    elif record_id=='352702':
        #MOU_STATPG_tbl['PCT_INET_BD'] = IF VERSION_NBR GE 39 THEN (EDIT(APCT_TRAF)) * .01 ELSE 0.00;
        #MOU_STATPG_tbl['PCT_INET_BD'] = "0"
        MOU_STATPG_tbl['PCT_INET_BD']=convertnumber(line[108:113],2);
        
        #MOU_STATPG_tbl['PRCNT_VOIP_USGE'] = IF VERSION_NBR GE 51 THEN (EDIT(APVU)) * .01 ELSE 0.00;
        #MOU_STATPG_tbl['PRCNT_VOIP_USGE'] = "0";
        MOU_STATPG_tbl['PRCNT_VOIP_USGE']=convertnumber(line[88:93],2);
        
        MOU_STATPG_tbl['PCT_TRA_LOC']=line[84:87]
        MOU_STATPG_tbl['VOIP_USAGE_IND']=line[87:88]
        #MOU_STATPG_tbl['APVU']=line[88:93]
        MOU_STATPG_tbl['PVU_SOURCE_IND']=line[93:94]
        MOU_STATPG_tbl['UNB_IXC_UIND']=line[104:105]
        MOU_STATPG_tbl['DA_TYP_IND']=line[105:107]
        MOU_STATPG_tbl['SRV_AR_ELEM']=line[107:108]
        #MOU_STATPG_tbl['APCT_TRAF']=line[108:113]
        MOU_STATPG_tbl['UNB_TDM_UIND']=line[113:114]
        MOU_STATPG_tbl['TDM_RTE_IND']=line[169:170]
        MOU_STATPG_tbl['SECONDS_IND']=line[174:175]
        MOU_STATPG_tbl['EO_SW_ONR_SP']=line[176:180]
        MOU_STATPG_tbl['D_PCT_OTHR_MSG']=line[192:195]
        MOU_STATPG_tbl['INPUT_RECORDS']+="*"+str(record_id)
#
    elif record_id=='352750':
        
        #MOU_STATPG_tbl['PRCNT_VOIP_USGE_C'] = IF VERSION_NBR GE 51 THEN (EDIT(APVUC)) * .01 ELSE 0.00;
        #MOU_STATPG_tbl['PRCNT_VOIP_USGE_C'] = "0"
        MOU_STATPG_tbl['PRCNT_VOIP_USGE_C'] = convertnumber(line[87:92],2);
        
        #MOU_STATPG_tbl['PRCNT_VOIP_USGE_P'] = IF VERSION_NBR GE 51 THEN (EDIT(APVUP)) * .01 ELSE 0.00;        
        #MOU_STATPG_tbl['PRCNT_VOIP_USGE_P'] = "0" 
        MOU_STATPG_tbl['PRCNT_VOIP_USGE_P'] = convertnumber(line[92:97],2);    
        
        #MOU_STATPG_tbl['APVUC']=line[87:92]
        #MOU_STATPG_tbl['APVUP']=line[92:97]
        MOU_STATPG_tbl['INPUT_RECORDS']+="*"+str(record_id)
    else:
        writelog("3527 Child record type not found", output_log)

def process_TYP390500():
    global line
    global record_id
    global MOU_DTLREC_tbl    
    global curr_RE
    global parent_3905_rec
    global current_ZCIC
    global current_TACCT_FGRP
    global current_abbd_rec_key
    global current_parent_rec
    global abbdLst
    global abbdcLst
    global abbdcrecicjdLst

    curr_RE = line[227:229] + line[97:99] + line[155:156] + line[103:104] + ' ' + ' ' + line[99:100] + ' ' + line[102:103] + ' ' + ' ' + ' ' + line[100:101] + line[154:155] + line[180:181]
    
    if line[154:155] != '0':
        count_record("390500_IND12_SKIPPED",False)
        parent_3905_rec=False
        current_parent_rec=''
    else:
        initialize_DTLREC_tbl()        

        MOU_DTLREC_tbl['RE']=curr_RE
        MOU_DTLREC_tbl['CLLI']=line[61:72]
    
        if current_TACCT_FGRP == '1':
            MOU_DTLREC_tbl['CIC']=' '
            current_record_num=line[214:224]
        else:
            MOU_DTLREC_tbl['CIC']=current_ZCIC
            current_record_num=' '       
      
        MOU_DTLREC_tbl['JRSDN_CD']=line[224:225]
        MOU_DTLREC_tbl['NAT_REG']='0'        
        MOU_DTLREC_tbl['EO_TANDIND']=line[72:73]
        MOU_DTLREC_tbl['RATPT1']=line[73:84]
        MOU_DTLREC_tbl['RATPT1IND']=line[84:85]
        MOU_DTLREC_tbl['RATPT2']=line[85:96]
        MOU_DTLREC_tbl['RATPT2IND']=line[96:97]        
        MOU_DTLREC_tbl['RECORD_NUM']=current_record_num
        MOU_DTLREC_tbl['AMILE']=line[104:107]
        MOU_DTLREC_tbl['QUANT']=convertnumber(line[107:118],0)
        MOU_DTLREC_tbl['RATE']=convertnumber(line[118:127],9)
        MOU_DTLREC_tbl['AMOUNT']=convertnumber(line[127:138],2)
        MOU_DTLREC_tbl['UBDFROMCC']=line[138:146]
        MOU_DTLREC_tbl['UBDTHRUCC']=line[146:154]
        MOU_DTLREC_tbl['SP_TRAN_PCT']=' '
        MOU_DTLREC_tbl['ST_ID']=line[156:158]
        MOU_DTLREC_tbl['ST_LV_CC']=line[158:162]
        MOU_DTLREC_tbl['BIP']=' '
        MOU_DTLREC_tbl['PIU']=' '
        MOU_DTLREC_tbl['RCI']=line[163:164]
        MOU_DTLREC_tbl['PARFROMDTCC']=line[164:172]
        MOU_DTLREC_tbl['PARTHRUDTCC']=line[172:180]
        MOU_DTLREC_tbl['MLTPL_SW_IND']=line[200:201]
        MOU_DTLREC_tbl['UNB_TRM_MPLR']=line[201:202]
        MOU_DTLREC_tbl['MLTPL_NTWK_I']=line[202:203]
        MOU_DTLREC_tbl['TANDM_CLLI']=line[203:214]        
        MOU_DTLREC_tbl['INPUT_RECORDS']=str(record_id)
        MOU_DTLREC_tbl['QRCI']='5'
        # RCDCC is a date field but the value is always defauled to ' ' because of BOS Version number
        # So default the RCDCC to blank/null
        MOU_DTLREC_tbl['RCDCC']=''

        parent_3905_rec=True
        current_parent_rec="390500"

def process_TYP391000():
    global line
    global record_id
    global MOU_DTLREC_tbl    
    global curr_RE
    global parent_3910_rec
    global current_ZCIC
    global current_TACCT_FGRP
    global current_abbd_rec_key
    global current_parent_rec
    global abbdLst
    global abbdcLst
    global abbdcrecicjdLst

    curr_RE = line[227:229] + line[83:85] + line[86:87] + line[137:138] + ' ' + ' ' + ' ' + line[89:90] + ' ' + ' ' + ' ' + ' ' + line[87:88] + line[138:139] + line[145:146]
    
    if line[138:139] != '0':
        count_record("391000_IND12_SKIPPED",False)
        parent_3910_rec=False
        current_parent_rec=''
    else:
        initialize_DTLREC_tbl()        

        MOU_DTLREC_tbl['RE']=curr_RE
        MOU_DTLREC_tbl['CLLI']=line[61:72]
        
        if current_TACCT_FGRP == '1':            
            current_record_num=line[214:224]
        else:            
            current_record_num=' '       
      
        MOU_DTLREC_tbl['JRSDN_CD']=line[224:225]
        MOU_DTLREC_tbl['QUANT']=convertnumber(line[90:101],0)
        MOU_DTLREC_tbl['RATE']=convertnumber(line[101:110],9)
        MOU_DTLREC_tbl['AMOUNT']=convertnumber(line[110:121],2)
        MOU_DTLREC_tbl['EO_TANDIND']=line[72:73]                    
        MOU_DTLREC_tbl['RECORD_NUM']=current_record_num
        MOU_DTLREC_tbl['MLTPL_NTWK_I']=line[167:168]
        MOU_DTLREC_tbl['MLTPL_SW_IND']=line[166:167]            
        MOU_DTLREC_tbl['PARFROMDTCC']=line[147:155]
        MOU_DTLREC_tbl['PARTHRUDTCC']=line[155:163]            
        MOU_DTLREC_tbl['RCI']=line[146:147]            
        MOU_DTLREC_tbl['ST_ID']=line[139:141]
        MOU_DTLREC_tbl['ST_LV_CC']=line[141:145]
        MOU_DTLREC_tbl['TRECORD_NUM']=line[214:224]
        MOU_DTLREC_tbl['UBDFROMCC']=line[121:129]
        MOU_DTLREC_tbl['UBDTHRUCC']=line[129:137]
        MOU_STATPG_tbl['INPUT_RECORDS']=str(record_id)
        #DEFAULTED VALUES
        MOU_DTLREC_tbl['PIU']=' '
        MOU_DTLREC_tbl['CIC']=' ' #ANUM IN FOCUS JOB
        MOU_DTLREC_tbl['LTL_IDENTIF']=' ' #LTL IN FOCUS JOB
        MOU_DTLREC_tbl['AMILE']=' '
        MOU_DTLREC_tbl['TERM_CLLI']=' '
        MOU_DTLREC_tbl['TANDM_CLLI']=' '
        MOU_DTLREC_tbl['BIP']=' '
        MOU_DTLREC_tbl['SP_TRAN_PCT']=' ' #STP IN FOCUS JOB
        MOU_DTLREC_tbl['QRCI']='T'
        MOU_DTLREC_tbl['NAT_REG']='0'
        
        
        parent_3910_rec=True
        current_parent_rec="391000"

def process_TYP392000():
    global line
    global record_id
    global MOU_DTLREC_tbl    
    global curr_RE
    global parent_3920_rec
    global current_ZCIC
    global current_TACCT_FGRP
    global current_abbd_rec_key
    global current_parent_rec
    
    curr_RE = line[227:229] + line[97:99] + line[159:160] + line[154:155] + ' ' + line[185:186] + ' ' + line[187:188] + line[153:154] + line[150:151] + line[152:153] + ' ' + line[151:152] + line[158:159] + line[207:208]
    
    if line[158:159] != '0':
        count_record("392000_IND12_SKIPPED",False)
        parent_3920_rec=False
        current_parent_rec=''
    else:
        initialize_DTLREC_tbl()        

        MOU_DTLREC_tbl['RE']=curr_RE
        MOU_DTLREC_tbl['CLLI']=line[61:72]
        
        if current_TACCT_FGRP == '1':            
            current_record_num=line[214:224]
        else:            
            current_record_num=' '
        
        #TPIU = TPIU_A|TPIU_B|TPIU_C
        tpiu=line[194:195] + line[186:187] + line[188:189]        
        if tpiu == '999':
            MOU_DTLREC_tbl['PIU']=' '
        else:
            MOU_DTLREC_tbl['PIU']=tpiu
      
        MOU_DTLREC_tbl['ST_ID']=line[160:162]        
        MOU_DTLREC_tbl['AMILE']=line[155:158]
        MOU_DTLREC_tbl['AMOUNT']=convertnumber(line[123:134],2)
        MOU_DTLREC_tbl['EO_TANDIND']=line[72:73]        
        MOU_DTLREC_tbl['JRSDN_CD']=line[224:225]        
        MOU_DTLREC_tbl['PARFROMDTCC']=line[169:177]
        MOU_DTLREC_tbl['PARTHRUDTCC']=line[177:185]
        MOU_DTLREC_tbl['QUANT']=convertnumber(line[99:110],0)
        MOU_DTLREC_tbl['RATE']=convertnumber(line[110:123],9)
        MOU_DTLREC_tbl['RCI']=line[168:169]        
        MOU_DTLREC_tbl['ST_LV_CC']=line[162:166]        
        MOU_DTLREC_tbl['RECORD_NUM']=current_record_num
        MOU_DTLREC_tbl['UBDFROMCC']=line[134:142]
        MOU_DTLREC_tbl['UBDTHRUCC']=line[142:150]
        MOU_STATPG_tbl['INPUT_RECORDS']=str(record_id)
        #DEFAULTED VALUES        
        MOU_DTLREC_tbl['CIC']=' ' #ANUM IN FOCUS JOB        
        MOU_DTLREC_tbl['TERM_CLLI']=' '
        MOU_DTLREC_tbl['TANDM_CLLI']=' '        
        MOU_DTLREC_tbl['BIP']=' '        
        MOU_DTLREC_tbl['QRCI']='2'
        
        parent_3920_rec=True
        current_parent_rec="392000"

def process_TYP3927_PARENT():
    global line
    global record_id
    global MOU_STATPG_tbl
    global parent_3927_rec
    global current_parent_rec
    
    #SUB_TOT_IND line[207:208]
    if line[207:208] != '0':
        count_record("392700_SKIPPED",False)
        parent_3927_rec=False
        current_parent_rec=''        
    else:
        initialize_STATPG_tbl()
        MOU_STATPG_tbl['CLLI']=line[61:72]
        MOU_STATPG_tbl['REC_NUM']=line[214:224]
        MOU_STATPG_tbl['ATTEMPTS']=convertnumber(line[147:152],4)
        MOU_STATPG_tbl['DA_800_IND']=line[175:176]
        MOU_STATPG_tbl['DIR_TANDIND']=line[91:92]
        MOU_STATPG_tbl['EO_TAND_IND']=line[72:73]
        MOU_STATPG_tbl['FACT_#MESG']=convertnumber(line[136:147],0)
        MOU_STATPG_tbl['FACTOR_MOU']=convertnumber(line[114:125],0)
        MOU_STATPG_tbl['FROMDATECC']=line[75:83]
        MOU_STATPG_tbl['JURIS_IND']=line[224:225]
        MOU_STATPG_tbl['MOU_ATTEMPT']=convertnumber(line[152:157],4)
        MOU_STATPG_tbl['NBR_MESG']=convertnumber(line[103:114],0)
        MOU_STATPG_tbl['NCTA_AP_IND']=line[173:174]
        MOU_STATPG_tbl['NCTA_FACTOR']=convertnumber(line[157:162],4)
        MOU_STATPG_tbl['NCTA_TT_IND']=line[171:172]
        MOU_STATPG_tbl['PCT_INTERST']=convertnumber(line[176:179],0)
        MOU_STATPG_tbl['PCT_TERTER']=convertnumber(line[185:188],0)
        MOU_STATPG_tbl['PCT_TRA_IND']=line[172:173]
        MOU_STATPG_tbl['PCT_TRATER']=convertnumber(line[179:182],0)
        MOU_STATPG_tbl['PCT_TRATRA']=convertnumber(line[182:185],0)
        MOU_STATPG_tbl['POT_TRAN_IND']=line[194:195]
        MOU_STATPG_tbl['REC_MIN_IND']=line[170:171]
        MOU_STATPG_tbl['RECORD_MOU']=convertnumber(line[92:103],0)
        MOU_STATPG_tbl['ST_CO_CODE']=line[203:207]
        MOU_STATPG_tbl['STATE']=line[201:203]
        MOU_STATPG_tbl['STATS_ELEM']=line[73:75]
        MOU_STATPG_tbl['SUB_TOT_IND']=line[207:208]
        MOU_STATPG_tbl['TERM_ORG_RAT']=convertnumber(line[162:169],6)
        MOU_STATPG_tbl['THRUDATECC']=line[83:91]
        MOU_STATPG_tbl['VERT_FEATIND']=line[195:196]
        MOU_STATPG_tbl['INPUT_RECORDS']=str(record_id)

        parent_3927_rec=True
        current_parent_rec="392700"

def process_TYP3927_CHILD():
    global line
    global record_id
    global MOU_STATPG_tbl
    global parent_3927_rec
    
    if parent_3927_rec != True:
          count_record("392701_NO_PARENT",False)
          writelog("Record - " + str(inputLineCnt) + " - 392701 - Has no 392700 parent", output_log)
#
    elif record_id=='392701':
        MOU_STATPG_tbl['RFB_CHG_IND']=line[78:79]
        MOU_STATPG_tbl['U28PARFROMCC']=line[79:87]
        MOU_STATPG_tbl['U28PARTHRUCC']=line[87:95]
        MOU_STATPG_tbl['U28_800CLIND']=line[95:96]
        MOU_STATPG_tbl['U28_CL_FCTR']=convertnumber(line[96:105],8)
        MOU_STATPG_tbl['U28_NCL_MOU']=convertnumber(line[105:116],0)
        MOU_STATPG_tbl['U28_CL_MOU']=convertnumber(line[116:127],0)
        MOU_STATPG_tbl['U28_CRCD_MOU']=convertnumber(line[127:138],0)
        MOU_STATPG_tbl['U28_CRCD_FTR']=convertnumber(line[138:147],8)
        MOU_STATPG_tbl['U28_NCC_MOU']=convertnumber(line[147:158],0)
        MOU_STATPG_tbl['U28_DIRRTG']=convertnumber(line[158:163],4)
        MOU_STATPG_tbl['U28_ISW_FCTR']=convertnumber(line[173:178],4)
        MOU_STATPG_tbl['U28_LTR_API']=line[178:179]
        MOU_STATPG_tbl['U28_ISW_TMOU']=convertnumber(line[180:191],0)
        MOU_STATPG_tbl['U28_ISW_DMOU']=convertnumber(line[191:202],0)
        #        
        #ATDM_SWPCT=line[170:173]
        #U28_TDM_SPCT = IF VERSION_NBR GE 39 THEN (EDIT(ATDM_SWPCT)) ELSE 0;
        MOU_STATPG_tbl['U28_TDM_SPCT']=convertnumber(line[170:173],0)
        
        MOU_STATPG_tbl['INPUT_RECORDS']+="*"+str(record_id)        
#
    else:
        writelog("3927 Child record type not found", output_log)

def initialize_DTLREC_tbl():    
    global MOU_DTLREC_tbl
#    global mou_version

    MOU_DTLREC_tbl['ACNA']=current_abbd_rec_key['ACNA']
    MOU_DTLREC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    MOU_DTLREC_tbl['BAN']=current_abbd_rec_key['BAN']
#    MOU_DTLREC_tbl['MOU_VERSION']=mou_version

    MOU_DTLREC_tbl['CLLI']=''
    MOU_DTLREC_tbl['RCI']=''
    MOU_DTLREC_tbl['RCDCC']=''
    MOU_DTLREC_tbl['UBDFROMCC']=''
    MOU_DTLREC_tbl['UBDTHRUCC']=''
    MOU_DTLREC_tbl['ST_ID']=''
    MOU_DTLREC_tbl['ST_LV_CC']=''
    MOU_DTLREC_tbl['IUDFROMCC']=''
    MOU_DTLREC_tbl['IUDTHRUCC']=''
    MOU_DTLREC_tbl['IBDCC']=''
    MOU_DTLREC_tbl['BIP']=''
    MOU_DTLREC_tbl['RATE']=''
    MOU_DTLREC_tbl['QUANT']=''
    MOU_DTLREC_tbl['AMOUNT']=''
    MOU_DTLREC_tbl['SP_TRAN_PCT']=''
    MOU_DTLREC_tbl['BAID_EO']=''
    MOU_DTLREC_tbl['MPB_IND']=''
    MOU_DTLREC_tbl['PIU']=''
    MOU_DTLREC_tbl['AMILE']=''
    MOU_DTLREC_tbl['LTL_IDENTIF']=''
    MOU_DTLREC_tbl['RECORD_NUM']=''
    MOU_DTLREC_tbl['TERM_CLLI']=''
    MOU_DTLREC_tbl['TANDM_CLLI']=''
    MOU_DTLREC_tbl['PARFROMDTCC']=''
    MOU_DTLREC_tbl['PARTHRUDTCC']=''
    MOU_DTLREC_tbl['EO_TANDIND']=''
    MOU_DTLREC_tbl['QRCI']=''
    MOU_DTLREC_tbl['CNNCTN']=''
    MOU_DTLREC_tbl['EO_SW_ONR_ID']=''
    MOU_DTLREC_tbl['LATA_WD_IND']=''
    MOU_DTLREC_tbl['MLTPL_SW_IND']=''
    MOU_DTLREC_tbl['MLTPL_NTWK_I']=''
    MOU_DTLREC_tbl['TDM_TRNST_I']=''
    MOU_DTLREC_tbl['UNB_TRM_MPLR']=''
    MOU_DTLREC_tbl['RATPT1']=''
    MOU_DTLREC_tbl['RATPT1IND']=''
    MOU_DTLREC_tbl['RATPT2']=''
    MOU_DTLREC_tbl['RATPT2IND']=''
    MOU_DTLREC_tbl['VOIP_USAGE_IND1']=''

def  initialize_STATPG_tbl():
    global MOU_BCTFMOU_tbl
    global current_abbd_rec_key 
#    global mou_version
    
    MOU_STATPG_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    MOU_STATPG_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    MOU_STATPG_tbl['BAN']=current_abbd_rec_key['BAN']   
#    MOU_STATPG_tbl['MOU_VERSION']=mou_version
    MOU_STATPG_tbl['CLLI']=''
    
    MOU_STATPG_tbl['REC_NUM']=''
    MOU_STATPG_tbl['STATS_ELEM']=''
    MOU_STATPG_tbl['RFB_CHG_IND']=''
    MOU_STATPG_tbl['REC_MIN_IND']=''
    MOU_STATPG_tbl['PIU_SRC_IND']=''
    MOU_STATPG_tbl['NCTA_TT_IND']=''
    MOU_STATPG_tbl['PCT_TRA_IND']=''
    MOU_STATPG_tbl['NCTA_AP_IND']=''
    MOU_STATPG_tbl['UNK_LAT_IND']=''
    MOU_STATPG_tbl['NNJ_PRV_IND']=''
    MOU_STATPG_tbl['INC_DIL_IND']=''
    MOU_STATPG_tbl['CL_800_IND']=''
    MOU_STATPG_tbl['DA_800_IND']=''
    MOU_STATPG_tbl['SUB_TOT_IND']=''
    MOU_STATPG_tbl['TER_CEL_IND']=''
    MOU_STATPG_tbl['DIR_TANDIND']=''
    MOU_STATPG_tbl['MULTI_JURIS']=''
    MOU_STATPG_tbl['POT_TRAN_IND']=''
    MOU_STATPG_tbl['VERT_FEATIND']=''
    MOU_STATPG_tbl['ICEC_FACT']=''
    MOU_STATPG_tbl['PCT_INTERST']=''
    MOU_STATPG_tbl['PCT_TRATER']=''
    MOU_STATPG_tbl['PCT_TRATRA']=''
    MOU_STATPG_tbl['PCT_TERTER']=''
    MOU_STATPG_tbl['EO_TAND_IND']=''
    MOU_STATPG_tbl['FACT_#MESG']=''
    MOU_STATPG_tbl['STATE']=''
    MOU_STATPG_tbl['LOC_TRANSP']=''
    MOU_STATPG_tbl['WATS_EXTDER']=''
    MOU_STATPG_tbl['RECORD_MOU']=''
    MOU_STATPG_tbl['NBR_MESG']=''
    MOU_STATPG_tbl['ST_CO_CODE']=''
    MOU_STATPG_tbl['FROMDATECC']=''
    MOU_STATPG_tbl['THRUDATECC']=''
    MOU_STATPG_tbl['ICEC_FAC_MIN']=''
    MOU_STATPG_tbl['ATTEMPTS']=''
    MOU_STATPG_tbl['MOU_ATTEMPT']=''
    MOU_STATPG_tbl['TERM_ORG_RAT']=''
    MOU_STATPG_tbl['FACTOR_MOU']=''
    MOU_STATPG_tbl['BILL_ARRANG']=''
    MOU_STATPG_tbl['NCTA_FACTOR']=''
    MOU_STATPG_tbl['JURIS_IND']=''
    MOU_STATPG_tbl['PCT_LO']=''
    MOU_STATPG_tbl['PCT_TRA_LOC']=''
    MOU_STATPG_tbl['UNB_IXC_UIND']=''
    MOU_STATPG_tbl['DA_TYP_IND']=''
    MOU_STATPG_tbl['SRV_AR_ELEM']=''
    MOU_STATPG_tbl['TDM_RTE_IND']=''
    MOU_STATPG_tbl['SECONDS_IND']=''
    MOU_STATPG_tbl['EO_SW_ONR_SP']=''
    MOU_STATPG_tbl['U28PARFROMCC']=''
    MOU_STATPG_tbl['U28PARTHRUCC']=''
    MOU_STATPG_tbl['U28_800CLIND']=''
    MOU_STATPG_tbl['U28_CL_FCTR']=''
    MOU_STATPG_tbl['U28_NCL_MOU']=''
    MOU_STATPG_tbl['U28_CL_MOU']=''
    MOU_STATPG_tbl['U28_CRCD_FTR']=''
    MOU_STATPG_tbl['U28_CRCD_MOU']=''
    MOU_STATPG_tbl['U28_NCC_MOU']=''
    MOU_STATPG_tbl['U28_DIRRTG']=''
    MOU_STATPG_tbl['U28_ISW_FCTR']=''
    MOU_STATPG_tbl['U28_LTR_API']=''
    MOU_STATPG_tbl['U28_ISW_TMOU']=''
    MOU_STATPG_tbl['U28_ISW_DMOU']=''
    MOU_STATPG_tbl['TDM_TRNI_SP']=''
    MOU_STATPG_tbl['PCT_INET_BD']=''
    MOU_STATPG_tbl['UNB_TDM_UIND']=''
    MOU_STATPG_tbl['U28_TDM_SPCT']=''
    MOU_STATPG_tbl['D_PCT_OTHR_MSG']=''
    MOU_STATPG_tbl['VOIP_USAGE_IND']=''
    MOU_STATPG_tbl['PVU_SOURCE_IND']=''
    MOU_STATPG_tbl['PRCNT_VOIP_USGE']=''
    MOU_STATPG_tbl['PRCNT_VOIP_USGE_C']=''
    MOU_STATPG_tbl['PRCNT_VOIP_USGE_P']=''
    MOU_STATPG_tbl['EXP_PIU']=''


def  initialize_BCTFMOU_tbl():
    global MOU_BCTFMOU_tbl
    global current_abbd_rec_key 
#    global mou_version
    
    MOU_BCTFMOU_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    MOU_BCTFMOU_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    MOU_BCTFMOU_tbl['BAN']=current_abbd_rec_key['BAN']
#    MOU_BCTFMOU_tbl['MOU_VERSION']=mou_version
    MOU_BCTFMOU_tbl['BILL_DATE']=''
    MOU_BCTFMOU_tbl['MPB']=''
    MOU_BCTFMOU_tbl['TLF']=''
    MOU_BCTFMOU_tbl['TACCNT_FGRP']=''
    MOU_BCTFMOU_tbl['ICSC_OFC']=''
    MOU_BCTFMOU_tbl['NLATA']=''
    MOU_BCTFMOU_tbl['HOLD_BILL']=''
    MOU_BCTFMOU_tbl['IBC_SBC']=''
    MOU_BCTFMOU_tbl['BIL_ACC_REF']=''
    MOU_BCTFMOU_tbl['BACR_1']=''
    MOU_BCTFMOU_tbl['BACR_2']=''
    MOU_BCTFMOU_tbl['BACR_3']=''
    MOU_BCTFMOU_tbl['BACR_4']=''
    MOU_BCTFMOU_tbl['BACR_5']=''
    MOU_BCTFMOU_tbl['BACR_6']=''
    MOU_BCTFMOU_tbl['BACR_7']=''
    MOU_BCTFMOU_tbl['CAIMS_REL']=''
    MOU_BCTFMOU_tbl['EOBDATECC']=''
    MOU_BCTFMOU_tbl['BILLDATECC']=''
    MOU_BCTFMOU_tbl['UNB_SWA_PROV']=''
    MOU_BCTFMOU_tbl['MAN_BAN_TYPE']=''
    
#def db_record_exists(acna,ban,eob_date):
#    params = {'acna':acna.rstrip(' '), 'ban':ban.rstrip(' '), 'eob_date':eob_date}
#    
#    selCur = con.cursor()
#    selCur.execute("SELECT count(*) FROM caims_mou_bctfmou WHERE acna=:acna AND ban=:ban AND eob_date=TO_DATE(:eob_date,'YYMMDD')", params)
#    if selCur.fetchone()[0] > 0:
#        selCur.close()
#        return True
#    selCur.close()
#    return False
    
"########################################################################################################################"
"####################################TRANSLATION MODULES#################################################################"

   
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
    
def process_insertmany():
    global mou_bctfmou_records, mou_dtlrec_records, mou_statpg_records
    global MOU_BCTFMOU_DEFN_DICT, MOU_DTLREC_DEFN_DICT, MOU_STATPG_DEFN_DICT    
    global con
    global output_log
    global schema
    global cntr, rows, insql, results, inscnt
    global bctfmou_cnt, dtlrec_cnt, statpg_cnt
    
    results={}
    selCur = con.cursor()

    cntr=0
    rows=[]
    insql=""
    inscnt=0
   
    if len(mou_bctfmou_records) > 0:
        for key, value in mou_bctfmou_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table("CAIMS_MOU_BCTFMOU", value, MOU_BCTFMOU_DEFN_DICT,con,schema, 'SQ_CSR_BCCBSPL',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()

    if len(mou_dtlrec_records) > 0:
        for key, value in mou_dtlrec_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table("CAIMS_MOU_DTLREC", value, MOU_DTLREC_DEFN_DICT,con,schema, 'SQ_CSR_ACTLREC',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()        
    
    if len(mou_statpg_records) > 0:  
        for key, value in mou_statpg_records.items():       
            tmpResult=process_insert_many_table("CAIMS_MOU_STATPG", value, MOU_STATPG_DEFN_DICT,con,schema, 'SQ_CSR_BILLREC',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()
         

    selCur.close()
    
    mou_bctfmou_records={}
    mou_dtlrec_records={}
    mou_statpg_records={}

    #writelog("Records Inserted", output_log)    

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

"####################################TRANSLATION MODULES END ############################################################"    
"########################################################################################################################" 

   
def process_write_program_stats():
    global record_counts
    global unknown_record_counts
    global BDT_KEY_cnt
    writelog("\n", output_log)
    
    idCnt=0
    writelog("**", output_log)
    writelog("**Processed record IDs and counts**", output_log)
    writelog("record_id, count", output_log)
    
    keylist = record_counts.keys()
    keylist.sort()
    for key in keylist:
#        print "%s: %s" % (key, record_counts[key])   
        writelog(str(key)+", "+str(record_counts[key]), output_log)   
        idCnt+=record_counts[key]       
#    for key, value in record_counts.items():
#        writelog(str(key)+", "+str(value))   
#        idCnt+=value
    writelog("\n  Total count: "+str(idCnt), output_log)
    writelog(" ", output_log)
    writelog("**", output_log)
    unkCnt=0
    writelog( "There are "+str(len(unknown_record_counts))+" different record ID types not processed: ", output_log)    
    writelog("**UNKNOWN record IDs and counts**", output_log)
    writelog("record_id, count", output_log)

    keylist = unknown_record_counts.keys()
    keylist.sort()
    for key in keylist:
#        print "%s: %s" % (key, record_counts[key])   
        writelog(str(key)+", "+str(unknown_record_counts[key]), output_log)   
        unkCnt+=unknown_record_counts[key]  
    
    
#    for key, value in unknown_record_counts.items():
#        writelog(str(key)+", "+str(value))
#        unkCnt+=value
    writelog("\n  Total count: "+str(unkCnt), output_log)    
    writelog("**", output_log)    

    writelog("TOTAL ACNA/EOB_DATE/BAN's processed:"+str(BDT_KEY_cnt), output_log)
    writelog(" ", output_log)
    writelog("Total input records read from input file:"+str(idCnt+unkCnt), output_log)
    writelog(" ", output_log)    

def process_ERROR_END(msg):
    writelog("ERROR:"+msg)
    process_close_files()
    raise "ERROR:"+msg
    
    
def process_close_files():
    global bdt_input
    global output_log
    
        
    bdt_input.close();
    output_log.close()
   
    
    
def endProg(msg):
#    global hdlr
 
    con.commit()
    con.close()
    
    process_write_program_stats()
     
    endTM=datetime.datetime.now()
    print "\nTotal run time:  %s" % (endTM-startTM)
    writelog("\nTotal run time:  %s" % (endTM-startTM), output_log)
     

    writelog("\n"+msg, output_log)
     
    process_close_files()
    
#   hdlr.close()
#   logger.removeHandler(hdlr)



"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
"BEGIN Program logic"
"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


print "Starting Program"
init()
main()
endProg("-END OF PROGRAM-")