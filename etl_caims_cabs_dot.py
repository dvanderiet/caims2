# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
PROGRAM NAME:     etl_caims_cabs_dot.py                                  
LOCATION:                      
PROGRAMMER(S):    Jason Yelverton                                
DESCRIPTION:      CAIMS Extract/Transformation/Load program for Bill Data Tape
                  (DOT) records.
                  
REPLACES:         Legacy CTL program BC100FT0 - LOAD BCCBDOT FOCUS DATABASE.
                                                                         
LANGUAGE/VERSION: Python/2.7.10                                          
INITIATION DATE:  x/x/2016                                                
INPUT FILE(S):    PBCL.CY.XRU0102O.CABS.G0*V00.txt (build from GDG)
LOCATION:         MARION MAINFRAME           
                                                                         
OUTPUT:           ORACLE DWBS001P                                
                  Table names CAIMS_DOT_*
                                                                         
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
from etl_caims_cabs_utility import  process_insert_many_table, translate_TACCNT_FGRP, writelog, format_date, \
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

"GLOBAL VARIABLES"   
global dot_cnt
global schg_cnt 
global ftax_cnt 
global line
global current_parent_rec
global parent_1010_rec
global parent_5005_rec
global parent_5505_rec
global tstx

global dup_ban_lock
global invalid_fgrp_lock
global badKey

tstx=''
dot_cnt = 0
schg_cnt = 0
ftax_cnt = 0

parent_1010_rec=False
parent_5005_rec=False
parent_5505_rec=False
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
#Create lists for each unique record
abbdLst=[]              #acna ban billdate 
abbdcLst=[]             #acna ban billdate clli
abbdcrecicjdLst=[]      #acna ban billdate clli re cic jursdn_cd (3505,3510,3515,3520 record types RATEREC)
abbdcrnLst=[]           #acna ban billdate clli record_num (3527 record types STATPG)

dot_bctfdot_records={}
dot_detftax_records={}
dot_detschg_records={}

record_counts={}
unknown_record_counts={}
BILL_REC='10' 
STAR_LINE='*********************************************************************************************'  
MONTH_DICT={'01':'JAN','02':'FEB','03':'MAR','04':'APR','05':'MAY','06':'JUN','07':'JUL','08':'AUG','09':'SEP','10':'OCT','11':'NOV','12':'DEC'}
DIGIT_DICT={'0':'0','1':'1','2':'2','3':'3','4':'4','5':'5','6':'6','7':'7','8':'8','9':'9',\
            '{':'0','A':'1','B':'2','C':'3','D':'4','E':'5','F':'6','G':'7','H':'8','I':'9',\
            '}':'0','J':'1','K':'2','L':'3','M':'4','N':'5','O':'6','P':'7','Q':'8','R':'9'} 
NEGATIVE_NUMS=['}','J','K','L','M','N','O','P','Q','R']
VALID_FGRP=['1','2','3','4','J','U']
VALID_RECORD_TYPES={'010100':'PARENT',\
                    '500500':'PARENT',\
                    '550500':'PARENT'}

def debug(msg):
    if debugOn:
        debug_log.write("\n"+str(msg))

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
    log_file=os.path.join(output_dir,"BDTDOT_Cycle"+str(cycl_yy)+str(cycl_mmdd)+str(cycl_time)+"_"+startTM.strftime("%Y%m%d_@%H%M") +".txt")
         
    output_log = open(log_file, "w");
    output_log.write("-BDTDOT CAIMS ETL PROCESS-")
    
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
    global DOT_BCTFDOT_tbl
    global DOT_BCTFDOT_DEFN_DICT
    global DOT_DETSCHG_tbl
    global DOT_DETSCHG_DEFN_DICT
    global DOT_DETFTAX_tbl
    global DOT_DETFTAX_DEFN_DICT
    "text files"
    global bdt_input
    global output_log
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
    global dot_bctfdot_records, dot_detftax_records, dot_detschg_records
    
    firstBillingRec=True
#DECLARE TABLE ARRAYS AND DICTIONARIES
    DOT_BCTFDOT_DEFN_DICT=createTableTypeDict('CAIMS_DOT_BCTFDOT',con,schema,output_log)
    DOT_BCTFDOT_tbl=setDictFields('CAIMS_DOT_BCTFDOT', DOT_BCTFDOT_DEFN_DICT) 
    
    DOT_DETSCHG_DEFN_DICT=createTableTypeDict('CAIMS_DOT_DETSCHG',con,schema,output_log)
    DOT_DETSCHG_tbl=setDictFields('CAIMS_DOT_DETSCHG', DOT_DETSCHG_DEFN_DICT) 
    
    DOT_DETFTAX_DEFN_DICT=createTableTypeDict('CAIMS_DOT_DETFTAX',con,schema,output_log)
    DOT_DETFTAX_tbl=setDictFields('CAIMS_DOT_DETFTAX', DOT_DETFTAX_DEFN_DICT) 
    
    "COUNTERS"
    inputLineCnt=1 #header record was read in init()
    BDT_KEY_cnt=0
    status_cnt=0
    exmanyCnt=0
    "KEY"
    
    prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}
#    theban=0
    
    "LOOP THROUGH INPUT CABS TEXT FILE"
    for line in bdt_input:
        
        "Count each line"
        inputLineCnt += 1 
        status_cnt+=1
        if status_cnt>999:
            print str(inputLineCnt)+" lines completed processing***********************************"+str(datetime.datetime.now())
            status_cnt=0               
        
        if len(line) > 231:
            record_id=line[225:231]
        else:
            record_id=line[:6]

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
                        dot_bctfdot_records={}
                        dot_detftax_records={}
                        dot_detschg_records={}
                
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
    global record_id
    global firstBillingRec
    
    if firstBillingRec:
        "FIRST RECORD ONLY"
        #write BOS version number to log
        writelog("**--------------------------**", output_log)
        writelog("** BOS VERSION NUMBER IS "+line[82:84]+" ** ", output_log)
        writelog("**--------------------------**", output_log)
        firstBillingRec=False
        
    unknownRecord=False

    tstx = line[73:78]
    
    if record_id == '010100':
        process_TYP010100_PARENT()
        set_flags(record_id)
#        
    elif record_id == '550500' and tstx != 'TOTAL':             
        process_TYP5505_PARENT()
        set_flags(record_id)
#
    elif record_id == '500500' and tstx != 'TOTAL':        
        process_TYP5005_PARENT()
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
    global current_parent_rec
    global DOT_BCTFDOT_tbl
    global DOT_BCTFDOT_DEFN_DICT
    global DOT_DETSCHG_tbl
    global DOT_DETSCHG_DEFN_DICT
    global DOT_DETFTAX_tbl
    global DOT_DETFTAX_DEFN_DICT
    global dot_cnt, schg_cnt, ftax_cnt
    global dot_bctfdot_records, dot_detftax_records, dot_detschg_records

    if current_parent_rec=='010100':        
        #process_insert_table("CAIMS_DOT_BCTFDOT", DOT_BCTFDOT_tbl, DOT_BCTFDOT_DEFN_DICT)
        dot_cnt  += 1
        dot_bctfdot_records[dot_cnt]= copy.deepcopy(DOT_BCTFDOT_tbl)        
        initialize_BCTFDOT_tbl()
        current_parent_rec=record_id
    
    elif current_parent_rec=='500500':
        #process_insert_table("CAIMS_DOT_DETFTAX", DOT_DETFTAX_tbl, DOT_DETFTAX_DEFN_DICT)
        ftax_cnt += 1
        dot_detftax_records[ftax_cnt]= copy.deepcopy(DOT_DETFTAX_tbl)
        initialize_DETFTAX_tbl()        
        current_parent_rec=record_id
        
    elif current_parent_rec=='550500':
        #process_insert_table("CAIMS_DOT_DETSCHG", DOT_DETSCHG_tbl, DOT_DETSCHG_DEFN_DICT)
        schg_cnt += 1
        dot_detschg_records[schg_cnt]= copy.deepcopy(DOT_DETSCHG_tbl)        
        initialize_DETSCHG_tbl()        
        current_parent_rec=record_id
        
    else:
        current_parent_rec=record_id

def set_flags(current_record_type):
    global parent_1010_rec
    global parent_5005_rec
    global parent_5505_rec    
    
    if current_record_type=='010100':        
        parent_5005_rec = False
        parent_5505_rec = False
    elif current_record_type=='500500':
        parent_1010_rec = False
        parent_5505_rec = False
    elif current_record_type=='550500':
        parent_1010_rec = False
        parent_5005_rec = False
    else:
        parent_1010_rec=False
        parent_5005_rec=False
    
def process_TYP010100_PARENT():
    global DOT_BCTFDOT_tbl
    global current_TACCT_FGRP
    global current_abbd_rec_key
    global current_ZCIC
    global dup_ban_lock
    global invalid_fgrp_lock
    global parent_1010_rec
    global line
    global current_parent_rec
    
    initialize_BCTFDOT_tbl()
    
    tacct=line[107:108]
    tfgrp=line[108:109]   
    current_TACCT_FGRP=translate_TACCNT_FGRP(tacct,tfgrp)
    
    if current_abbd_rec_key in abbdLst or db_record_exists('CAIMS_DOT_BCTFDOT', current_abbd_rec_key['ACNA'],current_abbd_rec_key['BAN'],current_abbd_rec_key['EOB_DATE'], con, schema, output_log):
        dup_ban_lock=True
        writelog("WARNING: DUPLICATE BAN - ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'], output_log)
        parent_1010_rec=False
        current_parent_rec=''
    else:
        dup_ban_lock=False
        invalid_fgrp_lock=False
        abbdLst.append(current_abbd_rec_key)
        jdate=line[71:76]
    #   DOT_BCTFDOT_tbl['BANLOCK']='N'    #defaulted db value
    #   DOT_BCTFDOT_tbl['VERSION_NBR']=line[82:84]    
        DOT_BCTFDOT_tbl['TLF']=line[97:98]
        DOT_BCTFDOT_tbl['NLATA']=line[98:101]
        DOT_BCTFDOT_tbl['HOLD_BILL']=line[105:106]
        #DOT_BCTFDOT_tbl['TACCNT']=tacct
        #DOT_BCTFDOT_tbl['TFGRP']=tfgrp  
        DOT_BCTFDOT_tbl['TACCNT_FGRP']=current_TACCT_FGRP
        DOT_BCTFDOT_tbl['BILL_DATE']=jdate
        #DOT_BCTFDOT_tbl['EOBDATEA']=DOT_BCTFDOT_tbl['EOB_DATE']
        DOT_BCTFDOT_tbl['EOBDATECC']=DOT_BCTFDOT_tbl['EOB_DATE']
        DOT_BCTFDOT_tbl['BILLDATECC']=DOT_BCTFDOT_tbl['BILL_DATE']
        DOT_BCTFDOT_tbl['CAIMS_REL']='B'
        DOT_BCTFDOT_tbl['MPB']=line[224:225]
        DOT_BCTFDOT_tbl['INPUT_RECORDS']=str(record_id)
        current_ZCIC=line[110:115]        
        
        parent_1010_rec=True
        current_parent_rec="010100"

def process_TYP5505_PARENT():
    global line
    global record_id
    global DOT_DETSCHG_tbl
    global parent_5505_rec
    global current_parent_rec    

    initialize_DETSCHG_tbl()     
    
    DOT_DETSCHG_tbl['DINV_DATE']=line[32:37]
    DOT_DETSCHG_tbl['SCHG_IND']=line[61:62]
    DOT_DETSCHG_tbl['SCHG_JUR']=line[62:73]
    DOT_DETSCHG_tbl['SCHG_PHRASE']=line[73:116]
    DOT_DETSCHG_tbl['SCHG_MONTHLY']=convertnumber(line[116:127],2)
    DOT_DETSCHG_tbl['SCHG_USAGE']=convertnumber(line[127:138],2)
    DOT_DETSCHG_tbl['SCHG_OTHER']=convertnumber(line[138:149],2)
    DOT_DETSCHG_tbl['SCHG_MLTST_I']=line[160:161]
    DOT_DETSCHG_tbl['TSTATE']=line[161:163]
    DOT_DETSCHG_tbl['SCHG_ST_ID']=line[161:163]
    DOT_DETSCHG_tbl['SCHG_STLVL']=line[163:167]
    DOT_DETSCHG_tbl['INPUT_RECORDS']=str(record_id)

    parent_5505_rec=True
    current_parent_rec="550500"

def process_TYP5005_PARENT():
    global line
    global record_id
    global DOT_DETFTAX_tbl
    global parent_5005_rec
    global current_parent_rec    

    initialize_DETFTAX_tbl()     

    DOT_DETFTAX_tbl['DINV_DATE']=line[32:37]
    DOT_DETFTAX_tbl['TAX_IND']=line[61:62]
    DOT_DETFTAX_tbl['TAX_JUR']=line[62:73]
    DOT_DETFTAX_tbl['TAX_PHRASE']=line[73:116]
    DOT_DETFTAX_tbl['TAX_MONTHLY']=convertnumber(line[116:127],2)
    DOT_DETFTAX_tbl['TAX_USAGE']=convertnumber(line[127:138],2)
    DOT_DETFTAX_tbl['TAX_OTHER']=convertnumber(line[138:149],2)
    DOT_DETFTAX_tbl['TAX_MLTST_I']=line[160:161]
    DOT_DETFTAX_tbl['TSTATE']=line[161:163]
    DOT_DETFTAX_tbl['TAX_ST_ID']=line[161:163]
    DOT_DETFTAX_tbl['TAX_STLVL']=line[163:167]
    DOT_DETFTAX_tbl['INPUT_RECORDS']=str(record_id)

    parent_5005_rec=True
    current_parent_rec="500500"

def initialize_DETSCHG_tbl():    
    global DOT_DETSCHG_tbl
    global current_abbd_rec_key
    
    DOT_DETSCHG_tbl['ACNA']=current_abbd_rec_key['ACNA']
    DOT_DETSCHG_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    DOT_DETSCHG_tbl['BAN']=current_abbd_rec_key['BAN']
    DOT_DETSCHG_tbl['DINV_DATE']=''
    DOT_DETSCHG_tbl['SCHG_IND']=''
    DOT_DETSCHG_tbl['SCHG_JUR']=''
    DOT_DETSCHG_tbl['SCHG_PHRASE']=''
    DOT_DETSCHG_tbl['SCHG_MONTHLY']=''
    DOT_DETSCHG_tbl['SCHG_USAGE']=''
    DOT_DETSCHG_tbl['SCHG_OTHER']=''
    DOT_DETSCHG_tbl['SCHG_MLTST_I']=''
    DOT_DETSCHG_tbl['TSTATE']=''
    DOT_DETSCHG_tbl['SCHG_ST_ID']=''
    DOT_DETSCHG_tbl['SCHG_STLVL']=''
    
def initialize_DETFTAX_tbl():    
    global DOT_DETFTAX_tbl
    global current_abbd_rec_key

    DOT_DETFTAX_tbl['ACNA']=current_abbd_rec_key['ACNA']
    DOT_DETFTAX_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    DOT_DETFTAX_tbl['BAN']=current_abbd_rec_key['BAN']
    DOT_DETFTAX_tbl['DINV_DATE']=''
    DOT_DETFTAX_tbl['TAX_IND']=''
    DOT_DETFTAX_tbl['TAX_JUR']=''
    DOT_DETFTAX_tbl['TAX_PHRASE']=''
    DOT_DETFTAX_tbl['TAX_MONTHLY']=''
    DOT_DETFTAX_tbl['TAX_USAGE']=''
    DOT_DETFTAX_tbl['TAX_OTHER']=''
    DOT_DETFTAX_tbl['TAX_MLTST_I']=''
    DOT_DETFTAX_tbl['TSTATE']=''
    DOT_DETFTAX_tbl['TAX_ST_ID']=''
    DOT_DETFTAX_tbl['TAX_STLVL']=''

def  initialize_BCTFDOT_tbl():
    global DOT_BCTFDOT_tbl
    global current_abbd_rec_key
    
    DOT_BCTFDOT_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    DOT_BCTFDOT_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    DOT_BCTFDOT_tbl['BAN']=current_abbd_rec_key['BAN']
    DOT_BCTFDOT_tbl['BILL_DATE']=''
    DOT_BCTFDOT_tbl['MPB']=''
    DOT_BCTFDOT_tbl['TLF']=''
    DOT_BCTFDOT_tbl['TACCNT_FGRP']=''
    DOT_BCTFDOT_tbl['ICSC_OFC']=''
    DOT_BCTFDOT_tbl['NLATA']=''
    DOT_BCTFDOT_tbl['HOLD_BILL']=''
    DOT_BCTFDOT_tbl['IBC_SBC']=''
    DOT_BCTFDOT_tbl['BIL_ACC_REF']=''
    DOT_BCTFDOT_tbl['BACR_1']=''
    DOT_BCTFDOT_tbl['BACR_2']=''
    DOT_BCTFDOT_tbl['BACR_3']=''
    DOT_BCTFDOT_tbl['BACR_4']=''
    DOT_BCTFDOT_tbl['BACR_5']=''
    DOT_BCTFDOT_tbl['BACR_6']=''
    DOT_BCTFDOT_tbl['BACR_7']=''
    DOT_BCTFDOT_tbl['CAIMS_REL']=''
    DOT_BCTFDOT_tbl['EOBDATECC']=''
    DOT_BCTFDOT_tbl['BILLDATECC']=''
    DOT_BCTFDOT_tbl['UNB_SWA_PROV']=''
    DOT_BCTFDOT_tbl['MAN_BAN_TYPE']=''
    
#def format_date(datestring):
#    
#    dtSize=len(datestring)
#    
#    if dtSize ==5:
#        #jdate conversion
#        return "TO_DATE('"+datestring+"','YY-DDD')"
#    elif dtSize==6:
#        return "TO_DATE('"+datestring[4:6]+"-"+MONTH_DICT[datestring[2:4]]+"-"+"20"+datestring[:2]+"','DD-MON-YY')"  
#    elif dtSize==8:
#        return "TO_DATE('"+datestring[:4]+"-"+datestring[4:6]+"-"+datestring[6:8]+"','YYYY-MM-DD')"
    
def process_insertmany():
    global DOT_BCTFDOT_DEFN_DICT, DOT_DETSCHG_DEFN_DICT, DOT_DETFTAX_DEFN_DICT
    global dot_bctfdot_records, dot_detftax_records, dot_detschg_records
    global con
    global output_log
    global schema
    global cntr, rows, insql, results, inscnt
    
    results={}
    selCur = con.cursor()

    cntr=0
    rows=[]
    insql=""
    inscnt=0

   
    if len(dot_bctfdot_records) > 0:
        for key, value in dot_bctfdot_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table("CAIMS_DOT_BCTFDOT", value,DOT_BCTFDOT_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()

    if len(dot_detftax_records) > 0:
        for key, value in dot_detftax_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table("CAIMS_DOT_DETFTAX", value, DOT_DETFTAX_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()        
    
    if len(dot_detschg_records) > 0:  
        for key, value in dot_detschg_records.items():       
            tmpResult=process_insert_many_table("CAIMS_DOT_DETSCHG", value, DOT_DETSCHG_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()         

    selCur.close()
    
    dot_bctfdot_records={}
    dot_detftax_records={}
    dot_detschg_records={}

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
    
"####################################TRANSLATION MODULES END ############################################################"    
"########################################################################################################################" 

def process_write_program_stats():
    global record_counts
    global unknown_record_counts
    global BDT_KEY_cnt
    global dot_cnt    
    global schg_cnt
    global ftax_cnt
    
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
    
    
    writelog("\n  Total count: "+str(unkCnt), output_log)    
    writelog("**", output_log)    
    
    writelog("TOTAL ACNA/EOB_DATE/BAN's processed:"+str(BDT_KEY_cnt), output_log)
    writelog(" ", output_log)
    writelog("Total input records read from input file:"+str(idCnt+unkCnt), output_log)
    writelog(" ", output_log)   
    writelog("Total DOT inserts:"+str(dot_cnt), output_log)
    writelog(" ", output_log)   
    writelog("Total SCHG inserts:"+str(schg_cnt), output_log)
    writelog(" ", output_log)   
    writelog("Total FTAX inserts:"+str(ftax_cnt), output_log)
    writelog(" ", output_log)   
    
    
def process_ERROR_END(msg):
    writelog("ERROR:"+msg, output_log)
    process_close_files()
    raise "ERROR:"+msg
    
    
def process_close_files():
    global bdt_input
    global output_log
    
    bdt_input.close();
    output_log.close()
   
    
    
def endProg(msg):
 
    con.commit()
    con.close()
    
    process_write_program_stats()
     
    endTM=datetime.datetime.now()
    print "\nTotal run time:  %s" % (endTM-startTM)
    writelog("\nTotal run time:  %s" % (endTM-startTM), output_log)
     

    writelog("\n"+msg, output_log)
     
    process_close_files()
    
"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
"BEGIN Program logic"
"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


print "Starting Program"
init()
main()
endProg("-END OF PROGRAM-")