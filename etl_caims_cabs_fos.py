# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
PROGRAM NAME:     etl_caims_cabs_fos.py                                  
LOCATION:                      
PROGRAMMER(S):    Jason Yelverton                                
DESCRIPTION:      CAIMS Extract/Transformation/Load program for Bill Data Tape
                  (FOS) records.
                  
REPLACES:         Legacy CTL program BC100FT0 - LOAD BCCBFOS FOCUS DATABASE.
                                                                         
LANGUAGE/VERSION: Python/2.7.10                                          
INITIATION DATE:  x/x/2016                                                
INPUT FILE(S):    PBCL.CY.XRU0102O.CABS.G0*V00.txt (build from GDG)
LOCATION:         MARION MAINFRAME           
                                                                         
OUTPUT:           ORACLE DWBS001P                                
                  Table names CAIMS_FOS_*
                                                                         
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
global fos_cnt
global ckt_cnt 
global line
global current_parent_rec
global parent_1010_rec
global parent_6005_rec

global dup_ban_lock
global invalid_fgrp_lock
global badKey

fos_cnt = 0
ckt_cnt = 0

parent_1010_rec=False
parent_6005_rec=False
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

fos_bctffos_records={}
fos_psm_ckt_records={}


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
VALID_RECORD_TYPES={'010100':'PARENT', '050500':'CHILD',\
                    '600500':'PARENT', '600501':'CHILD'}

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
    log_file=os.path.join(output_dir,"BDTFOS_Cycle"+str(cycl_yy)+str(cycl_mmdd)+str(cycl_time)+"_"+startTM.strftime("%Y%m%d_@%H%M") +".txt")
         
    output_log = open(log_file, "w");
    output_log.write("-BDTFOS CAIMS ETL PROCESS-")
    
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
    global FOS_BCTFFOS_tbl
    global FOS_BCTFFOS_DEFN_DICT
    global FOS_PSM_CKT_tbl
    global FOS_PSM_CKT_DEFN_DICT
    
    
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
    global fos_bctffos_records, fos_psm_ckt_records
    
    firstBillingRec=True
#DECLARE TABLE ARRAYS AND DICTIONARIES    
    FOS_BCTFFOS_DEFN_DICT=createTableTypeDict('CAIMS_FOS_BCTFFOS',con,schema,output_log)
    FOS_BCTFFOS_tbl=setDictFields('CAIMS_FOS_BCTFFOS', FOS_BCTFFOS_DEFN_DICT) 
    
    FOS_PSM_CKT_DEFN_DICT=createTableTypeDict('CAIMS_FOS_PSM_CKT',con,schema,output_log)
    FOS_PSM_CKT_tbl=setDictFields('CAIMS_FOS_PSM_CKT', FOS_PSM_CKT_DEFN_DICT) 
    
    
        
        
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
            print str(inputLineCnt)+" lines completed processing***********************************"
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
                        fos_bctffos_records={}
                        fos_psm_ckt_records={}
                        
                
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
    
    
    
    if record_id == '010100':
        process_TYP010100_PARENT()
        set_flags(record_id)
# 
    elif record_id == '050500':  #050500 records update the 010100 record
        process_TYP010100_CHILD()
#        
    elif record_id == '600500':             
        process_TYP6005_PARENT()
        set_flags(record_id)
#
    elif record_id == '600501':        
        process_TYP6005_CHILD()
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
    global FOS_BCTFFOS_tbl
    global FOS_BCTFFOS_DEFN_DICT
    global FOS_PSM_CKT_tbl
    global FOS_PSM_CKT_DEFN_DICT
    global fos_cnt, ckt_cnt
    global fos_bctffos_records, fos_psm_ckt_records

    if current_parent_rec=='010100':        
        #process_insert_table("TMP_CAIMS_FOS_BCTFFOS", FOS_BCTFFOS_tbl, FOS_BCTFFOS_DEFN_DICT)
        fos_cnt  += 1
        fos_bctffos_records[fos_cnt]= copy.deepcopy(FOS_BCTFFOS_tbl)
        initialize_BCTFFOS_tbl()
        current_parent_rec=record_id
    
    elif current_parent_rec=='600500':
        #process_insert_table("TMP_CAIMS_FOS_PSM_CKT", FOS_PSM_CKT_tbl, FOS_PSM_CKT_DEFN_DICT)
        ckt_cnt += 1
        fos_psm_ckt_records[ckt_cnt]= copy.deepcopy(FOS_PSM_CKT_tbl)   
        initialize_PSM_CKT_tbl()        
        current_parent_rec=record_id
    else:
        current_parent_rec=record_id


def set_flags(current_record_type):
    global parent_1010_rec
    global parent_6005_rec    
    
    if current_record_type=='010100':        
        parent_6005_rec = False        
    elif current_record_type=='600500':
        parent_1010_rec = False
    else:
        parent_1010_rec=False
        parent_6005_rec=False
    
def process_TYP010100_PARENT():
    global FOS_BCTFFOS_tbl
    global current_TACCT_FGRP
    global current_abbd_rec_key
    global current_ZCIC
    global dup_ban_lock
    global invalid_fgrp_lock
    global parent_1010_rec
    global line
    global current_parent_rec
    
    initialize_BCTFFOS_tbl()
    
    tacct=line[107:108]
    tfgrp=line[108:109]   
    current_TACCT_FGRP=translate_TACCNT_FGRP(tacct,tfgrp)
    
    if current_abbd_rec_key in abbdLst or db_record_exists('CAIMS_FOS_BCTFFOS',current_abbd_rec_key['ACNA'],current_abbd_rec_key['BAN'],current_abbd_rec_key['EOB_DATE'], con, schema, output_log):
        dup_ban_lock=True
        writelog("WARNING: DUPLICATE BAN - ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'], output_log)
        parent_1010_rec=False
        current_parent_rec=''
    else:
        dup_ban_lock=False
        invalid_fgrp_lock=False
        abbdLst.append(current_abbd_rec_key)
        jdate=line[71:76]
    #   FOS_BCTFFOS_tbl['BANLOCK']='N'    #defaulted db value
    #   FOS_BCTFFOS_tbl['VERSION_NBR']=line[82:84]    
        FOS_BCTFFOS_tbl['TLF']=line[97:98]
        FOS_BCTFFOS_tbl['NLATA']=line[98:101]
        FOS_BCTFFOS_tbl['HOLD_BILL']=line[105:106]
        #FOS_BCTFFOS_tbl['TACCNT']=tacct
        #FOS_BCTFFOS_tbl['TFGRP']=tfgrp  
        FOS_BCTFFOS_tbl['TACCNT_FGRP']=current_TACCT_FGRP
        FOS_BCTFFOS_tbl['BILL_DATE']=jdate
        #FOS_BCTFFOS_tbl['EOBDATEA']=FOS_BCTFFOS_tbl['EOB_DATE']
        FOS_BCTFFOS_tbl['EOBDATECC']=FOS_BCTFFOS_tbl['EOB_DATE']
        FOS_BCTFFOS_tbl['BILLDATECC']=FOS_BCTFFOS_tbl['BILL_DATE']
        FOS_BCTFFOS_tbl['CAIMS_REL']='B'
        FOS_BCTFFOS_tbl['MPB']=line[224:225]
        FOS_BCTFFOS_tbl['INPUT_RECORDS']=str(record_id)
        current_ZCIC=line[110:115]        
        
        parent_1010_rec=True
        current_parent_rec="010100"
        #read the 0505 record next       
 
    
def process_TYP010100_CHILD():    
    global FOS_BCTFFOS_tbl
    global record_id
    global line  

    if record_id=='050500':
        FOS_BCTFFOS_tbl['ICSC_OFC']=line[211:215].rstrip(' ').lstrip(' ')
        FOS_BCTFFOS_tbl['INPUT_RECORDS']+="*"+str(record_id)    
    else:
        writelog("WARNING: INVALID 010100 CHILD - "+record_id, output_log)


def process_TYP6005_PARENT():
    global line
    global record_id
    global FOS_PSM_CKT_tbl
    global parent_6005_rec
    global current_parent_rec    

    initialize_PSM_CKT_tbl()     
    
    #FOS_PSM_CKT_tbl['ACNA']=line[6:11]
    #FOS_PSM_CKT_tbl['EOB_DATE']=line[11:17]
    #FOS_PSM_CKT_tbl['BAN']=line[17:30]
    FOS_PSM_CKT_tbl['HICAP_IND']=line[30:31]
    FOS_PSM_CKT_tbl['ASG']=line[31:37]
    FOS_PSM_CKT_tbl['CLF_CLS_IND']=line[37:38]
    FOS_PSM_CKT_tbl['CKTNO']=line[38:48]
    FOS_PSM_CKT_tbl['BASE_IND']=line[48:49]    
    FOS_PSM_CKT_tbl['PSM']=line[61:77]
    FOS_PSM_CKT_tbl['EC_CKT_ID']=line[61:108]
    FOS_PSM_CKT_tbl['LTP_FID']=line[108:112]
    FOS_PSM_CKT_tbl['PSEUDO_CKT_IND']=line[112:113]
    FOS_PSM_CKT_tbl['PLN_TYP_IND']=line[113:114]
    FOS_PSM_CKT_tbl['IC_CKT_ID']=line[114:167]
    FOS_PSM_CKT_tbl['MPB_ID']=line[167:168]
    FOS_PSM_CKT_tbl['CKT_TYP_ID']=line[168:169]
    FOS_PSM_CKT_tbl['ST_LV_CC']=line[169:173]
    FOS_PSM_CKT_tbl['ST_ID']=line[173:175]    
    FOS_PSM_CKT_tbl['SODE_IND']=line[175:176]
    FOS_PSM_CKT_tbl['CKT_CHG_IR']=convertnumber(line[177:188],2)
    FOS_PSM_CKT_tbl['CKTCHG_PP_IR']=convertnumber(line[188:199],2)
    FOS_PSM_CKT_tbl['CKT_CHG_IA']=convertnumber(line[199:210],2)
    FOS_PSM_CKT_tbl['CKTCHG_PP_IA']=convertnumber(line[210:221],2)
    FOS_PSM_CKT_tbl['SPLT_BLD_CKT_IND']=line[221:222]
    FOS_PSM_CKT_tbl['SONET_IND']=line[222:223]
    FOS_PSM_CKT_tbl['CFA_IND']=line[223:224]
    FOS_PSM_CKT_tbl['INPUT_RECORDS']=str(record_id)

    parent_6005_rec=True
    current_parent_rec="600500"

def process_TYP6005_CHILD():
    global line
    global record_id
    global FOS_PSM_CKT_tbl
    global parent_6005_rec
    
    if parent_6005_rec != True:
        count_record("600501_NO_PARENT",False)
        writelog("Record - " + str(inputLineCnt) + " - 600501 - Has no 600500 parent", output_log)
    else:      
        #FOS_PSM_CKT_tbl['ACNA']=line[6:11]
        #FOS_PSM_CKT_tbl['EOB_DATE']=line[11:17]
        #FOS_PSM_CKT_tbl['BAN']=line[17:30]        
        FOS_PSM_CKT_tbl['PSM']=line[61:77]        
        FOS_PSM_CKT_tbl['EC_CKT_ID']=line[61:108]
        FOS_PSM_CKT_tbl['AC_SVC_GRP']=line[108:119]
        FOS_PSM_CKT_tbl['CFA']=line[119:161]
        FOS_PSM_CKT_tbl['CKT_CHG_IRIA']=convertnumber(line[161:172],2)
        FOS_PSM_CKT_tbl['CKT_CHG_IAIA']=convertnumber(line[172:183],2)
        FOS_PSM_CKT_tbl['CKT_CHG_LOC']=convertnumber(line[183:194],2)
        FOS_PSM_CKT_tbl['INPUT_RECORDS']+="*"+str(record_id)

def initialize_PSM_CKT_tbl():    
    global FOS_PSM_CKT_tbl
    global current_abbd_rec_key

    FOS_PSM_CKT_tbl['ACNA']=current_abbd_rec_key['ACNA']
    FOS_PSM_CKT_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    FOS_PSM_CKT_tbl['BAN']=current_abbd_rec_key['BAN']

    FOS_PSM_CKT_tbl['HICAP_IND']=''
    FOS_PSM_CKT_tbl['ASG']=''
    FOS_PSM_CKT_tbl['CLF_CLS_IND']=''
    FOS_PSM_CKT_tbl['CKTNO']=''
    FOS_PSM_CKT_tbl['BASE_IND']=''
    FOS_PSM_CKT_tbl['PSM']=''
    FOS_PSM_CKT_tbl['EC_CKT_ID']=''
    FOS_PSM_CKT_tbl['LTP_FID']=''
    FOS_PSM_CKT_tbl['PSEUDO_CKT_IND']=''
    FOS_PSM_CKT_tbl['PLN_TYP_IND']=''
    FOS_PSM_CKT_tbl['IC_CKT_ID']=''
    FOS_PSM_CKT_tbl['MPB_ID']=''
    FOS_PSM_CKT_tbl['CKT_TYP_ID']=''
    FOS_PSM_CKT_tbl['ST_LV_CC']=''
    FOS_PSM_CKT_tbl['ST_ID']=''
    FOS_PSM_CKT_tbl['SODE_IND']=''
    FOS_PSM_CKT_tbl['CKT_CHG_IR']=''
    FOS_PSM_CKT_tbl['CKTCHG_PP_IR']=''
    FOS_PSM_CKT_tbl['CKT_CHG_IA']=''
    FOS_PSM_CKT_tbl['CKTCHG_PP_IA']=''
    FOS_PSM_CKT_tbl['SPLT_BLD_CKT_IND']=''
    FOS_PSM_CKT_tbl['SONET_IND']=''
    FOS_PSM_CKT_tbl['CFA_IND']=''

def  initialize_BCTFFOS_tbl():
    global FOS_BCTFFOS_tbl
    global current_abbd_rec_key
#    global mou_version
    
    FOS_BCTFFOS_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    FOS_BCTFFOS_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    FOS_BCTFFOS_tbl['BAN']=current_abbd_rec_key['BAN']
#    MOU_BCTFMOU_tbl['MOU_VERSION']=mou_version
    FOS_BCTFFOS_tbl['BILL_DATE']=''
    FOS_BCTFFOS_tbl['MPB']=''
    FOS_BCTFFOS_tbl['TLF']=''
    FOS_BCTFFOS_tbl['TACCNT_FGRP']=''
    FOS_BCTFFOS_tbl['ICSC_OFC']=''
    FOS_BCTFFOS_tbl['NLATA']=''
    FOS_BCTFFOS_tbl['HOLD_BILL']=''
    FOS_BCTFFOS_tbl['IBC_SBC']=''
    FOS_BCTFFOS_tbl['BIL_ACC_REF']=''
    FOS_BCTFFOS_tbl['BACR_1']=''
    FOS_BCTFFOS_tbl['BACR_2']=''
    FOS_BCTFFOS_tbl['BACR_3']=''
    FOS_BCTFFOS_tbl['BACR_4']=''
    FOS_BCTFFOS_tbl['BACR_5']=''
    FOS_BCTFFOS_tbl['BACR_6']=''
    FOS_BCTFFOS_tbl['BACR_7']=''
    FOS_BCTFFOS_tbl['CAIMS_REL']=''
    FOS_BCTFFOS_tbl['EOBDATECC']=''
    FOS_BCTFFOS_tbl['BILLDATECC']=''
    FOS_BCTFFOS_tbl['UNB_SWA_PROV']=''
    FOS_BCTFFOS_tbl['MAN_BAN_TYPE']=''

    
def process_insertmany():
    global FOS_BCTFFOS_DEFN_DICT, FOS_PSM_CKT_DEFN_DICT
    global fos_bctffos_records, fos_psm_ckt_records
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

   
    if len(fos_bctffos_records) > 0:
        for key, value in fos_bctffos_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table("CAIMS_FOS_BCTFFOS", value,FOS_BCTFFOS_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()

    if len(fos_psm_ckt_records) > 0:
        for key, value in fos_psm_ckt_records.items():
            inscnt+=1
            tmpResult=process_insert_many_table("CAIMS_FOS_PSM_CKT", value, FOS_PSM_CKT_DEFN_DICT,con,schema, 'SQ',output_log)        
            results=copy.deepcopy(tmpResult)
            prepareInsert(value)
        selCur.prepare(str(insql))
        selCur.executemany(None, rows)
        con.commit()      
        resetVals()        

    selCur.close()
    
    fos_bctffos_records={}
    fos_psm_ckt_records={}


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
    global fos_cnt
    global ckt_cnt 
    
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
    writelog("Total FOS inserts:"+str(fos_cnt), output_log)
    writelog(" ", output_log)   
    writelog("Total CKT inserts:"+str(ckt_cnt), output_log)
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