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
import cx_Oracle
import sys
import ConfigParser
import platform
import os
import logging

startTM=datetime.datetime.now();

settings = ConfigParser.ConfigParser();
settings.read('C:/Users/cjy005423/Documents/CAIMS FILES/settings.ini')
settings.sections()

"INIT LOGGING TO A FILE"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
hdlr = logging.FileHandler("C:/Users/cjy005423/Documents/Python Scripts/out/example.log")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 

"SET ORACLE CONNECTION"
con=cx_Oracle.connect(settings.get('OracleSettings','OraCAIMSUser'),settings.get('OracleSettings','OraCAIMSPw'),settings.get('OracleSettings','OraCAIMSSvc'))    


"CONSTANTS"
if str(platform.system()) == 'Windows': 
    output_dir = settings.get('GlobalSettings','WINDOWS_LOG_DIR');
else:
    output_dir = settings.get('GlobalSettings','LINUX_LOG_DIR');
    
#set to true to get debug statements
debugOn=True
if debugOn:
    debug_log=open(os.path.join(output_dir,settings.get('BDTSettings','BDT_DEBUG_FILE_NM')),"w")

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

fullname =os.path.join(inputPath,fileNm)

if os.path.isfile(fullname):
    print ("Input file:"+fullname)
else:
    raise Exception("ERROR: File not found:"+fullname)


"GLOBAL VARIABLES"    
global statpg_rec
global line
global dtl_rec
global parent_1010_rec
global parent_3505_rec
global parent_3520_rec
global parent_3527_rec
global parent_3905_rec
global dup_ban_lock
global invalid_fgrp_lock
global badKey

global curr_3520_RE_part1
global curr_3520_RE_part2

statpg_rec=False
dtl_rec=False
parent_1010_rec=False
parent_3505_rec=False
parent_3520_rec=False
parent_3527_rec=False
parent_3905_rec=False
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
VALID_RECORD_TYPES={'010100':'PARENT', '050500':'CHILD', '070500':'CHILD',                      \
                    '350500':'PARENT', '350501':'CHILD',                                        \
                    '351000':'PARENT',                                                          \
                    '351500':'PARENT',                                                          \
                    '352000':'PARENT', '352001':'CHILD', '352002':'CHILD',                      \
                    '352700':'PARENT', '352701':'CHILD', '352702':'CHILD', '352750':'CHILD',    \
                    '352500':'PARENT'}
   
"TRANSLATERS"

def debug(msg):
    if debugOn:
        debug_log.write("\n"+str(msg))

def init():
    global bdt_input 
    global bdt_BCTFMOU_log
    global record_id
  
    "OPEN FILES"
    "   CABS INPUT FILE"
    bdt_input = open(fullname, "r");
    
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
         
    bdt_BCTFMOU_log = open(log_file, "w");
    bdt_BCTFMOU_log.write("-BDT CAIMS ETL PROCESS-")
    
    if record_id not in settings.get('BDTSettings','BDTHDR'):
        process_ERROR_END("The first record in the input file was not a "+settings.get('BDTSettings','BDTHDR').rstrip(' ')+" record.")

    writelog("Process "+sys.argv[0])
    writelog("   started execution at: " + str(startTM))
    writelog(STAR_LINE)
    writelog(" ")
    writelog("Input file: "+str(bdt_input))
    writelog("Log file: "+str(bdt_BCTFMOU_log))
    
    "Write header record informatio only"
    writelog("Header Record Info:")
    writelog("     Record ID: "+record_id+" YY: "+cycl_yy+", MMDD: "+cycl_mmdd+", TIME: "+str(cycl_time))
    
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
    global MOU_RATEREC_tbl
    global MOU_RATEREC_DEFN_DICT
    global MOU_DTLREC_tbl
    global MOU_DTLREC_DEFN_DICT
    
    
    "text files"
    global bdt_input
    global bdt_BCTFMOU_log
    global BDT_column_names
    global firstBillingRec    
    global record_id
    global prev_record_id
    global current_abbd_rec_key 
    global prev_abbd_rec_key   
    global goodKey
    global dup_ban_lock
    global invalid_fgrp_lock
    
    firstBillingRec=True
#DECLARE TABLE ARRAYS AND DICTIONARIES
    MOU_BCTFMOU_tbl={}
    MOU_BCTFMOU_DEFN_DICT=createTableTypeDict('CAIMS_MOU_BCTFMOU')
    MOU_STATPG_tbl={}
    MOU_STATPG_DEFN_DICT=createTableTypeDict('CAIMS_MOU_STATPG')    
    MOU_RATEREC_tbl={}
    MOU_RATEREC_DEFN_DICT=createTableTypeDict('CAIMS_MOU_RATEREC')
    MOU_DTLREC_tbl={}
    MOU_DTLREC_DEFN_DICT=createTableTypeDict('CAIMS_MOU_DTLREC')
    

    "COUNTERS"
    inputLineCnt=1 #header record was read in init()
    BDT_KEY_cnt=0
    status_cnt=0
    "KEY"
    
    prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}
    
    "LOOP THROUGH INPUT CABS TEXT FILE"
    for line in bdt_input:    
        
        inputLineCnt += 1 #Count each line
        status_cnt+=1
        if status_cnt>999:
            print str(inputLineCnt)+" lines completed processing***********************************"
            status_cnt=0               
        
        if len(line) > 231:
            record_id=line[225:231]
        else:
            record_id=line[:6]

        #if inputLineCnt > 5000:
        #    break
        "Process by record_id"    
        "Header rec (first rec) processed in init()"
        #If the new record is a parent we need to insert records
        if record_id in VALID_RECORD_TYPES.keys() and VALID_RECORD_TYPES[record_id] == "PARENT":
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
                writelog("WARNING: BAD INPUT DATA.  ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'])
            else:
                if current_abbd_rec_key != prev_abbd_rec_key:
                    BDT_KEY_cnt+=1                    
                    
                process_bill_records()
   
            "set previous key for comparison in next iteration"   
            prev_abbd_rec_key=current_abbd_rec_key
            
        elif record_id in settings.get('BDTSettings','BDTFTR'):
            process_inserts(record_id)
            "FOOTER RECORD"
            log_footer_rec_info()     
            
        else:
            "ERROR:Unidentified Record"
            writelog("ERROR: Not sure what type of record this is:")
            writelog(line)
         
         
        prev_record_id=record_id
        "END-MAIN PROCESS LOOP"    
    #END of For Loop
 

def log_footer_rec_info():
    global record_id
    
    BAN_cnt=line[6:12]
    REC_cnt=line[13:22]
    writelog(" ")
    writelog("Footer Record Info:")
    writelog("     Record ID "+record_id+" BAN Count: "+BAN_cnt+" RECORD CNT: "+REC_cnt)
    writelog("The total number of lines counted in input file is : "+ str(inputLineCnt))
    writelog(" ")
    
    count_record(record_id,False)


def process_bill_records():
    global MOU_BCTFMOU_tbl
    global record_id
    global firstBillingRec    
    
    if firstBillingRec:
        "FIRST RECORD ONLY"
        #write BOS version number to log
        writelog("**--------------------------**")
        writelog("** BOS VERSION NUMBER IS "+line[82:84]+" ** ")
        writelog("**--------------------------**")
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
    elif record_id[:4] == '3525':
        #process_TYP352500()
        writelog(line[6:30]+"- 3525 - SUB_TOT_IND - "+line[96:97])        
    else:  #UNKNOWN RECORDS
        unknownRecord=True
    
    count_record(record_id,unknownRecord)


def process_getkey():
    global badKey
    
    if line[6:11].rstrip(' ') == '' or line[17:30].rstrip(' ') == '' or line[11:17].rstrip(' ') == '':
        badKey=True
    else:
        badKey=False
        
    return { 'ACNA':line[6:11],'EOB_DATE':line[11:17],'BAN':line[17:30]}
     
     
     
     
def process_inserts(record_id):
    global MOU_BCTFMOU_tbl
    global MOU_BCTFMOU_DEFN_DICT
    global MOU_DTLREC_tbl
    global MOU_DTLREC_DEFN_DICT
    global MOU_STATPG_tbl
    global MOU_STATPG_DEFN_DICT    
    global current_parent_rec

#    if current_parent_rec=='010100' and record_id != '050500' and record_id != '070500':
#        process_insert_table("CAIMS_MOU_BCTFMOU", MOU_BCTFMOU_tbl,MOU_BCTFMOU_DEFN_DICT)
#        current_parent_rec=''
        
    if current_parent_rec=='010100':
        writelog("INSERTING 010100 RECORD")
        process_insert_table("CAIMS_MOU_BCTFMOU", MOU_BCTFMOU_tbl,MOU_BCTFMOU_DEFN_DICT)
        current_parent_rec=record_id
    
    elif current_parent_rec=='350500':
        writelog("INSERTING 350500 RECORD")
        process_insert_table("CAIMS_MOU_DTLREC", MOU_DTLREC_tbl,MOU_DTLREC_DEFN_DICT)
        current_parent_rec=record_id
    
    elif current_parent_rec=='351000':
        writelog("INSERTING 351000 RECORD")
        process_insert_table("CAIMS_MOU_DTLREC", MOU_DTLREC_tbl,MOU_DTLREC_DEFN_DICT)
        current_parent_rec=record_id
    
    elif current_parent_rec=='351500':
        writelog("INSERTING 351500 RECORD")
        process_insert_table("CAIMS_MOU_DTLREC", MOU_DTLREC_tbl,MOU_DTLREC_DEFN_DICT)
        current_parent_rec=record_id
    
    elif current_parent_rec=='352000':
        writelog("INSERTING 352000 RECORD")
        process_insert_table("CAIMS_MOU_DTLREC", MOU_DTLREC_tbl,MOU_DTLREC_DEFN_DICT)
        current_parent_rec=record_id
        
    elif current_parent_rec=='352500':
        writelog("INSERTING 352500 RECORD")
        process_insert_table("CAIMS_MOU_DTLREC", MOU_DTLREC_tbl, MOU_DTLREC_DEFN_DICT)
        current_parent_rec=record_id
        
    elif current_parent_rec=='352700':
        writelog("INSERTING 352700 RECORD")
        process_insert_table("CAIMS_MOU_STATPG", MOU_STATPG_tbl, MOU_STATPG_DEFN_DICT)
        current_parent_rec=record_id
    else:
        current_parent_rec=record_id


def set_flags(current_record_type):
    global parent_1010_rec
    global parent_3505_rec
    global parent_3520_rec
    global parent_3527_rec
    global parent_3905_rec    
    
    if current_record_type=='010100':        
        parent_3505_rec = False
        parent_3520_rec = False
        parent_3527_rec = False
        parent_3905_rec = False
    elif current_record_type=='350500':
        parent_1010_rec = False
        parent_3520_rec = False
        parent_3527_rec = False
        parent_3905_rec = False
    elif current_record_type=='352000':
        parent_1010_rec=False
        parent_3505_rec=False
        parent_3527_rec=False
        parent_3905_rec=False
    elif current_record_type=='352700':
        parent_1010_rec=False
        parent_3505_rec=False
        parent_3520_rec=False
        parent_3905_rec=False
    elif current_record_type=='390500':
        parent_1010_rec=False
        parent_3505_rec=False
        parent_3520_rec=False
        parent_3527_rec=False
    else:
        parent_1010_rec=False
        parent_3505_rec=False
        parent_3520_rec=False
        parent_3527_rec=False
        parent_3905_rec=False
    

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
        writelog("WARNING: INVALID FGRP - ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'] + ", FGRP="+current_TACCT_FGRP)
        parent_1010_rec=False
    elif current_abbd_rec_key in abbdLst:
        dup_ban_lock=True
        writelog("WARNING: DUPLICATE BAN - ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'])
        parent_1010_rec=False
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
        writelog("070500 RECORD")
    else:
        writelog("WARNING: INVALID 010100 CHILD - "+record_id)


def process_TYP3505_PARENT():
    global line
    global record_id
    global MOU_DTLREC_tbl    
    global curr_RE
    global parent_3505_rec
    global current_ZCIC
    global current_abbd_rec_key
    global current_parent_rec
         
    if line[176:177] != '0':
        count_record("350500_SKIPPED",False)
        parent_3505_rec=False        
    else:
        initialize_DTLREC_tbl()     
        #RE =    RID1  |   RE1   |   IND1  |   IND2  |   IND3  |   IND4  |   IND5  |   IND6  |  IND7   |IND8|  IND9  |  IND10  |   IND11 |   IND12 |  IND13;
        #RE = [227:229]|[118:120]|[179:180]|[123:124]|[178:179]|[180:181]|[120:121]|[221:222]|[177:178]|' '|[122:123]|[207:208]|[121:122]|[176:177]|[192:193];
        #QCIC
        curr_RE=line[227:229] + line[118:120] + line[179:180] + line[123:124] + line[178:179] + line[180:181] + line[120:121] + line[221:222] + line[177:178] + ' ' +line[122:123] + line[207:208] + line[121:122] + line[176:177] + line[192:193]        
        #Populate fields for RateRec and map for the 350501 recs to use
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
        #elif QCIC != ' ':
        #    MOU_RATEREC_tbl['CIC']=' '
        else:
            MOU_DTLREC_tbl['CIC']=current_ZCIC
            current_record_num=' '
        
        ##########  ISSUE
        # RCD=' '
        # RCDA/I6YMD=EDIT(RCD);
        # RCDB/YMD=RCDA;
        # RCDCC=RCDB;
        #   RCDCC is a date field but the value is always defauled to ' '
        #########  For now just always leave RCDCC blank/null
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
        writelog("Record - " + str(inputLineCnt) + " - 350501 - Has no 350500 parent")
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
    global curr_RE    
    global current_parent_rec
    global current_TACCT_FGRP
    
    #IND12 - line[95:96]
    if line[95:96] != '0':
        count_record("351000_SKIPPED",False)
    else:
        initialize_DTLREC_tbl()     
        #RE =    RID1  |  RE1  |   IND1  |  IND2 |  IND3 |  IND4 |  IND5 |  IND6 |  IND7 |  IND8 |  IND9 | IND10 |  IND11  | IND12 | IND13
        #RE = [227:229]|[82:84]|[115:116]|[85:86]|[86:87]|[87:88]|[88:89]|[94:95]|[90:91]|[91:92]|[92:93]|[93:94]|[190:191]|[95:96]|[96:97]
        curr_RE=line[227:229] + line[82:84] + line[115:116] + line[85:86] + line[86:87] + line[87:88] + line[88:89] + line[94:95] + line[90:91] + line[91:92] + line[92:93] + line[93:94] + line[190:191] + line[95:96] + line[96:97]
        #
        current_record_num=line[72:82]        
        if current_TACCT_FGRP == '1':
            current_record_num=line[72:82]
        else:            
            current_record_num=' '
        #        
        currTSTP=line[208:211]
        if currTSTP == '999':
            currTSTP=' '        
        #
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
        MOU_DTLREC_tbl['JRSDN_CD']=line[224:225]
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
          
        current_parent_rec="351000"

def process_TYP351500():
    global line
    global record_id
    global MOU_DTLREC_tbl    
    global curr_RE    
    global current_parent_rec
    global current_TACCT_FGRP
     
    #IND12 - line[93:94]
    if line[93:94] != '0':
        count_record("351500_SKIPPED",False)
    else:
        
        #RE =    RID1  |  RE1  |  IND1 |  IND2 |  IND3 |  IND4 |  IND5 |  IND6 |  IND7 |  IND8 |  IND9 | IND10 | IND11 | IND12 | IND13
        #RE = [227:229]|[82:84]|[84:85]|[85:86]|[86:87]|  ' '  |  ' '  |[89:90]|  ' '  |  ' '  |  ' '  |  ' '  |[92:93]|[93:94]|[94:95]

        curr_RE=line[227:229] + line[82:84] + line[84:85] + line[85:86] + line[86:87] + ' ' + ' ' + line[89:90] + ' ' + ' ' + ' ' + ' ' + line[92:93] + line[93:94] + line[94:95]
        #
        current_record_num=line[72:82]        
        if current_TACCT_FGRP == '1':
            current_record_num=line[72:82]
        else:            
            current_record_num=' '
        #
        #currTSTP=line[208:211]
        #if currTSTP == '999':
        #    currTSTP=' '
        currTSTP=' '
        #        
        MOU_DTLREC_tbl['CLLI']=line[61:72]
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
        MOU_DTLREC_tbl['CIC']=line[203:208]
        MOU_DTLREC_tbl['SP_TRAN_PCT']=currTSTP
        MOU_DTLREC_tbl['ST_LV_CC']=line[211:215]
        MOU_DTLREC_tbl['VOIP_USAGE_IND1']=line[215:216]
        MOU_DTLREC_tbl['EO_TANDIND']=line[223:224]
        MOU_DTLREC_tbl['JRSDN_CD']=line[224:225]
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
     
    #IND12 - [189:190]
    if line[189:190] != '0':
        count_record("352000_SKIPPED",False)
        parent_3520_rec=False
    else:
       
       
        #RE = RID1|RE1|IND1|IND2|IND3|IND4|IND5|IND6|IND7|IND8|IND9|IND10|IND11|IND12|IND13
        #  |   RID1  |   RE1   |   IND1  |   IND2  |   IND3  |   IND4  |   IND5  |   IND6  |   IND7  |   IND8  |   IND9  |   IND10 (352001 will populate it)   |  IND11  |  IND12  |  IND13   |
        #   [227:229]|[118:120]|[191:192]|[123:124]|[222:223]|[193:194]|[192:193]|[207:208]|[190:191]|[120:121]|[122:123]|   ' '                               |[121:122]|[189:190]|[205:206]|
        curr_RE=line[227:229] + line[118:120] + line[191:192] + line[123:124] + line[222:223] + line[193:194] + line[192:193] + line[207:208] + line[190:191] + line[120:121] + line[122:123] + ' ' + line[121:122] + line[189:190] + line[205:206]
        curr_3520_RE_part1=line[227:229] + line[118:120] + line[191:192] + line[123:124] + line[222:223] + line[193:194] + line[192:193] + line[207:208] + line[190:191] + line[120:121] + line[122:123]
        curr_3520_RE_part2=line[121:122] + line[189:190] + line[205:206]
        #
        current_record_num=line[108:118]                
        if current_TACCT_FGRP == '1':
            current_record_num=line[108:118]
        else:            
            current_record_num=' '
        #
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
    global current_TACCT_FGRP
    global parent_3520_rec
    global curr_3520_RE_part1
    global curr_3520_RE_part2
    
    if parent_3520_rec != True:
        count_record("352001_NO_PARENT",False)
        
    elif record_id=='352001':
        #['IND10']=line[106:107]
        curr_RE= curr_3520_RE_part1 + line[106:107] + curr_3520_RE_part2       
        #
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
        writelog("352001 record has " + MOU_DTLREC_tbl['INPUT_RECORDS'])
        
    elif record_id=='352002':
        MOU_DTLREC_tbl['EO_SW_ONR_ID']=line[84:88]
        MOU_DTLREC_tbl['TERM_CLLI']=line[88:99]        
        MOU_DTLREC_tbl['INPUT_RECORDS']+="*"+str(record_id)        
        writelog("352002 record has " + MOU_DTLREC_tbl['INPUT_RECORDS'])
        
    else:
        writelog("3520 Child record type not found")


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
          writelog("Record - " + str(inputLineCnt) + " - 352701 - Has no 350500 parent")
#
    elif record_id=='352701':
        MOU_STATPG_tbl['TDM_TRNI_SP']=line[121:122]
        MOU_STATPG_tbl['PCT_LO']=line[123:126]
        MOU_STATPG_tbl['EXP_PIU']=convertnumber(line[157:162],2)
        MOU_STATPG_tbl['INPUT_RECORDS']+="*"+str(record_id)
        MOU_STATPG_tbl
#
    elif record_id=='352702':
        #MOU_STATPG_tbl['PCT_INET_BD'] = IF VERSION_NBR GE 39 THEN (EDIT(APCT_TRAF)) * .01 ELSE 0.00;
        #MOU_STATPG_tbl['PRCNT_VOIP_USGE'] = IF VERSION_NBR GE 51 THEN (EDIT(APVU)) * .01 ELSE 0.00;
        MOU_STATPG_tbl['PCT_INET_BD'] = "0";
        MOU_STATPG_tbl['PRCNT_VOIP_USGE'] = "0";
        
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
        #MOU_STATPG_tbl['PRCNT_VOIP_USGE_P'] = IF VERSION_NBR GE 51 THEN (EDIT(APVUP)) * .01 ELSE 0.00;        
        MOU_STATPG_tbl['PRCNT_VOIP_USGE_C'] = "0"
        MOU_STATPG_tbl['PRCNT_VOIP_USGE_P'] = "0"     
        
        #MOU_STATPG_tbl['APVUC']=line[87:92]
        #MOU_STATPG_tbl['APVUP']=line[92:97]
        MOU_STATPG_tbl['INPUT_RECORDS']+="*"+str(record_id)
    else:
        writelog("3527 Child record type not found")

    

def initialize_DTLREC_tbl():    
    global MOU_DTLREC_tbl

    MOU_DTLREC_tbl['ACNA']=current_abbd_rec_key['ACNA']
    MOU_DTLREC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    MOU_DTLREC_tbl['BAN']=current_abbd_rec_key['BAN']

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



def initialize_RATEREC_tbl():
    global MOU_RATEREC_tbl
   
    MOU_RATEREC_tbl['ACNA']=current_abbd_rec_key['ACNA']
    MOU_RATEREC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    MOU_RATEREC_tbl['BAN']=current_abbd_rec_key['BAN']   
    MOU_RATEREC_tbl['CLLI']=''
    
    MOU_RATEREC_tbl['RE']=''
    MOU_RATEREC_tbl['CIC']=''
    MOU_RATEREC_tbl['JRSDN_CD']=''
    MOU_RATEREC_tbl['NAT_REG']=''
     
def  initialize_STATPG_tbl():
    global MOU_BCTFMOU_tbl
    global current_abbd_rec_key 
    
    MOU_STATPG_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    MOU_STATPG_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    MOU_STATPG_tbl['BAN']=current_abbd_rec_key['BAN']   
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
    
    MOU_BCTFMOU_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    MOU_BCTFMOU_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    MOU_BCTFMOU_tbl['BAN']=current_abbd_rec_key['BAN']

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
    
def format_date(datestring):
    dtSize=len(datestring)
    
    if dtSize ==5:
        #jdate conversion
        return "TO_DATE('"+datestring+"','YY-DDD')"
    elif dtSize==6:
        return "TO_DATE('"+datestring[4:6]+"-"+MONTH_DICT[datestring[2:4]]+"-"+"20"+datestring[:2]+"','DD-MON-YY')"  
    elif dtSize==8:
        return "TO_DATE('"+datestring[:4]+"-"+datestring[4:6]+"-"+"20"+datestring[6:2]+"','YYYY-MM-DD')"
 
 
    
def process_insert_table(tbl_name, tbl_rec,tbl_dic):
        
    firstPart="INSERT INTO "+tbl_name+" (" #add columns
    secondPart=" VALUES ("  #add values
    writelog("GETTING VALUES FOR INSERT ")
    for key, value in tbl_rec.items() :
        nulVal=False
        if str(value).rstrip(' ') == '':
            nulVal=True
#             "EMPTY VALUE"
    
        try:
            if tbl_dic[key] == 'STRING':
                firstPart+=key+","
                if nulVal:
                    secondPart+="NULL,"
                else:
                    secondPart+="'"+str(value).rstrip(' ')+"',"
            elif tbl_dic[key] =='DATETIME':
                firstPart+=key+","
                if nulVal:
                    secondPart+="NULL,"
                else:                    
                    secondPart+=format_date(value)+","
            elif tbl_dic[key] =='NUMBER':
                firstPart+=key+","
                "RULE: if null/blank number, then populate with 0."
                if nulVal:
                    secondPart+="0,"
                else:                    
                    secondPart+=str(value)+","                    
            else:
                print "ERROR:" +tbl_dic[key]
        except KeyError as e:
            writelog("WARNING: Column "+key+" not found in the "+tbl_name+" table.  Skipping.")
            writelog("KeyError:"+e.message)
    writelog("END GETTING VALUES FOR INSERT ")
    firstPart=firstPart.rstrip(',')+")"     
    secondPart=secondPart.rstrip(',') +")"      
     
    insCurs=con.cursor()
    insSQL=firstPart+secondPart
    writelog("START INSERT")
    try:
        insCurs.execute(insSQL) 
        con.commit()
        writelog("SUCCESSFUL INSERT INTO "+tbl_name+".")
    except cx_Oracle.DatabaseError , e:
        if ("%s" % e.message).startswith('ORA-00001:'):
            writelog("****** DUPLICATE INSERT INTO"+str(tbl_name)+"*****************")
            writelog("Insert SQL: "+str(insSQL))
        else:
            writelog("ERROR:"+str(e.message))
            writelog("SQL causing problem:"+insSQL)
    finally:
        insCurs.close()
        writelog("END INSERT")

def convertnumber(num, decimalPlaces) :
    global record_id    
#   0000022194F
#   0000000000{
#   00000000000
#    writelog("number in :"+str(num))
    isNegative=False
    lastdigit="0"
    lastStr=num[-1:]
    if lastStr in NEGATIVE_NUMS:
        isNegative=True
        lastdigit=DIGIT_DICT[str(lastStr)]
    else:
        isNegative=False
        lastdigit=DIGIT_DICT[str(lastStr)]
        
    newNum=num[:len(str(num))-1]+lastdigit
    
    if newNum.isdigit() and decimalPlaces==0:
        if str(newNum).lstrip('0')=='':
            return "0"
        else:
            return str(newNum).lstrip('0')
    
    if newNum.isdigit():        
        rightPart=str(newNum)[-decimalPlaces:]
        leftPart=str(newNum)[:len(str(newNum))-decimalPlaces]    
        if str(leftPart).lstrip('0')=='':
            leftPart="0."
        else:
            leftPart=str(leftPart).lstrip('0')+"."
            
        if isNegative==True:
            leftPart="-"+leftPart
        
        return leftPart+rightPart
    else:
        process_ERROR_END("ERROR: Cant Convert Number: "+str(num) +"   from line:"+str(line))



def getTableColumns(tablenm):             
    myCurs=con.cursor()
    myTbl=tablenm
    mySQL="select * FROM %s WHERE ROWNUM=1" % (myTbl)
    tmpArray=[]
    try:    
        myCurs.execute(mySQL)  
        for x in myCurs.description:
            tmpArray.append(x)
        con.commit()
    except cx_Oracle.DatabaseError, e:
        if ("%s" % e.message).startswith('ORA-:'):
            writelog("ERROR:"+str(exc.message))
            writelog("SQL causing problem:"+updateSQL)
    finally:
        myCurs.close()
        
    return tmpArray  
 
 
 
def createTableTypeDict(tablenm):               
    colTypDict= {}        
    myArray=[]
    myArray=getTableColumns(tablenm)
     
    for y in myArray:
        colTypDict[y[0]]=str(y[1]).replace("<type 'cx_Oracle.",'').replace("'>",'') 

    return colTypDict  
    
"########################################################################################################################"
"####################################TRANSLATION MODULES#################################################################"

def translate_TACCNT_FGRP(taccnt,tfgrp):        
    if taccnt.rstrip(' ')=='S':
        if tfgrp.rstrip(' ') == 'A':
            return '1'
        elif tfgrp.rstrip(' ') == 'B':
            return '2'
        elif tfgrp.rstrip(' ') == 'C':
            return '3'  
        elif tfgrp.rstrip(' ') == 'D':
            return '4'            
        elif tfgrp.rstrip(' ') == 'E':
            return '5'            
        elif tfgrp.rstrip(' ') == 'G':
            return '6'            
        elif tfgrp.rstrip(' ') == 'L':
            return '7'            
        elif tfgrp.rstrip(' ') == 'Q':
            return '8'            
        elif tfgrp.rstrip(' ') == 'S':
            return '9'            
        elif tfgrp.rstrip(' ') == 'T':
            return 'Z'            
        elif tfgrp.rstrip(' ') == 'V':
            return 'Y'            
        else:
            return taccnt.rstrip(' ')
    else:
        return tfgrp.rstrip(' ')
#  TACCNT_FGRP=IF TACCNT EQ 'S' THEN DECODE TFGRP(
#              'A' '1' 'B' '2' 'C' '3' 'D' '4' 'E' '5' 'G' '6'
#              'L' '7' 'Q' '8' 'S' '9' 'T' 'Z' 'V' 'Y' ELSE '$') ELSE
#              TACCNT;    
#   
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
    writelog("\n")
    
    idCnt=0
    writelog("**")
    writelog("**Processed record IDs and counts**")
    writelog("record_id, count")
    
    keylist = record_counts.keys()
    keylist.sort()
    for key in keylist:
#        print "%s: %s" % (key, record_counts[key])   
        writelog(str(key)+", "+str(record_counts[key]))   
        idCnt+=record_counts[key]       
#    for key, value in record_counts.items():
#        writelog(str(key)+", "+str(value))   
#        idCnt+=value
    writelog("\n  Total count: "+str(idCnt))
    writelog(" ")
    writelog("**")
    unkCnt=0
    writelog( "There are "+str(len(unknown_record_counts))+" different record ID types not processed: ")    
    writelog("**UNKNOWN record IDs and counts**")
    writelog("record_id, count")

    keylist = unknown_record_counts.keys()
    keylist.sort()
    for key in keylist:
#        print "%s: %s" % (key, record_counts[key])   
        writelog(str(key)+", "+str(unknown_record_counts[key]))   
        unkCnt+=unknown_record_counts[key]  
    
    
#    for key, value in unknown_record_counts.items():
#        writelog(str(key)+", "+str(value))
#        unkCnt+=value
    writelog("\n  Total count: "+str(unkCnt))    
    writelog("**")    
    
    writelog("TOTAL ACNA/EOB_DATE/BAN's processed:"+str(BDT_KEY_cnt))
    writelog(" ")
    writelog("Total input records read from input file:"+str(idCnt+unkCnt))
    writelog(" ")    
 
    
def writelog(msg):
    global bdt_BCTFMOU_log
    logger.info(msg)
    
    bdt_BCTFMOU_log.write("\n"+msg)


def process_ERROR_END(msg):
    writelog("ERROR:"+msg)
    debug("ERROR:"+msg)
    process_close_files()
    raise "ERROR:"+msg
    
    
def process_close_files():
    global bdt_input
    global bdt_BCTFMOU_log
    
    if debugOn:
        debug_log.close()
        
    bdt_input.close();
    bdt_BCTFMOU_log.close()
   
    
    
def endProg(msg):
    global hdlr
 
    con.commit()
    con.close()
    
    process_write_program_stats()
     
    endTM=datetime.datetime.now()
    print "\nTotal run time:  %s" % (endTM-startTM)
    writelog("\nTotal run time:  %s" % (endTM-startTM))
     

    writelog("\n"+msg)
     
    process_close_files()
    
    hdlr.close()
    logger.removeHandler(hdlr)



"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
"BEGIN Program logic"
"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


print "Starting Program"
init()
main()
endProg("-END OF PROGRAM-")