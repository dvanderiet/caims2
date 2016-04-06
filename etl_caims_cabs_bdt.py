# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
PROGRAM NAME:     etl_caims_cabs_bdt.py                                     
LOCATION:                      
PROGRAMMER(S):    Dan VandeRiet                                 
DESCRIPTION:      CAIMS Extract/Transformation/Load program for Bill Data Tape
                  (BDT) records.
                  
REPLACES:         Legacy CTL program BC100FT0 - LOAD BCCBBIL FOCUS DATABASE.
                                                                         
LANGUAGE/VERSION: Python/2.7.10                                          
INITIATION DATE:  x/x/2016                                                
INPUT FILE(S):    PBCL.CY.XRU0102O.CABS.G0358V00.txt (build from GDG)
LOCATION:         MARION MAINFRAME           
                                                                         
OUTPUT:           ORACLE DWBS001P                                
                  Table names CAIMS_BDT_*
                                                                         
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
startTM=datetime.datetime.now();
import cx_Oracle
import sys
import ConfigParser
import platform
import os

###IMPORT COMMON/SHARED UTILITIES
from etl_caims_cabs_utility import  process_insert_table, process_update_table, writelog, convertnumber, \
                                   createTableTypeDict,translate_TACCNT_FGRP ,process_check_exists  

settings = ConfigParser.ConfigParser();
settings.read('settings.ini')
settings.sections()

"SET ORACLE CONNECTION"
con=cx_Oracle.connect(settings.get('OracleSettings','OraCAIMSUser'),settings.get('OracleSettings','OraCAIMSPw'),settings.get('OracleSettings','OraCAIMSSvc'))
schema=settings.get('OracleSettings','OraInsertSchema')
    
"CONSTANTS"
#Set Debug Trace Below - Set to trun to turn on
#DEBUGISON=False

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
    inputPath=settings.get('BDTSettings','WINDOWS_BDT_inDir') 
else:
    inputPath=settings.get('BDTSettings','LINUX_BDT_inDir') 

IP_FILENM_AND_PATH =os.path.join(inputPath,fileNm)

if os.path.isfile(IP_FILENM_AND_PATH):
    print ("Input file:"+IP_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+IP_FILENM_AND_PATH)

"GLOBAL VARIABLES"     
#record_id='123456'
badKey=False
current_abbd_rec_key={'ACNA':'x','EOB_DATE':'1','BAN':'x'}
prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}   #EOB_DATE,ACNA,BAN key   
 
record_counts={}
unknown_record_counts={}
BILL_REC='10' 
STAR_LINE='*********************************************************************************************'  

#http://www.3480-3590-data-conversion.com/article-signed-fields.html


   
"TRANSLATERS"
#
#def debug(msg):
#    if DEBUGISON:
#        DEBUG_LOG.write("\n"+str(msg))
        
    
def init():
    #debug("****** procedure==>  "+whereami()+" ******")
    
    global bdt_input 
    global record_id,output_log
 
    "OPEN FILES"
    "   CABS INPUT FILE"
    bdt_input = open(IP_FILENM_AND_PATH, "r")
    
    

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
    log_file=os.path.join(OUTPUT_DIR,"BDT_Cycle"+str(cycl_yy)+str(cycl_mmdd)+str(cycl_time)+"_"+startTM.strftime("%Y%m%d_@%H%M") +".txt")
         
    output_log=open(log_file, "w");
    output_log.write("-BDT CAIMS ETL PROCESS-")
    
    if record_id not in settings.get('BDTSettings','BDTHDR'):
        process_ERROR_END("The first record in the input file was not a "+settings.get('BDTSettings','BDTHDR').rstrip(' ')+" record.")

    writelog("Process "+sys.argv[0],output_log)
    writelog("   started execution at: " + str(startTM),output_log)
    writelog(STAR_LINE,output_log)
    writelog(" ",output_log)
    writelog("Input file: "+str(bdt_input),output_log)
    writelog("Log file: "+str(output_log),output_log)
    
    "Write header record informatio only"
    writelog("Header Record Info:",output_log)
    writelog("     Record ID: "+record_id+" YY: "+cycl_yy+", MMDD: "+cycl_mmdd+", TIME: "+str(cycl_time),output_log)
    
    count_record(record_id,False)
    del headerLine,cycl_yy,cycl_mmdd,cycl_time
    
def main():
    #BDT_config.initialize_BDT() 
    global record_type
    global line,output_log
    global record_counts, unknown_record_counts
    global bccbbil_id
    global crnt1_id
    global crnt2_id
    global baldtl_id
    global baldtl_prevKey
    global baldue_id
    global adjmtdtl_id
    global pmntadj_id
    global swsplchg_id
    global stlvcc
    "Counters"
    
    global inputLineCnt, BDT_KEY_cnt

    "TABLE Dictionaries - for each segment"
    global BDT_BCCBBIL_tbl,  BDT_BCCBBIL_DEFN_DICT
    global BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT
    global BDT_CRNT1_tbl, BDT_CRNT1_DEFN_DICT
    global BDT_CRNT2_tbl, BDT_CRNT2_DEFN_DICT
    global BDT_SWSPLCHG_tbl, BDT_SWSPLCHG_DEFN_DICT
    global BDT_PMNTADJ_tbl,BDT_PMNTADJ_DEFN_DICT
    global BDT_ADJMTDTL_tbl,BDT_ADJMTDTL_DEFN_DICT
    
    "text files"
    global bdt_input
    
    
    global firstBillingRec
    firstBillingRec=True
    
    global record_id
    global prev_record_id
    global current_abbd_rec_key 
    global prev_abbd_rec_key   
    global goodKey      
    
#DECLARE TABLE ARRAYS AND DICTIONARIES
    bccbbil_id=0   
    baldue_id=0
    BDT_BCCBBIL_tbl=dict() 
    BDT_BCCBBIL_DEFN_DICT=createTableTypeDict('CAIMS_BDT_BCCBBIL',con,schema,output_log)
    
    baldtl_id=0
    baldtl_prevKey=''
    BDT_BALDTL_tbl=dict()
    BDT_BALDTL_DEFN_DICT=createTableTypeDict('CAIMS_BDT_BALDTL',con,schema,output_log)
    
    crnt1_id=0    
    BDT_CRNT1_tbl=dict()
    BDT_CRNT1_DEFN_DICT=createTableTypeDict('CAIMS_BDT_CRNT1',con,schema,output_log)  

    crnt2_id=0
    BDT_CRNT2_tbl=dict()
    BDT_CRNT2_DEFN_DICT=createTableTypeDict('CAIMS_BDT_CRNT2',con,schema,output_log)
    
    swsplchg_id=0 
    BDT_SWSPLCHG_tbl=dict()
    BDT_SWSPLCHG_DEFN_DICT=createTableTypeDict('CAIMS_BDT_SWSPLCHG',con,schema,output_log)       

    pmntadj_id=0
    BDT_PMNTADJ_tbl=dict()
    BDT_PMNTADJ_DEFN_DICT=createTableTypeDict('CAIMS_BDT_PMNTADJ',con,schema,output_log)        

    adjmtdtl_id=0
    BDT_ADJMTDTL_tbl=dict()
    BDT_ADJMTDTL_DEFN_DICT=createTableTypeDict('CAIMS_BDT_ADJMTDTL',con,schema,output_log)         
         

    "COUNTERS"
    inputLineCnt=0
    BDT_KEY_cnt=0
    status_cnt=0
    "KEY"
    
    prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}
    
    "LOOP THROUGH INPUT CABS TEXT FILE"
    #note variables in for line appear to be global
    for line in bdt_input:
      
        "Count each line"
        inputLineCnt += 1
        status_cnt+=1
        if status_cnt>999:
            print str(inputLineCnt)+" lines completed processing...."
            status_cnt=0
        record_type=line[:2]
               
        
        if len(line) > 231:
            record_id=line[225:231]
        else:
            record_id=line[:6]

        
        "Process by record_id"
     
        "Header rec (first rec) processed in init()"
          
             
        if record_type == BILL_REC:
            "START-MAIN PROCESS LOOP"
            "GET KEY OF NEXT RECORD"
            current_abbd_rec_key=process_getkey()
            if badKey:
                count_record("BAD_ABD_KEY",True)
                writelog("WARNING: BAD INPUT DATA.  ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'],output_log)
            else:
                if current_abbd_rec_key != prev_abbd_rec_key:
                    BDT_KEY_cnt+=1
                    reset_record_flags()
                    
                process_bill_records()
   
            "set previous key for comparison in next iteration"   
            prev_abbd_rec_key=current_abbd_rec_key
            
        elif record_id in settings.get('BDTSettings','BDTFTR'):
            "FOOTER RECORD"
            log_footer_rec_info()     
            
        else:
            "ERROR:Unidentified Record"
            writelog("ERROR: Not sure what type of record this is:",output_log)
            writelog(line,output_log)
         
         
        prev_record_id=record_id
        "END-MAIN PROCESS LOOP"    
    #END of For Loop
       
 
###############################################main end#########################################################


def log_footer_rec_info(): 
    global record_id,output_log
    
    BAN_cnt=line[6:12]
    REC_cnt=line[13:22]
    writelog(" ",output_log)
    writelog("Footer Record Info:",output_log)
    writelog("     Record ID "+record_id+" BAN Count: "+BAN_cnt+" RECORD CNT: "+REC_cnt,output_log)
    writelog("The total number of lines counted in input file is : "+ str(inputLineCnt),output_log)
    writelog(" ",output_log)
    
    count_record(record_id,False)

#         ROOTREC
# 01      SH3
#**************
#*EOB_DATE    **I
#*ACNA        **I
#*BAN         **I
#*BILL_DATE   **I
#*            **
#***************
# **************
#       I
#       +-----------------+
#       I                 I
#       I CUSTSEG         I BALDUE
# 02    I KU        04    I S3
#..............    **************
#:BAN         :K   *INVDATECC   **I
#:CUST_GROUP  :    *ASTATE      **
#:MKT_ID      :    *PREVBAL     **
#:SPLRID      :    *CURR_INVOICE**
#:            :    *            **
#:............:    ***************
# JOINEDI BCCBCACB  **************
#       I                 I
#       I                 +-----------------+
#       I                 I                 I
#       I CUSTSEG2        I SWSPLCHG        I BALDTL
# 03    I KU        05    I S7        06    I S2
#..............    **************    **************
#:CUST_GROUP  :K   *STATE_IND   **   *DINVDATECC  **
#:CUST_GRP_NAM:    *ST_LVLCC    **   *DSTATE      **
#:BUS_OFC_ID  :    *CHGFROMDTCC **   *DTL_INVOICE **
#:            :    *CHGTHRUDTCC **   *DPREVBAL    **
#:            :    *            **   *            **
#:............:    ***************   ***************
# JOINED  BCTFCACG  **************    **************
#                                           I
#       +-----------------------------------+-----------------+
#       I                                   I                 I
#       I DISPDTL                           I PMNTADJ         I CRNT1
# 07    I S2                          08    I S4        10    I S3
#**************                      **************    **************
#*STLVL_CC    **                     *AINV_REF    **   *INVDT1CC    **
#*DSP_AUDIT_NO**                     *AINVDATECC  **   *SUBSTATE    **
#*DSPDATECC   **                     *APSTATE     **   *STLVCC      **
#*DSP_STATUS  **                     *APSTLVCC    **   *LPC         **
#*            **                     *            **   *            **
#***************                     ***************   ***************
# **************                      **************    **************
#                                           I                 I
#                                           I                 I
#                                           I                 I
#                                           I ADJMTDTL        I CRNT2
#                                     09    I S1        11    I S3
#                                    **************    **************
#                                    *ADJMT_SER_NO**   *INVDT2CC    **
#                                    *CKT_ID      **   *SUBST2      **
#                                    *CKT_LOC     **   *STLVCC2     **
#                                    *USOC_NAME   **   *TOT_OCC     **
#                                    *            **   *            **
#                                    ***************   ***************
#                                     **************    **************
#                                                             I
#                                                             I
#                                                             I
#                                                             I CRNTSCHG
#                                                       12    I S0
#                                                      **************
#                                                      *SCHG_IND    **
#                                                      *SCHG_JUR    **
#                                                      *SCHG_PHRASE **
#                                                      *SCHG_MONTHLY**
#                                                      *            **
#                                                      ***************
#                                                       **************


def process_bill_records():
    global record_id,output_log
    global firstBillingRec
    global tstx, verno, tsty
   
    
    tstx=line[77:79]
    verno=line[82:84]
    
    if firstBillingRec:
        "FIRST RECORD ONLY"
        #write BOS version number to log
        writelog("**--------------------------**",output_log)
        writelog("** BOS VERSION NUMBER IS "+verno+" ** ",output_log)
        writelog("**--------------------------**",output_log)
        firstBillingRec=False
        
    unknownRecord=False
    #BCCBBIL/ROOT/BALDUE always have 010100, 050500, and 051000    
    if record_id == '010100':
        process_TYP0101_ROOT()
    elif record_id == '050500':
        process_TYP0505_BO_CODE()   
    elif record_id == '051000':
        process_TYP0510_BALDUE() 
    
    elif record_id in ('051200','055000') and tstx != 'XX':     
        process_TYP0512_CRNT1()
    elif record_id in('051300','055100') and tstx != 'XX':
        process_TYP0513_CRNT2() 
    elif record_id == '051301' and tstx != 'XX':     
        process_TYP05131_CRNT2()  
        
    elif record_id in ('051400','051500','051600','052500','055200','055300','055400','055800','052300','055600','052400','055700'):  
        process_TYP0514_SWSPLCHG()

    elif record_id == '150500':
        process_TYP1505_PMNTADJ() 

    elif record_id == '200500':
        process_TYP2005_PMNTADJ()
    elif record_id == '200501':
        process_TYP20051_ADJMTDTL()
        
    elif record_id =='250500':
        process_TYP2505_BALDTL()
    
    elif record_id =='271500':
        process_TYP2715_BALDTL()
        
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
     
    
def reset_record_flags():
    "These flags are used to check if a record already exists for the current acna/ban/eob_date"
    "The first time a record is processed, if the count == 0, then an insert will be performed,"
    "else an update."
    global bccbbil_id
    global crnt1_id
    global crnt2_id
    global baldtl_id
    global adjmtdtl_id
    global pmntadj_id
    global swsplchg_id
    global baldtl_prevKey
    global stlvcc
    
    bccbbil_id=0
    crnt1_id=0
    crnt2_id=0
    baldtl_id=0
    adjmtdtl_id=0
    pmntadj_id=0
    swsplchg_id=0
    baldtl_prevKey=''
    stlvcc=''
    
    
def process_TYP0101_ROOT():
    global firstBillingRec,output_log
    global verno
    global bccbbil_id
#    global BDT_BCCBBIL_tbl,  BDT_BCCBBIL_DEFN_DICT  
    global  record_id
####COMPUTE####   
#    global banlock      
#    banlock = 'N';
####COMPUTE####       
    
    
    initialize_tbl('BDT_BCCBBIL_tbl')
 
#FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X6 X20 X10
#        [71:76] [76:82] [82:84]       [84:97] [97:98] [98:101] [101:105] [105:106]
#FIXFORM JDATE/5 X6      VERSION_NBR/2 X13     TLF/1   NLATA/3  X4        HOLD_BILL/1
#        [106:107] [107:108] [108:109] [109:218] [218:219]
#FIXFORM X1        TACCNT/A1 TFGRP/A1  X109      MAN_BAN_TYPE/1
#        [219:220]  [220:224]  [224:225]
#FIXFORM UNB_SWA_PROV/1 X4     MPB/A1
 
#    BDT_BCCBBIL_tbl['BANLOCK']='N'    #defaulted db value
#    BDT_BCCBBIL_tbl['VERSION_NBR']=line[82:84] NOT IN mfd

    BDT_BCCBBIL_tbl['TLF']=line[97:98]
    BDT_BCCBBIL_tbl['NLATA']=line[98:101]
    BDT_BCCBBIL_tbl['HOLD_BILL']=line[105:106]
   
#                                                                 TACCNT,TFGRP
    BDT_BCCBBIL_tbl['TACCNT_FGRP']=translate_TACCNT_FGRP(line[107:108],line[108:109])  
    BDT_BCCBBIL_tbl['MAN_BAN_TYPE']=line[218:219]
    BDT_BCCBBIL_tbl['UNB_SWA_PROV']=line[219:220]
    BDT_BCCBBIL_tbl['MPB']=line[224:225]
    BDT_BCCBBIL_tbl['BILL_DATE']=line[71:76]  #jdate
#    BDT_BCCBBIL_tbl['EOBDATEA']=BDT_BCCBBIL_tbl['EOB_DATE'] no tin mfd
    BDT_BCCBBIL_tbl['EOBDATECC']=BDT_BCCBBIL_tbl['EOB_DATE']
    BDT_BCCBBIL_tbl['BILLDATECC']=BDT_BCCBBIL_tbl['BILL_DATE']
    BDT_BCCBBIL_tbl['CAIMS_REL']='B'
    BDT_BCCBBIL_tbl['INPUT_RECORDS']=str(record_id)
    #NOTE did a record count and BCCBBIL/BALDUE always have an 010100, 050500, and 05100 records
    #so not doing an insert here... defer the inser to the 3rd record(051000)
####COMPUTE####    
#  taccnt_fgrp= BDT_BCCBBIL_tbl['TACCNT_FGRP']
#  bill_date=[71:76] 
#  eobdate=BDT_BCCBBIL_tbl['EOB_DATE'] 
#  eobdatecc=eobdate
#  bildatecc=bill_date
#  CAIMS_REL='B';
####COMPUTE####    
def process_TYP0505_BO_CODE():
    global bccbbil_id
    "BO_CODE"
    global  record_id,output_log
  
    BDT_BCCBBIL_tbl['BO_CODE']=line[211:215].rstrip(' ').lstrip(' ')
    BDT_BCCBBIL_tbl['INPUT_RECORDS']=str(record_id)
    
    #NOTE did a record count and BCCBBIL/BALDUE always have an 010100, 050500, and 05100 records
    #so not doing an insert here... defer the inser to the 3rd record(051000)

def process_TYP0510_BALDUE():
    "BALDUE"   
    global output_log
    global  record_id
    global bccbbil_id
    global baldtl_id

    #CURR_INVOICE
#    BDT_BCCBBIL_tbl['REF_NUM']=line[79:89]   NOT IN MFD
    
#                                            [37:39]    [39:79]
#FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X7 TASTATE/A2 X4 X6 X12 X18
#        [79:89]    [89:94]     [94:97]
#FIXFORM REF_NO/A10 INV_DATE/A5 X1 X2
#        [97:108]      [108:119]    [119:130]  [130:141]   [141:152] 
#FIXFORM PREVBAL/Z11.2 PAYMNT/Z11.2 ADJT/Z11.2 ADJIR/Z11.2 ADJIA/Z11.2
#        [152:163]   [163:174] [174:178] [178:189]
#FIXFORM ADJUS/Z11.2 BAL/Z11.2 X4        ADLOC/Z11.2
    
      
    #populate astate
    if line[37:39] == '  ':
        BDT_BCCBBIL_tbl['ASTATE']='XX'
    else:
        BDT_BCCBBIL_tbl['ASTATE']=line[37:39] 
    
    BDT_BCCBBIL_tbl['INVDATECC']=line[89:94]    
    BDT_BCCBBIL_tbl['PREVBAL']=convertnumber(line[97:108],2)
    BDT_BCCBBIL_tbl['CURR_INVOICE']=line[79:89].rstrip(' ')+line[89:94].rstrip(' ')
    BDT_BCCBBIL_tbl['PAYMNT']=convertnumber(line[108:119],2)
    BDT_BCCBBIL_tbl['ADJT']=convertnumber(line[119:130],2)
    BDT_BCCBBIL_tbl['ADJIR']=convertnumber(line[130:141],2)
    BDT_BCCBBIL_tbl['ADJIA']=convertnumber(line[141:152],2)
    BDT_BCCBBIL_tbl['ADJUS']=convertnumber(line[152:163],2)
    BDT_BCCBBIL_tbl['BAL']=convertnumber(line[163:174],2)
    BDT_BCCBBIL_tbl['LPC_TOT']=0	
    BDT_BCCBBIL_tbl['LPC_TOT_IR']=0
    BDT_BCCBBIL_tbl['LPC_TOT_IA']=0
    BDT_BCCBBIL_tbl['LPC_TOT_ND']=0
    BDT_BCCBBIL_tbl['ADLOC']=convertnumber(line[178:189],2) 
    BDT_BCCBBIL_tbl['INPUT_RECORDS']=str(record_id)
####COMPUTE####
#    curr_invoie = REF_NO|INV_DATE;
#    astate = IF TASTATE EQ '  ' THEN 'XX' ELSE TASTATE;
#    invdate0=EDIT(INV_DATE);
#    invdatea=GREGDT (INVDATE0, 'I6');
#    invdateb=INVDATEA;
#    invdatecc=INVDATEB;
####COMPUTE####        
    #NEW CODE TO FIX DUPLICATE ERRORS
    tmpTblRec={}
    tmpTblRec=BDT_BCCBBIL_tbl
    if process_check_exists("CAIMS_BDT_BCCBBIL", tmpTblRec, BDT_BCCBBIL_DEFN_DICT,con,schema,output_log)>0:
        writelog("Root record already exists for "+BDT_BCCBBIL_tbl['ACNA']+",ban="+BDT_BCCBBIL_tbl['BAN']+", "+BDT_BCCBBIL_tbl['EOB_DATE'],output_log) 
    else:        
        bccbbil_id=process_insert_table("CAIMS_BDT_BCCBBIL", BDT_BCCBBIL_tbl,BDT_BCCBBIL_DEFN_DICT,con,schema,"SQ_BDT_BCCBBIL",output_log)
        writelog(str(record_id)+"BCCBBIL",output_log)
    #NEW CODE TO FIX DUPLICATE ERRORS
 
def process_TYP0512_CRNT1():    
    global output_log
    global crnt1_id
    global baldtl_id
    global bccbbil_id
    global baldtl_prevKey
    global stlvcc    
    
    if line[77:79] =='XX':
        pass
    else:

#   FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X2 X6 X6 X12
#            [61:71]    [71:76]   [76:77] [77:79]
#   FIXFORM REF_NUM/A10 INVDT1/A5 X1      SUBSTATE/A2
#   FIXFORM MONCHGFROMCC/8 MONCHGTHRUCC/8 DTINVDUECC/8 LPC/Z11.2
#   FIXFORM X22 TOT_MRC/Z11.2
#   FIXFORM MRCIR/Z11.2 MRCIA/Z11.2 X11 MRCLO/Z11.2 X11 X19 STLVCC/A4
      #                                               [47:57]
#FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X9 X8 ADJMT_SER_NO/10
#        [57:61] [61:71]      [71:76]  
####COMPUTE####
#    COMPUTE
#    INVDATECC=INVDATECC;      from root
#    ASTATE=ASTATE;            from root
#    DINVDATECC=DINVDATECC;    from baldtl
#    DSTATE=DSTATE;            from baldtl
#    DTL_INVOICE/A15=REF_NUM|INVDT1;
#    INVDT10/I5=EDIT(INVDT1);
#    INVDT1A/I6YMD=GREGDT (INVDT10, 'I6');
#    INVDT1B/YMD=INVDT1A;
#    INVDT1CC=INVDT1B;
####COMPUTE####        
        initialize_tbl('BDT_BALDTL_tbl')
#        baldtl_id=0
        BDT_BALDTL_tbl['BCCBBIL_ID']=bccbbil_id
        BDT_BALDTL_tbl['DINVDATECC']=BDT_BCCBBIL_tbl['INVDATECC'] 
#        if BDT_BCCBBIL_tbl['ASTATE'].rstrip(' ') == '':
#            BDT_BALDTL_tbl['DSTATE'] = 'nl'
#        else:
#            BDT_BALDTL_tbl['DSTATE']=BDT_BCCBBIL_tbl['ASTATE']  
        BDT_BALDTL_tbl['DSTATE']=line[77:79]
#                                        REF_NUM INVDT1
        BDT_BALDTL_tbl['DTL_INVOICE']= line[61:71].rstrip(' ')+line[71:76].rstrip(' ') 
        BDT_BALDTL_tbl['DPREVBAL']=0;
        BDT_BALDTL_tbl['DPAYMNT']=0;
#        BDT_BALDTL_tbl['DINV_REF']=' '   NOT IN MFD
        BDT_BALDTL_tbl['DADJT']=0;
        BDT_BALDTL_tbl['DBAL']=0;
        BDT_BALDTL_tbl['LPC_APPLIED']=0;
        BDT_BALDTL_tbl['LPC_INV_IR']=0;
        BDT_BALDTL_tbl['LPC_INV_IA']=0;
        BDT_BALDTL_tbl['LPC_INV_ND']=0;
        BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id)
        tmpTblRec={}
        
        
###NEW        
        tmpTblRec['BCCBBIL_ID']=BDT_BALDTL_tbl['BCCBBIL_ID']
        tmpTblRec['DINVDATECC']=BDT_BALDTL_tbl['DINVDATECC']
        tmpTblRec['DSTATE']=BDT_BALDTL_tbl['DSTATE']
        if process_check_exists("CAIMS_BDT_BALDTL", tmpTblRec, BDT_BALDTL_DEFN_DICT,con,schema,output_log)>0:
            pass #ON MATCH CONTINUE
        else:
            #ON MATCH INCLUDE
            baldtl_id=process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,"SQ_BDT_BALDTL",output_log)
            writelog(str(record_id)+"     BALDTL",output_log)
###NEW
        
        
###OLD                
        
#        if baldtl_id > 0 and baldtl_prevKey == str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE'].rstrip(' ')): 
#            writelog("BALDTL keys are equal. ON MATCH CONTINUE",output_log)
#        else:
#
#            try:
#                if BDT_BALDTL_tbl['BCCBBIL_ID'] > 0 and str(BDT_BALDTL_tbl['DINVDATECC']).rstrip(' ') <> '' \
#                and  str(BDT_BALDTL_tbl['DSTATE'].rstrip(' ') <> ''):
#                    tmpTblRec['BCCBBIL_ID']=BDT_BALDTL_tbl['BCCBBIL_ID']
#                    tmpTblRec['DINVDATECC']=BDT_BALDTL_tbl['DINVDATECC']
#                    tmpTblRec['DSTATE']=BDT_BALDTL_tbl['DSTATE']
#                    if process_check_exists("CAIMS_BDT_BALDTL", tmpTblRec, BDT_BALDTL_DEFN_DICT,con,schema,output_log)>0:
#                        process_update_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,output_log)
#                    else:
#                        baldtl_id=process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,"SQ_BDT_BALDTL",output_log)
#                        baldtl_prevKey=str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE']).rstrip(' ')
#                        writelog(str(record_id)+"     BALDTL",output_log)
#                else:
#                    baldtl_id=process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,"SQ_BDT_BALDTL",output_log)
#                    baldtl_prevKey=str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE']).rstrip(' ')
#                    writelog(str(record_id)+"     BALDTL",output_log)
#            except KeyError:
#                writelog("ERROR: THIS SHOULD NEVER HAPPEN?", output_log)
####OLD                
        #POPULATE CRNT1
#   FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X2 X6 X6 X12
#   FIXFORM REF_NUM/A10 INVDT1/A5 X1 SUBSTATE/A2
#   FIXFORM MONCHGFROMCC/8 MONCHGTHRUCC/8 DTINVDUECC/8 LPC/Z11.2
#   FIXFORM X22 TOT_MRC/Z11.2
#   FIXFORM MRCIR/Z11.2 MRCIA/Z11.2 X11 MRCLO/Z11.2 X11 X19 STLVCC/A4

        BDT_CRNT1_tbl['BALDTL_ID']=baldtl_id 
#        BDT_CRNT1_tbl['REF_NUM']=line[61:71]   not on MDF
        BDT_CRNT1_tbl['INVDT1CC']=line[71:76]
        BDT_CRNT1_tbl['SUBSTATE']=line[77:79]
        BDT_CRNT1_tbl['MONCHGFROMCC']=line[79:87] 
        BDT_CRNT1_tbl['MONCHGTHRUCC']=line[87:95]  
        BDT_CRNT1_tbl['DTINVDUECC']=line[95:103] 
        BDT_CRNT1_tbl['LPC']=convertnumber(line[103:114],2)
        BDT_CRNT1_tbl['TOT_MRC']=convertnumber(line[136:147],2) 
        BDT_CRNT1_tbl['MRCIR']=convertnumber(line[147:158],2)
        BDT_CRNT1_tbl['MRCIA']=convertnumber(line[158:169],2)        
        if record_id == '051200':
            BDT_CRNT1_tbl['MRCLO']=convertnumber(line[180:191],2)
            BDT_CRNT1_tbl['STLVCC']=line[221:225] 
        elif record_id == '055000':
#                                            [30:61]
#   FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X2 X6 X6 X12
#           [61:71]     [71:76]   [76:77]   [77:79]
#   FIXFORM REF_NUM/A10 INVDT1/A5 X1        SUBSTATE/A2
#           [79:87]        [87:95]        [95:103]     [103:114]
#   FIXFORM MONCHGFROMCC/8 MONCHGTHRUCC/8 DTINVDUECC/8 LPC/Z11.2
#           [114:136] [136:147]
#   FIXFORM X22       TOT_MRC/Z11.2
#           [147:158]   [158: 169]  [169:180]   [180:217] [217:221]
#   FIXFORM MRCIR/Z11.2 MRCIA/Z11.2 MRCLO/Z11.2 X33 X4    STLVCC/A4
            BDT_CRNT1_tbl['MRCLO']=convertnumber(line[169:180],2)
            BDT_CRNT1_tbl['STLVCC']=line[217:221]         
        else:
            process_ERROR_END("ERROR: Expected record_id 051200 or 055000 but recieved a record_id of "+str(record_id))
        
        if BDT_CRNT1_tbl['STLVCC'].rstrip(' ') == '':
            BDT_CRNT1_tbl['STLVCC']='nl'
        BDT_CRNT1_tbl['INPUT_RECORDS']=str(record_id)
        
        stlvcc=BDT_CRNT1_tbl['STLVCC'] 
        
        if baldtl_id ==0:
            writelog("THIS IS WHERE WE INSERT CRNT1 with a 0 baldtl_id. record_id:" +record_id, output_log)
        crnt1_id=process_insert_table("CAIMS_BDT_CRNT1", BDT_CRNT1_tbl, BDT_CRNT1_DEFN_DICT,con,schema,"SQ_BDT_CRNT1",output_log)
        writelog(str(record_id)+"               CRNT1",output_log)
   
    
def process_TYP0513_CRNT2():    
    global output_log
    global crnt1_id
    global bccbbil_id
    global baldue_id
    global baldtl_id
    global crnt2_id
    global baldtl_prevKey
    #?not sure why record_id is recognized in this para when it is not declared global???    
    
    initialize_tbl('BDT_CRNT2_tbl')
    
    if crnt1_id == 0:
        writelog("ERROR???: Writing crnt2 record but missing parent crnt1 record.",output_log)
 
#REF_NUM/A10=' ';
#   FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X2 X6 X6 X12
#           [61:71]     [71:76]   [76:77]  [77:79]    
#   FIXFORM REF_NUM/A10 INVDT2/A5 X1       SUBSTATE2/A2
#           [79:90]       [90:101]    [101:112]   [112:123] 
#   FIXFORM TOT_OCC/Z11.2 OCCIR/Z11.2 OCCIA/Z11.2 OCCUS/Z11.2
#           [123:134]     [134:145]   [145:156]   [156:167] 
#   FIXFORM TOT_USG/Z11.2 USGIR/Z11.2 USGIA/Z11.2 TOT_TAX/Z11.2
#           [167:178]      [172:184] [184:195]        [195:200] [200:204]     
#   FIXFORM CURNTCHG/Z11.2 X12       TOT_SURCHG/Z11.2 X5        STLVCC2/A4
#   FIXFORM ON BCTFBDTI X105 OCCLO/Z11.2 USGLO/Z11.2
 

#######COMPUTE###############
#COMPUTE
# DTL_INVOICE/A15=REF_NUM|INVDT2;
# INVDATECC=INVDATECC;
# ASTATE=ASTATE;
# DINVDATECC=DINVDATECC;
# DSTATE=DSTATE;
# SUBSTATE=SUBSTATE;
# STLVCC=STLVCC;
# INVDT1CC=INVDT1CC;
# INVDT20/I5=EDIT(INVDT2);
# INVDT2A/I6YMD=GREGDT (INVDT20, 'I6');
# INVDT2B/YMD=INVDT2A;
# INVDT2CC=INVDT2B;
#######COMPUTE###############     
   
#    BDT_CRNT2_tbl
#    BUILD pieces here
    BDT_CRNT2_tbl['CRNT1_ID']=crnt1_id
#    BDT_CRNT2_tbl['REF_NUM']=line[61:71]   NOT ON MFD
    BDT_CRNT2_tbl['INVDT2CC']=line[71:76]
    BDT_CRNT2_tbl['SUBST2']=line[77:79]
    BDT_CRNT2_tbl['TOT_OCC']=convertnumber(line[79:90],2)
    BDT_CRNT2_tbl['OCCIR']=convertnumber(line[90:101],2)
    BDT_CRNT2_tbl['OCCIA']=convertnumber(line[101:112],2)
    BDT_CRNT2_tbl['OCCUS']=convertnumber(line[112:123],2)
    BDT_CRNT2_tbl['TOT_USG']=convertnumber(line[123:134],2)
    BDT_CRNT2_tbl['USGIR']=convertnumber(line[134:145],2)
    BDT_CRNT2_tbl['USGIA']=convertnumber(line[145:156],2)
    BDT_CRNT2_tbl['TOT_TAX']=convertnumber(line[156:167],2)
    BDT_CRNT2_tbl['CURNTCHG']=convertnumber(line[167:178],2)
    BDT_CRNT2_tbl['INPUT_RECORDS']=str(record_id)
    if record_id == '051300':
#       FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X2 X6 X6 X12
#       FIXFORM REF_NUM/A10 INVDT2/A5 X1 SUBSTATE2/A2
#       FIXFORM TOT_OCC/Z11.2 OCCIR/Z11.2 OCCIA/Z11.2 OCCUS/Z11.2
#       FIXFORM TOT_USG/Z11.2 USGIR/Z11.2 USGIA/Z11.2 TOT_TAX/Z11.2
#               [167:178]      [178:190]   [190:201]        [201:206] [206:210]    
#       FIXFORM CURNTCHG/Z11.2 X12         TOT_SURCHG/Z11.2 X5        SLVCC2/A4
#       FIXFORM ON BCTFBDTI X105 OCCLO/Z11.2 USGLO/Z11.2
        
        BDT_CRNT2_tbl['TOT_SURCHG']=convertnumber(line[190:201],2)
        BDT_CRNT2_tbl['STLVCC2']=line[206:210]
        "DONT UPDATE OR INSERT A RECORD, the 051300 should always be followed by a 051301 record"
        " to populate OCCLO and USGLO records, so return will go back to read another record"
        " and OCCLO and USGLO will be populated by the TYP05131 module"
    if record_id == '055100':
#   FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X2 X6 X6 X12
#   FIXFORM REF_NUM/A10 INVDT2/A5 X1 SUBSTATE2/A2
#   FIXFORM TOT_OCC/Z11.2 OCCIR/Z11.2 OCCIA/Z11.2 OCCUS/Z11.2
#   FIXFORM TOT_USG/Z11.2 USGIR/Z11.2 USGIA/Z11.2 TOT_TAX/Z11.2
#           [167:178]      [178:189]   [189:205] [205:209]   [209:220]
#   FIXFORM CURNTCHG/Z11.2 OCCLO/Z11.2 X16       STLVCC2/A4  USGLO/Z11.2
        
        
        BDT_CRNT2_tbl['OCCLO']=convertnumber(line[178:189],2)
        BDT_CRNT2_tbl['STLVCC2']=line[205:209]
        BDT_CRNT2_tbl['USGLO']=convertnumber(line[209:220],2)
        crnt2_id=process_insert_table("CAIMS_BDT_CRNT2", BDT_CRNT2_tbl, BDT_CRNT2_DEFN_DICT,con,schema,"SQ_BDT_CRNT2",output_log)
        writelog(str(record_id)+"                    CRNT2",output_log)

        initialize_tbl('BDT_BALDTL_tbl')
        BDT_BALDTL_tbl['BCCBBIL_ID']=bccbbil_id
        BDT_BALDTL_tbl['DINVDATECC']=BDT_CRNT2_tbl['INVDT2CC']
        if BDT_CRNT2_tbl['SUBST2'].rstrip(' ') == '':
            BDT_BALDTL_tbl['DSTATE'] = 'nl'
        else:
            BDT_BALDTL_tbl['DSTATE']=BDT_CRNT2_tbl['SUBST2']
#                                      REF_NUM INVDT2     
        BDT_BALDTL_tbl['DTL_INVOICE']=line[61:71].rstrip(' ')+line[71:76].rstrip(' ')
        
        BDT_BALDTL_tbl['DPREVBAL']=0;
        BDT_BALDTL_tbl['DPAYMNT']=0;
#        BDT_BALDTL_tbl['DINV_REF']=BDT_CRNT2_tbl['REF_NUM'] not on MDF
        BDT_BALDTL_tbl['DADJT']=0;
        BDT_BALDTL_tbl['DBAL']=0;
        BDT_BALDTL_tbl['LPC_APPLIED']=0;
        BDT_BALDTL_tbl['LPC_INV_IR']=0;
        BDT_BALDTL_tbl['LPC_INV_IA']=0;
        BDT_BALDTL_tbl['LPC_INV_ND']=0;
        BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id);

        if baldtl_prevKey == str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE'].rstrip(' ')):
            writelog("BALDTL keys are equal...avoiding a duplicate",output_log)
        else:

            try:
                tmpTblRec={}
                if baldtl_id > 0 and BDT_BALDTL_tbl['BCCBBIL_ID'] > 0 and str(BDT_BALDTL_tbl['DINVDATECC']).rstrip(' ') <> '' \
                and  str(BDT_BALDTL_tbl['DSTATE'].rstrip(' ') <> ''):
                    tmpTblRec['BCCBBIL_ID']=bccbbil_id
                    tmpTblRec['DINVDATECC']=BDT_BALDTL_tbl['DINVDATECC']
                    tmpTblRec['DSTATE']=BDT_BALDTL_tbl['DSTATE']
                    if process_check_exists("CAIMS_BDT_BALDTL", tmpTblRec, BDT_BALDTL_DEFN_DICT,con,schema,output_log)>0:
                        process_update_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,output_log)
                else:
                    baldtl_id=process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,"SQ_BDT_BALDTL",output_log)
                    baldtl_prevKey=str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE']).rstrip(' ')
                    writelog(str(record_id)+"     BALDTL",output_log)
            except KeyError:
                writelog("ERROR: THIS SHOULD NEVER HAPPEN?", output_log)

   
def process_TYP05131_CRNT2():    
    global output_log
    global crnt1_id
    global crnt2_id

    "Dont initialize.  Initialization was done in TYPE0513 para"
 
#   FIXFORM ON BCTFBDTI X105 OCCLO/Z11.2 USGLO/Z11.2
   
    BDT_CRNT2_tbl['OCCLO']=convertnumber(line[105:116],2)
    BDT_CRNT2_tbl['USGLO']=convertnumber(line[116:127],2)
    BDT_CRNT2_tbl['INPUT_RECORDS']+=","+str(record_id)
   
    
    if prev_record_id == '051300':
#        if crnt2_id>0:
#            writelog("ERROR: CRNT2 is "+str(crnt2_id)+", we shouldnt be writing another record",output_log)
        crnt2_id=process_insert_table("CAIMS_BDT_CRNT2", BDT_CRNT2_tbl, BDT_CRNT2_DEFN_DICT,con,schema,"SQ_BDT_CRNT2",output_log)
        writelog(str(record_id)+"                    CRNT2",output_log)
    else:
        process_ERROR_END("ERROR: Previous record should have been a 051300.")
                  
    
def process_TYP0514_SWSPLCHG():  
    global swsplchg_id 
    global output_log
    global bccbbil_id
    global baldue_id  
    
    initialize_tbl('BDT_SWSPLCHG_tbl')
  
    state_ind=line[76:78]
    if state_ind == 'XX':
        pass
    else:
        
        if bccbbil_id==0:
            writelog("ERROR???: Writing SWSPLCHG record but missing parent records.",output_log)
            
#   FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X2 X6 X6 X12
#           [61:76] [76:78]      [78:82]     [82:86] [86:94]        [94:102] 
#   FIXFORM X15     STATE_IND/A2 ST_LVLCC/A4 X4      CHGFROMDTCC/A8 CHGTHRUDTCC/A8
#           [102:103]    [103:104]    [104:115]   [115:126]            
#   FIXFORM MAC_ACCTYP/1 MAC_FACTYP/1 MACIR/Z11.2 MACIA/Z11.2
#           [126:137]     [137:148]     [148:159]
#   FIXFORM MACIRIA/Z11.2 MACIAIA/Z11.2 MACLOC/Z11.2
                
        BDT_SWSPLCHG_tbl['BCCBBIL_ID']=bccbbil_id
        BDT_SWSPLCHG_tbl['STATE_IND']=state_ind
        BDT_SWSPLCHG_tbl['ST_LVLCC']=line[78:82]
        BDT_SWSPLCHG_tbl['CHGFROMDTCC']=line[86:94]
        BDT_SWSPLCHG_tbl['CHGTHRUDTCC']=line[94:102] 
        
 

        if record_id in ('052500','055800','052300','052300','055600','052400','055700'):
            BDT_SWSPLCHG_tbl['MAC_ACCTYP']='0'
            BDT_SWSPLCHG_tbl['MAC_FACTYP']='0'
            BDT_SWSPLCHG_tbl['MACIR']=convertnumber(line[102:113],2)
            BDT_SWSPLCHG_tbl['MACIA']=convertnumber(line[113:124],2)
            BDT_SWSPLCHG_tbl['MACIRIA']=convertnumber(line[124:135],2)
            BDT_SWSPLCHG_tbl['MACIAIA']=convertnumber(line[135:146],2)
            BDT_SWSPLCHG_tbl['MACLOC']=convertnumber(line[146:157],2)

        else:        
            BDT_SWSPLCHG_tbl['MAC_ACCTYP']=line[102:103]
            BDT_SWSPLCHG_tbl['MAC_FACTYP']=line[103:104]
            BDT_SWSPLCHG_tbl['MACIR']=convertnumber(line[104:115],2)
            BDT_SWSPLCHG_tbl['MACIA']=convertnumber(line[115:126],2)
        #dflt MACND to 0 
        BDT_SWSPLCHG_tbl['MACND']=0 
        if record_id in ('051400','051600','055200'):
            BDT_SWSPLCHG_tbl['MACIRIA']=convertnumber(line[126:137],2)
            BDT_SWSPLCHG_tbl['MACIAIA']=convertnumber(line[137:148],2)
            BDT_SWSPLCHG_tbl['MACLOC']=convertnumber(line[148:159],2)
            
        if record_id in ('051500','055300'):
            
            BDT_SWSPLCHG_tbl['MACND']=convertnumber(line[126:137],0)
            if BDT_SWSPLCHG_tbl['MACND'].rstrip(' ') =='':
                BDT_SWSPLCHG_tbl['MACND']=0
            BDT_SWSPLCHG_tbl['MACIRIA']=convertnumber(line[137:148],2)
            BDT_SWSPLCHG_tbl['MACIAIA']=convertnumber(line[148:159],2)
            BDT_SWSPLCHG_tbl['MACLOC']=convertnumber(line[159:170],2)
        
        if record_id in ('051400','055200'):
            BDT_SWSPLCHG_tbl['MAC_RECTYP']=14
        elif record_id in ('051600','055400'):
            BDT_SWSPLCHG_tbl['MAC_RECTYP']=16
        elif record_id in ('051500','055300'):
            BDT_SWSPLCHG_tbl['MAC_RECTYP']=15
        elif record_id in ('052500','055800'):
            BDT_SWSPLCHG_tbl['MAC_RECTYP']=25
        elif record_id in ('052300','055600'):
            BDT_SWSPLCHG_tbl['MAC_RECTYP']=23
        elif record_id in ('052400','055700' ):
            BDT_SWSPLCHG_tbl['MAC_RECTYP']=24
    
        BDT_SWSPLCHG_tbl['INPUT_RECORDS']=str(record_id)
        swsplchg_id=process_insert_table("CAIMS_BDT_SWSPLCHG", BDT_SWSPLCHG_tbl, BDT_SWSPLCHG_DEFN_DICT,con,schema,"SQ_BDT_SWSPLCHG",output_log)
        writelog(str(record_id)+"     SWSPLCHG",output_log)
        
        
def process_TYP1505_PMNTADJ():
    global pmntadj_id
    global baldtl_id
    global record_id
    global output_log
    global baldtl_prevKey

#FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X2 X6 X18
#         [61:71]      [71:76]
#FIXFORM AINV_REF/A10 AINV_DATE/A5
#        [76:84]     [84:132]  [132:134]  [134:149]  
#FIXFORM DATERCVCC/8 X48       APSTATE/A2 X15
#        [149:160]
#FIXFORM AMOUNT/Z11.2
    initialize_tbl('BDT_PMNTADJ_tbl')
    
    if record_id == '150500':
        BDT_PMNTADJ_tbl['PORA']='P'
    if line[132:134].rstrip(' ') == '':
        BDT_PMNTADJ_tbl['APSTATE']='nl'
    else:
        BDT_PMNTADJ_tbl['APSTATE']=line[132:134]        
    BDT_PMNTADJ_tbl['AINVDATECC']=line[71:76]
    if line[61:71].rstrip(' ') == '':
        BDT_PMNTADJ_tbl['AINV_REF']='nl'
    else:
        BDT_PMNTADJ_tbl['AINV_REF']=line[61:71]
        
    BDT_PMNTADJ_tbl['APSTLVCC']='nl' 
    BDT_PMNTADJ_tbl['DATERCVCC']=line[76:84]
    BDT_PMNTADJ_tbl['AMOUNT']=convertnumber(line[149:160],2)
    BDT_PMNTADJ_tbl['INPUT_RECORDS']=str(record_id)


     
    initialize_tbl('BDT_BALDTL_tbl')
    BDT_BALDTL_tbl['BCCBBIL_ID']=bccbbil_id
    BDT_BALDTL_tbl['DINVDATECC']=BDT_PMNTADJ_tbl['AINVDATECC'] 
    if BDT_PMNTADJ_tbl['APSTATE'].rstrip(' ') == '':
        BDT_BALDTL_tbl['DSTATE'] = 'nl'
    else:
        BDT_BALDTL_tbl['DSTATE']=BDT_PMNTADJ_tbl['APSTATE']
        #                         AINV_REF AINV_DATE
    BDT_BALDTL_tbl['DTL_INVOICE']=line[61:71].rstrip(' ')+line[71:76].rstrip(' ')
    BDT_BALDTL_tbl['DPREVBAL']=0
    BDT_BALDTL_tbl['DPAYMNT']=0
#    BDT_BALDTL_tbl['DINV_REF']=BDT_PMNTADJ_tbl['AINV_REF'] no on MFD
    BDT_BALDTL_tbl['DADJT']=0
    BDT_BALDTL_tbl['DBAL']=0
    BDT_BALDTL_tbl['LPC_APPLIED']=0
    BDT_BALDTL_tbl['LPC_INV_IR']=0
    BDT_BALDTL_tbl['LPC_INV_IA']=0
    BDT_BALDTL_tbl['LPC_INV_ND']=0
    BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id)
    
  
    if baldtl_prevKey == str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE'].rstrip(' ')):
        writelog("BALDTL keys are equal...avoiding a duplicate",output_log)
    else:
        tmpTblRec={}
        try:
             if baldtl_id > 0 and BDT_BALDTL_tbl['BCCBBIL_ID'] > 0 and str(BDT_BALDTL_tbl['DINVDATECC']).rstrip(' ') <> '' \
                and  str(BDT_BALDTL_tbl['DSTATE'].rstrip(' ') <> ''):
                tmpTblRec['BCCBBIL_ID']=bccbbil_id
                tmpTblRec['DINVDATECC']=BDT_BALDTL_tbl['DINVDATECC']
                tmpTblRec['DSTATE']=BDT_BALDTL_tbl['DSTATE']
                if process_check_exists("CAIMS_BDT_BALDTL", tmpTblRec, BDT_BALDTL_DEFN_DICT,con,schema,output_log)>0:
                    process_update_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,output_log)
             else:
                baldtl_id=process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,"SQ_BDT_BALDTL",output_log)
                baldtl_prevKey=str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE']).rstrip(' ')
                writelog(str(record_id)+"     BALDTL",output_log)
        except KeyError:
            writelog("ERROR: THIS SHOULD NEVER HAPPEN?", output_log)
            
            
    BDT_PMNTADJ_tbl['BALDTL_ID']=baldtl_id
    pmntadj_id=process_insert_table("CAIMS_BDT_PMNTADJ", BDT_PMNTADJ_tbl, BDT_PMNTADJ_DEFN_DICT,con,schema,"SQ_BDT_PMNTADJ",output_log) 
    writelog(str(record_id)+"          PMNTADJ",output_log)

def process_TYP2005_PMNTADJ():                    
    "200500"
    global record_id
    global baldtl_id
    global pmntadj_id
    global bccbbil_id
    global baldtl_prevKey
    global dtl_invoice
 
#                                               [47:57]
#FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X9 X8 ADJMT_SER_NO/10
#        [57:61] [61:71]      [71:76]
#FIXFORM X4      AINV_REF/A10 AINV_DATE/A5
#        [76:84]     [84:87]     [87:134] [134:150]
#FIXFORM DATERCVCC/8 PHRASE_CD/3 X2 X45   AUDITNUM/16
#         [150:151]    [151:162]    [162:218] [218:220] [220:224]
#FIXFORM INTER_INTRA/1 AMOUNT/Z11.2 X45 X2 X9 APSTATE/2 APSTLVCC/A4 X7
# 
#???????????????????????????
#FIXFORM ON BCTFBDTI X91 ADJ_FACTYP/1 X3 PF_IND/1 X13
#FIXFORM ADJ_FRDTCC/8 ADJ_THRUDTCC/8 CKT_ID/A47 X39 ADJ_ACCTYP/1
#FIXFORM X2 BUS_RSDC_IND/A1 PF_BAND1/3 PF_BAND2/3
#???????????????????????????

    initialize_tbl('BDT_PMNTADJ_tbl')
    if line[218:220].rstrip(' ') == '':
        BDT_PMNTADJ_tbl['APSTATE']='nl'
    else:
        BDT_PMNTADJ_tbl['APSTATE']=line[218:220]
    BDT_PMNTADJ_tbl['AINVDATECC']=line[71:76]
    if line[61:71].rstrip(' ') == '':
        BDT_PMNTADJ_tbl['AINV_REF']='nl'
    else:
        BDT_PMNTADJ_tbl['AINV_REF']=line[61:71]
    
     
    initialize_tbl('BDT_BALDTL_tbl')
    BDT_BALDTL_tbl['BCCBBIL_ID']=bccbbil_id
    BDT_BALDTL_tbl['DINVDATECC']=line[71:76] 
    BDT_BALDTL_tbl['DSTATE']=BDT_PMNTADJ_tbl['APSTATE']
#                                                AINV_REF AINV_DATE
    dtl_invoice=line[61:71].rstrip(' ')+line[71:76].rstrip(' ')    
    BDT_BALDTL_tbl['DTL_INVOICE']=line[61:71].rstrip(' ')+line[71:76].rstrip(' ')   
    BDT_BALDTL_tbl['DPREVBAL']=0
    BDT_BALDTL_tbl['DPAYMNT']=0
#    BDT_BALDTL_tbl['DINV_REF']=BDT_PMNTADJ_tbl['AINV_REF'] no on MFD
    BDT_BALDTL_tbl['DADJT']=0
    BDT_BALDTL_tbl['DBAL']=0
    BDT_BALDTL_tbl['LPC_APPLIED']=0
    BDT_BALDTL_tbl['LPC_INV_IR']=0
    BDT_BALDTL_tbl['LPC_INV_IA']=0
    BDT_BALDTL_tbl['LPC_INV_ND']=0
    BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id)
    if baldtl_prevKey == str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE'].rstrip(' ')):
        writelog("BALDTL keys are equal...avoiding a duplicate",output_log)
    else:
        tmpTblRec={}
        try:
            if baldtl_id > 0 and BDT_BALDTL_tbl['BCCBBIL_ID'] > 0 and str(BDT_BALDTL_tbl['DINVDATECC']).rstrip(' ') <> '' \
            and  str(BDT_BALDTL_tbl['DSTATE'].rstrip(' ') <> ''):
                tmpTblRec['BCCBBIL_ID']=bccbbil_id
                tmpTblRec['DINVDATECC']=BDT_BALDTL_tbl['DINVDATECC']
                tmpTblRec['DSTATE']=BDT_BALDTL_tbl['DSTATE']
                if process_check_exists("CAIMS_BDT_BALDTL", tmpTblRec, BDT_BALDTL_DEFN_DICT,con,schema,output_log)>0:
                    process_update_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,output_log)
            else:
                baldtl_id=process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,"SQ_BDT_BALDTL",output_log)
                baldtl_prevKey=str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE']).rstrip(' ')
                writelog(str(record_id)+"     BALDTL",output_log)
        except KeyError:
            writelog("ERROR: THIS SHOULD NEVER HAPPEN?", output_log)

    
    BDT_PMNTADJ_tbl['BALDTL_ID']=baldtl_id
    BDT_PMNTADJ_tbl['PORA']='A'
    BDT_PMNTADJ_tbl['AINV_REF']=line[61:71]
    BDT_PMNTADJ_tbl['AINVDATECC']=line[71:76]
    BDT_PMNTADJ_tbl['DATERCVCC']=line[76:84]
    BDT_PMNTADJ_tbl['PHRASE_CD']=line[84:87]
    BDT_PMNTADJ_tbl['AUDITNUM']=line[134:150]
    BDT_PMNTADJ_tbl['INTER_INTRA']=line[150:151]
    BDT_PMNTADJ_tbl['AMOUNT']=convertnumber(line[151:162],2)
    if line[218:220].rstrip(' ') == '':
        BDT_PMNTADJ_tbl['APSTATE']='nl'
    else:
        BDT_PMNTADJ_tbl['APSTATE']=line[218:220]
    BDT_PMNTADJ_tbl['APSTLVCC']=line[220:224]
    if BDT_PMNTADJ_tbl['APSTLVCC'].rstrip('0').rstrip(' ') == '':
        BDT_PMNTADJ_tbl['APSTLVCC']='nl'
    BDT_PMNTADJ_tbl['INPUT_RECORDS']=str(record_id)

    
def process_TYP20051_ADJMTDTL():
    "200501"
    global record_id
    global output_log
    global baldtl_id
    global adjmtdtl_id
    global pmntadj_id
    global baldtl_prevKey
    global dtl_invoice
#COMPUTE
#   INVDATECC=INVDATECC;
#   ASTATE=ASTATE;
#   PORA/A1=IF REC_ID EQ '200500' THEN 'A';
#   DUP/A1=' ';
#                                               [47:57]
#FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X9 X8 ADJMT_SER_NO/10
#        [57:61]  [61:71]      [71:76]
#FIXFORM X4       AINV_REF/A10 AINV_DATE/A5
#        [76:84]     [84:87]     [87:89] [89:134] [134:150]
#FIXFORM DATERCVCC/8 PHRASE_CD/3 X2      X45      AUDITNUM/16
#        [150:151]     [151:162]    [162:218] [218:220] [220:224]    
#FIXFORM INTER_INTRA/1 AMOUNT/Z11.2 X45 X2 X9 APSTATE/2 APSTLVCC/A4 X7
# 
#??does this read a 200501??????
#                    [:91] [91:92]      [92:95] [95:96]  [96:109]
#FIXFORM ON BCTFBDTI X91   ADJ_FACTYP/1 X3      PF_IND/1 X13
#        [109:117]    [117:125]      [125:172]  [172:211] [211:212]
#FIXFORM ADJ_FRDTCC/8 ADJ_THRUDTCC/8 CKT_ID/A47 X39       ADJ_ACCTYP/1
#        [212:214]  [214:215]       [215:218]  [218:221]
#FIXFORM X2         BUS_RSDC_IND/A1 PF_BAND1/3 PF_BAND2/3
#COMPUTE
#AINVDATE0/I5=EDIT(AINV_DATE);
#AINVDATEA/I6YMD=GREGDT(AINVDATE0, 'I6');
#AINVDATEB/YMD=AINVDATEA;
#AINVDATECC=AINVDATEB;
#DINVDATECC=AINVDATECC;
#DTL_INVOICE/A15=AINV_REF|AINV_DATE;
#DSTATE=APSTATE;
 
    BDT_PMNTADJ_tbl['APSTLVCC']=line[220:224]
    if BDT_PMNTADJ_tbl['APSTLVCC'].rstrip('0').rstrip(' ') == '':
        BDT_PMNTADJ_tbl['APSTLVCC']='nl'
    BDT_PMNTADJ_tbl['ADJ_FACTYP']=line[91:92]
    BDT_PMNTADJ_tbl['ADJ_ACCTYP']=line[211:212]
    BDT_PMNTADJ_tbl['PF_IND']=line[95:96]
    BDT_PMNTADJ_tbl['PF_BAND1']=line[215:218]
    BDT_PMNTADJ_tbl['PF_BAND2']=line[218:221]
    BDT_PMNTADJ_tbl['ADJ_FRDTCC']=line[109:117]
    BDT_PMNTADJ_tbl['ADJ_THRUDTCC']=line[117:125]
    BDT_PMNTADJ_tbl['INPUT_RECORDS']+=','+str(record_id)
 

    
    BDT_ADJMTDTL_tbl['CKT_ID']=line[125:172]
    BDT_ADJMTDTL_tbl['BUS_RSDC_IND']=line[214:215]
    BDT_ADJMTDTL_tbl['USOC_AMT']=0
    BDT_ADJMTDTL_tbl['INPUT_RECORDS']=str(record_id)
   
    
    initialize_tbl('BDT_BALDTL_tbl')
    BDT_BALDTL_tbl['BCCBBIL_ID']=bccbbil_id
    BDT_BALDTL_tbl['DTL_INVOICE']=dtl_invoice # from 200500 record
    BDT_BALDTL_tbl['DINVDATECC']=BDT_PMNTADJ_tbl['AINVDATECC'] 
    if BDT_PMNTADJ_tbl['APSTATE'].rstrip(' ') == '':
        BDT_BALDTL_tbl['DSTATE'] = 'nl'
    else:
        BDT_BALDTL_tbl['DSTATE']=BDT_PMNTADJ_tbl['APSTATE']        
    
    
    BDT_BALDTL_tbl['DPREVBAL']=0
    BDT_BALDTL_tbl['DPAYMNT']=0
#    BDT_BALDTL_tbl['DINV_REF']=BDT_ADJMTDTL_tbl['AINV_REF'] not on MFD
    BDT_BALDTL_tbl['DADJT']=0
    BDT_BALDTL_tbl['DBAL']=0
    BDT_BALDTL_tbl['LPC_APPLIED']=0
    BDT_BALDTL_tbl['LPC_INV_IR']=0
    BDT_BALDTL_tbl['LPC_INV_IA']=0
    BDT_BALDTL_tbl['LPC_INV_ND']=0
    BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id)
    if baldtl_prevKey == str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE'].rstrip(' ')):
        writelog("BALDTL keys are equal...avoiding a duplicate",output_log)
    else:
        baldtl_id=process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,"SQ_BDT_BALDTL",output_log)
        baldtl_prevKey=str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE']).rstrip(' ')
        writelog(str(record_id)+"     BALDTL",output_log)
    
    
    BDT_PMNTADJ_tbl['BALDTL_ID']=baldtl_id
    pmntadj_id=process_insert_table("CAIMS_BDT_PMNTADJ", BDT_PMNTADJ_tbl, BDT_PMNTADJ_DEFN_DICT,con,schema,"SQ_BDT_PMNTADJ",output_log) 
    writelog(str(record_id)+"          PMNTADJ",output_log) 
    
    BDT_ADJMTDTL_tbl['PMNTADJ_ID']=pmntadj_id
    adjmtdtl_id=process_insert_table("CAIMS_BDT_ADJMTDTL", BDT_ADJMTDTL_tbl, BDT_ADJMTDTL_DEFN_DICT,con,schema,"SQ_BDT_ADJMTDTL",output_log)
    writelog(str(record_id)+"                    ADJMTDTL",output_log)

               
def process_TYP2505_BALDTL():                
    "250500"
    global BDT_BALDTL_tbl 
    global bccbbil_id
    global baldtl_id
    global record_id
    global output_log
    global baldtl_prevKey
    
    initialize_tbl('BDT_BALDTL_tbl')    
    
#DTL_INVOICE-concatenation of DINV_REF and DINV_DATE
#FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X13 X5 X2 X1 X6 X4
#        [61:71]      [71:76]      [76:78]
#FIXFORM DINV_REF/A10 DINV_DATE/A5 DSTATE/A2 X38
#FIXFORM DPREVBAL/Z11.2 DPAYMNT/Z11.2 DADJT/Z11.2 DBAL/Z11.2
#FIXFORM X4 LPC_APPLIED/Z11.2
#    COMPUTE
#    INVDATECC=INVDATECC
#    ASTATE=ASTATE   (ROOT SEGMENT)
     
#    BDT_BCCBBIL_tbl['ASTATE'] 
#    BDT_BCCBBIL_tbl['INVDATECC']
    
    BDT_BALDTL_tbl['BCCBBIL_ID']=bccbbil_id
#    BDT_BALDTL_tbl['DINV_REF']=line[61:71] no in 
    BDT_BALDTL_tbl['DINVDATECC']=line[71:76]
    if line[76:78].rstrip(' ') == '':
        BDT_BALDTL_tbl['DSTATE']='nl'        
    else:
        BDT_BALDTL_tbl['DSTATE']=line[76:78]
#                                   DINV_REF+DINV_DATE
    BDT_BALDTL_tbl['DTL_INVOICE']=line[61:71].rstrip(' ')+line[71:76].rstrip(' ')    
    BDT_BALDTL_tbl['DPREVBAL']=convertnumber(line[116:127],2)
    BDT_BALDTL_tbl['DPAYMNT']=convertnumber(line[127:138],2)
    BDT_BALDTL_tbl['DADJT']=convertnumber(line[138:149],2)
    BDT_BALDTL_tbl['DBAL']=convertnumber(line[149:160],2)
    BDT_BALDTL_tbl['LPC_APPLIED']=convertnumber(line[164:175],2)
    #default the LPC fields to 0.   
    BDT_BALDTL_tbl['LPC_INV_IR']=0
    BDT_BALDTL_tbl['LPC_INV_IA']=0
    BDT_BALDTL_tbl['LPC_INV_ND']=0
    BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id);
    
    
    tmpTblRec={}
    tmpTblRec['BCCBBIL_ID']=bccbbil_id
    tmpTblRec['DINVDATECC']=BDT_BALDTL_tbl['DINVDATECC']
    tmpTblRec['DSTATE']=BDT_BALDTL_tbl['DSTATE']
    
    if baldtl_prevKey == str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE'].rstrip(' ')):
        writelog("BALDTL keys are equal...DOING AN UPDATE HERE!!!!!!!!!!!!!!!!!!!!!",output_log)
        process_update_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,output_log)
    else:
        baldtl_id=process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,"SQ_BDT_BALDTL",output_log)
        baldtl_prevKey=str(BDT_BALDTL_tbl['BCCBBIL_ID'])+str(BDT_BALDTL_tbl['DINVDATECC'])+str(BDT_BALDTL_tbl['DSTATE']).rstrip(' ')
        writelog(str(record_id)+"     BALDTL",output_log)
       
    
def process_TYP2715_BALDTL():
    "271500"
    #Add this paragraph for combined company data.
    global BDT_BALDTL_tbl 
    global record_id
    global output_log
    # FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X6 X25 X10
    #FIXFORM DINV_DATE/A5   # same as DINVDATECC
    #FIXFORM X57 DSTATE/A2
    #FIXFORM LPC_INV_IR/Z11.2
    #FIXFORM LPC_INV_IA/Z11.2 X11 X8 LPC_INV_ND/Z11.2
    dinv_date=line[71:76]
    dstate=line[133:135]

    if BDT_BALDTL_tbl['DINVDATECC']==dinv_date and  BDT_BALDTL_tbl['DSTATE']==dstate:
        BDT_BALDTL_tbl['LPC_INV_IR']=convertnumber(line[135:146],2)
        BDT_BALDTL_tbl['LPC_INV_IA']=convertnumber(line[146:157],2)
        BDT_BALDTL_tbl['LPC_INV_ND']=convertnumber(line[176:187],2)
        BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,schema,output_log) 
    else:
        writelog("WARNING: The BALDTL record "+str(record_id)+" has no associated BALDUE record. INSERTING ANYWAY.",output_log)
    

##INITIALIZATION PARAGRAPHS
def initialize_tbl(tbl):
    global current_abbd_rec_key
 
       
    if  tbl == 'BDT_BCCBBIL_tbl':
        BDT_BCCBBIL_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        BDT_BCCBBIL_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        BDT_BCCBBIL_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in BDT_BCCBBIL_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                BDT_BCCBBIL_tbl[key]=''        
    elif tbl == 'BDT_BALDTL_tbl':
        for key,value in BDT_BALDTL_tbl.items() :
            BDT_BALDTL_tbl[key]='' 
    elif tbl == 'BDT_CRNT1_tbl':
        for key,value in BDT_CRNT1_tbl.items() :
            BDT_CRNT1_tbl[key]=''
    elif tbl == 'BDT_CRNT2_tbl':
        for key,value in BDT_CRNT2_tbl.items() :
            BDT_CRNT2_tbl[key]=''            
    elif tbl == 'BDT_SWSPLCHG_tbl':
        for key,value in BDT_SWSPLCHG_tbl.items() :
            BDT_SWSPLCHG_tbl[key]=''
    elif tbl == 'BDT_PMNTADJ_tbl':
        for key,value in BDT_PMNTADJ_tbl.items() :
            BDT_PMNTADJ_tbl[key]=''
    elif tbl == 'BDT_ADJMTDTL_tbl':
        for key,value in BDT_ADJMTDTL_tbl.items() :
            BDT_ADJMTDTL_tbl[key]=''
    else:
        process_ERROR_END("ERROR: No initialization code for "+tbl+" in the initialize_tbl method.")

####END INITIALIZE PROCEDURES    
 
        

 
def count_record(currec,unknownRec):
    #debug("****** procedure==>  "+whereami()+" ******")  
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
    #debug("****** procedure==>  "+whereami()+" ******")
    global record_counts, unknown_record_counts, BDT_KEY_cnt
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
    
    writelog("\n  Total count: "+str(unkCnt),output_log)    
    writelog("**",output_log)    
    
    writelog("TOTAL ACNA/EOB_DATE/BAN's processed:"+str(BDT_KEY_cnt),output_log)
    writelog(" ",output_log)
    writelog("Total input records read from input file:"+str(idCnt+unkCnt),output_log)
    writelog(" ",output_log)    
 
    
def process_ERROR_END(msg):
    writelog("ERROR:"+msg,output_log)
    con.commit()
    con.close()
    process_close_files()
    raise Exception("ERROR:"+msg)

    
def process_close_files():
    global bdt_input
    global output_log
        
    bdt_input.close();
    output_log.close()
   
    
    
def endProg(msg):
    global output_log
 
    con.commit()
    con.close()
    
    process_write_program_stats()
     
    endTM=datetime.datetime.now()
    print "\nTotal run time:  %s" % (endTM-startTM)
    writelog("\nTotal run time:  %s" % (endTM-startTM),output_log)
     

    writelog("\n"+msg,output_log)
     
    process_close_files()

"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
"BEGIN Program logic"
"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


print "Starting Program"
init()
main()
endProg("-END OF PROGRAM-")
