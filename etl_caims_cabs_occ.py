# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
PROGRAM NAME:     etl_caims_cabs_occ.py                                     
LOCATION:                      
PROGRAMMER(S):    Dan VandeRiet                                 
DESCRIPTION:      CAIMS Extract/Transformation/Load program for Bill Data Tape
                  (OCC) records.
                  
REPLACES:         Legacy CTL program BC100FT0 - LOAD BCCBBIL FOCUS DATABASE.
                                                                         
LANGUAGE/VERSION: Python/2.7.10                                          
INITIATION DATE:  x/x/2016                                                
INPUT FILE(S):    PBCL.CY.XRU0102O.CABS.G0358V00.txt (build from GDG)
LOCATION:         MARION MAINFRAME              
                                                                         
OUTPUT:           ORACLE DWBS001P                                
                  Table names CAIMS_OCC_*
                                                                         
EXTERNAL CALLS:                                    
                                                                         
LOCATION:         /home/caimsown/etl    
                                                                         
Copyright 2016, CenturyLink All Rights Reserved. Unpublished and          
Confidential Property of CenturyLink.                                          
                                                                         
CONFIDENTIAL: Disclose and distribute solely to CenturyLink employees
having a need to know.
=========================================================================
                 R E V I S I O N      H I S T O R Y                      
=========================================================================
PROGRAMMER:DAN VANDERIET                      DATE:04/20/2016
VALIDATOR:                                    DATE:                      
REASON: INITIAL VERSION 
=========================================================================
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
                                   createTableTypeDict,translate_TACCNT_FGRP ,process_check_exists, invalid_acna_chars  

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
#python etl_caims_cabs_occ.py PBCL.CY.XRU0102O.CABS.Oct14.txt
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
    inputPath=settings.get('OCCSettings','WINDOWS_OCC_inDir') 
else:
    inputPath=settings.get('OCCSettings','LINUX_OCC_inDir') 

IP_FILENM_AND_PATH =os.path.join(inputPath,fileNm)

if os.path.isfile(IP_FILENM_AND_PATH):
    print ("Input file:"+IP_FILENM_AND_PATH)
else:
    raise Exception("ERROR: File not found:"+IP_FILENM_AND_PATH)

"GLOBAL VARIABLES"   
#record_id='123456'
badKey=False
blankACNA=False
badCharsInACNA=False
current_abbd_rec_key={'ACNA':'x','EOB_DATE':'1','BAN':'x'}
prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}   #EOB_DATE,ACNA,BAN key   
 
record_counts={}
unknown_record_counts={}
BILL_REC='10' 
STAR_LINE='*********************************************************************************************'  

"TRANSLATERS"      
    
def init():
    global occ_input 
    global record_id
    global output_log
 
    "OPEN FILES"
    "   CABS INPUT FILE"
    occ_input = open(IP_FILENM_AND_PATH, "r")
    
    
    "PROCESS HEADER LINE"
    "  -want to get bill cycle info for output log file "
    "  -this will make the log file names more sensical " 
#    READ HEADER LINE    u
    headerLine=occ_input.readline()
    record_id=headerLine[:6]
    cycl_yy=headerLine[6:8]
    cycl_mmdd=headerLine[8:12]
    cycl_time=headerLine[12:21].replace('\n','')

    "  CREATE LOG FILE WITH CYCLE DATE FROM HEADER AND RUN TIME OF THIS JOB"
    log_file=os.path.join(OUTPUT_DIR,"OCC_Cycle"+str(cycl_yy)+str(cycl_mmdd)+str(cycl_time)+"_"+startTM.strftime("%Y%m%d_@%H%M") +".txt")
         
    output_log=open(log_file, "w");
    output_log.write("-OCC CAIMS ETL PROCESS-")
    
    if record_id not in settings.get('OCCSettings','OCCHDR'):
        process_ERROR_END("The first record in the input file was not a "+settings.get('OCCSettings','OCCHDR').rstrip(' ')+" record.")

    writelog("Process "+sys.argv[0],output_log)
    writelog("   started execution at: " + str(startTM),output_log)
    writelog(STAR_LINE,output_log)
    writelog(" ",output_log)
    writelog("Input file: "+str(occ_input),output_log)
    writelog("Log file: "+str(output_log),output_log)
    
    "Write header record information only"
    writelog("Header Record Info:",output_log)
    writelog("     Record ID: "+record_id+" YY: "+cycl_yy+", MMDD: "+cycl_mmdd+", TIME: "+str(cycl_time),output_log)
    
    count_record(record_id,False)
    del headerLine,cycl_yy,cycl_mmdd,cycl_time
 
def main():
    #OCC_config.initialize_OCC() 
    global record_type
    global line
    global output_log
    global record_counts, unknown_record_counts
    global unique_cntr
    global fid_cntr
    global sodate
    global so_id
    global fd1
    global cd_fid1
    global cd_fid2
    global fid_data1
    global fid_data2
    global piu
    global piu_src_ind
    global plu
    global pvu_src_ind
    global pct_voip_usg
    global pct_orig_usage 
    global bctfocc_id
    global occ_fid1_id
    global occ_fid2_id
    global occ_info_id
    global occ_hdr_id
    global occd_inf_id
    global occ_pidinf_id

    "Counters"  
    global inputLineCnt, OCC_KEY_cnt

    "TABLE Dictionaries - for each segment"
    global OCC_BCTFOCC_tbl,  OCC_BCTFOCC_DEFN_DICT
    global OCC_OCC_HDR_tbl, OCC_OCC_HDR_DEFN_DICT
    global OCC_OCC_FID1_tbl, OCC_OCC_FID1_DEFN_DICT
    global OCC_OCC_FID2_tbl, OCC_OCC_FID2_DEFN_DICT
    global OCC_OCC_INFO_tbl, OCC_OCC_INFO_DEFN_DICT    
    global OCC_OCCD_INF_tbl, OCC_OCCD_INF_DEFN_DICT   
    global OCC_PIDINF_tbl, OCC_PIDINF_DEFN_DICT

    "text files"
    global occ_input
    
    global firstBillingRec
    firstBillingRec=True
    
    global record_id
    global prev_record_id
    global current_abbd_rec_key 
    global prev_abbd_rec_key        
    
#DECLARE TABLE ARRAYS AND DICTIONARIES
    bctfocc_id=0   
    OCC_BCTFOCC_tbl=dict() 
    OCC_BCTFOCC_DEFN_DICT=createTableTypeDict('CAIMS_OCC_BCTFOCC',con,schema,output_log)
    
    occ_hdr_id=0
    OCC_OCC_HDR_tbl=dict()
    OCC_OCC_HDR_DEFN_DICT=createTableTypeDict('CAIMS_OCC_OCC_HDR',con,schema,output_log)
    
    occ_fid1_id=0    
    OCC_OCC_FID1_tbl=dict()
    OCC_OCC_FID1_DEFN_DICT=createTableTypeDict('CAIMS_OCC_OCC_FID1',con,schema,output_log)  

    occ_fid2_id=0   
    OCC_OCC_FID2_tbl=dict()
    OCC_OCC_FID2_DEFN_DICT=createTableTypeDict('CAIMS_OCC_OCC_FID2',con,schema,output_log)

    occ_info_id=0   
    OCC_OCC_INFO_tbl=dict()
    OCC_OCC_INFO_DEFN_DICT=createTableTypeDict('CAIMS_OCC_OCC_INFO',con,schema,output_log)
    
    occd_inf_id=0   
    OCC_OCCD_INF_tbl=dict()
    OCC_OCCD_INF_DEFN_DICT=createTableTypeDict('CAIMS_OCC_OCCD_INF',con,schema,output_log)

    occ_pidinf_id=0   
    OCC_PIDINF_tbl=dict()
    OCC_PIDINF_DEFN_DICT=createTableTypeDict('CAIMS_OCC_PIDINF',con,schema,output_log)

    "COUNTERS"
    inputLineCnt=0
    OCC_KEY_cnt=0
    status_cnt=0
    "KEY"
    prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}
    
    "LOOP THROUGH INPUT CABS TEXT FILE"
    #note variables in for line appear to be global
    for line in occ_input:
      
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
            
    #      badKey  #indicates empty ban or eob_date
    #      blankACNA #ok-well just insert without an ACNA  (i.e. ='nl')
    #      badCharsInACNA #warning- will insert w/out 
            
            if badKey:
                count_record("BAD_ABD_KEY",True)
                writelog("WARNING: BAD INPUT DATA.   ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'],output_log)   
            else:
                if blankACNA:
                    writelog("WARNING: Inserting data with blank ACNA.   ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'],output_log)
                if badCharsInACNA:
                    writelog("WARNING: Invalid chars found in ACNA.  Will write data with blank ACNA.   ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'],output_log)
                    
                if current_abbd_rec_key != prev_abbd_rec_key:
                    OCC_KEY_cnt+=1
                    reset_record_flags()

                process_occ_records()
   
            "set previous key for comparison in next iteration"   
            prev_abbd_rec_key=current_abbd_rec_key
            
        elif record_id in settings.get('OCCSettings','OCCFTR'):
            "FOOTER RECORD"
            log_footer_rec_info()     
            
        else:
            "ERROR:Unidentified Record"
            writelog("ERROR: Not sure what type of record this is:"+str(record_id),output_log)
            writelog(line,output_log)
         
         
        prev_record_id=record_id
        "END-MAIN PROCESS LOOP"    
    #END of For Loop
       
    print str(inputLineCnt)+" lines completed processing...."   
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
#             STRUCTURE OF FOCUS    FILE BCCBOCC 
#
#         ROOTREC
# 01      SH3
#**************
#*EOB_DATE    **I
#*ACNA        **I
#*BAN         **I
#*BILL_DATE   **I
#***************
# **************
#       I
#       +-----------------+
#       I CUSTSEG         I fd1
# 02    I KU        04    I S2
#..............    **************
#:BAN         :K   *SODATECC    **I
#:CUST_GROUP  :    *SO_ID       **
#:MKT_ID      :    *UNIQUE_CNTR **
#:SPLRID      :    *PON         **
#:............:    ***************
# JOINEDI BCCBCACB  **************
#       I                 I
#       I CUSTSEG2        I OCC_CK
# 03    I KU        05    I S1
#..............    **************
#:CUST_GROUP  :K   *FD1         **
#:CUST_GRP_NAM:    *            **
#:BUS_OFC_ID  :    *            **
#:............:    ***************
# JOINED  BCTFCACG  **************
#                         I
#                         I OCC_FID1
#                   06    I S1
#                  **************
#                  *CD_FID1     **
#                  *FID_DATA1   **
#                  *PIU         **
#                  *ASG         **
#                  ***************
#                         I
#                         I OCC_FID2
#                   07    I S1
#                  **************
#                  *CD_FID2     **
#                  *FID_DATA2   **
#                  ***************
#                         I
#                         I OCC_INFO
#                   08    I S1
#                  **************
#                  *PHRASE_CD   **
#                  *OCCFROMDTCC **
#                  *OCCTHRUDTCC **
#                  *FAC_CHG_TYPE**
#                   **************
#                         I
#       +-----------------+-----------------+
#       I                 I                 I
#       I OCCD_INF        I PIDINF          I PICSEG-NOTUSED
# 09    I S1        11    I S0         
#**************    **************    **************
#*USOC        **   *D_PLAN_ID   **    
#*STATE       **   *D_DIS_PERCNT**    
#*QTY_USOC    **   *D_DIS_PLAN_>**   
#*FAC_CHG_IND **   *D_VOL_PLN_T>**     
# **************    **************     
#       I
#       I PLN_INF   NOT USED
# 10    I U
#**************
 
def process_occ_records():
    global record_id,output_log
    global firstBillingRec

#    verno=line[82:84]
    
    if firstBillingRec:
        "FIRST RECORD ONLY"
        #write BILL CYCLE DATE AND BOS version number to log
        writelog("Bill Cycle:"+line[13:15]+"/"+line[15:17]+"/20"+line[11:13],output_log)
        writelog("**--------------------------**",output_log)
        writelog("** BOS VERSION NUMBER IS "+line[82:84]+" ** ",output_log)
        writelog("**--------------------------**",output_log)
        firstBillingRec=False
       
    unknownRecord=False
    #BCCBBIL/ROOT/BALDUE always have 010100, 050500, and 051000    
    if record_id == '010100':
       process_0101REC_39_ROOT()
    elif record_id == '050500':            
       process_0505REC_ROOT()     
    elif record_id == '300500':  
       process_3005REC_OCC_HDR()  
    elif record_id == '302500':
       process_3025REC_OCC_HDR()      #            
    elif record_id == '301000':     
       process_3010REC_OCC_FID()           
    elif record_id in ('301500','301501'):           
       process_3015DRVR_50()                    ##BRANCH to OCC-INFO"
    elif record_id == '301550':          ##84 RECS FOUND
       process_301550DRVR()
    elif record_id in ('302000','302001'):
       process_3020REC_34()
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
                
    # now check BAN and EOB_DATE
    if tmpBAN == '' or tmpEOB_DATE == '':
        badKey=True
        
    return { 'ACNA':newACNA,'EOB_DATE':tmpEOB_DATE,'BAN':tmpBAN}     
     
def reset_record_flags():
    writelog("     " ,output_log)
    writelog("reset_record_flags" ,output_log)
    "These flags are used to check if a record already exists for the current acna/ban/eob_date"
    "The first time a record is processed, if the count == 0, then an insert will be performed,"
    "else an update."
    global bctfocc_id
    global fd1_id
    global occ_fid1_id
    global occ_fid2_id
    global occ_info_id
    global occ_pidinf_id
    global occd_inf_id
    global pflag
    global unique_cntr
    global fid_cntr
    global piu_src_ind
    global plu
    global pvu_src_ind
    global pct_voip_usg
    global pct_orig_usage      
    
    
    
    global sodatecc
    global so_id
    global fd1
    global cd_fid1
    global cd_fid2
    global fid_data1
    global fid_data2
    global piu
        
    bctfocc_id=0
    occ_fid1_id=0
    occ_fid2_id=0
    occ_info_id=0
    occd_inf_id=0
    fd1_id=0
    occ_pidinf_id=0
    pflag=''
    unique_cntr=0
    fid_cntr=0
    sodatecc='19000101'
    so_id='nl'
    fd1=''
    cd_fid1=''
    cd_fid2=''
    fid_data1=''
    fid_data2=''
    piu=' '

    piu_src_ind=''
    plu=''
    pvu_src_ind=''
    pct_voip_usg=0
    pct_orig_usage='' 
 
 
  
def process_0101REC_39_ROOT():
    writelog(record_id+"--"+":0101REC_39_ROOT",output_log)
    global firstBillingRec
    global bctfocc_id
#    global OCC_BCTFOCC_tbl,  OCC_BCTFOCC_DEFN_DICT  
    global pflag
    pflag=' '
    
    initialize_tbl('OCC_BCTFOCC_tbl')
#COMPUTE
#BANLOCK='N';
#PFLAG = ' ';
#TASG = ' '
     
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 TNPA/A3 X-3 BAN/A13
#FIXFORM X4 X7 X2 X1 X7 X10 X10
#        [71:76] [76:82]  [82:84]       [84:97] 
#FIXFORM JDATE/5 X6       VERSION_NBR/2 X1 X4 X3 X2 X3
#        [97:98] [98:101] [101:105] [105:106]
#FIXFORM TLF/A1  NLATA/A3 X4        HOLD_BILL/1
##       [106:107] [107:108] [108:109] [109:218] [218:219]
#FIXFORM X1        TACCNT/A1 TFGRP/A1  X109      MAN_BAN_TYPE/1
##       [219:220]      [220:224]  [224:225]
#FIXFORM UNB_SWA_PROV/1 X4         MPB/A1

    OCC_BCTFOCC_tbl['BILL_DATE']=line[71:76]
    OCC_BCTFOCC_tbl['TLF']=line[97:98]
    OCC_BCTFOCC_tbl['NLATA']=line[98:101]
    OCC_BCTFOCC_tbl['HOLD_BILL']=line[105:106]
#                                                                 TACCNT,TFGRP
    OCC_BCTFOCC_tbl['TACCNT_FGRP']=translate_TACCNT_FGRP(line[107:108],line[108:109])       
    OCC_BCTFOCC_tbl['CAIMS_REL']=7
    OCC_BCTFOCC_tbl['EOBDATECC']=OCC_BCTFOCC_tbl['EOB_DATE']
    OCC_BCTFOCC_tbl['BILLDATECC']=OCC_BCTFOCC_tbl['BILL_DATE']
    OCC_BCTFOCC_tbl['UNB_SWA_PROV']=line[219:220] 
    OCC_BCTFOCC_tbl['MAN_BAN_TYPE']=line[218:219] 
    OCC_BCTFOCC_tbl['MPB']=line[224:225]
    OCC_BCTFOCC_tbl['INPUT_RECORDS']=str(record_id )
    
##    OCC_BCTFOCC_tbl['BANLOCK']='N'    #defaulted db value
##    OCC_BCTFOCC_tbl['VERSION_NBR']=line[82:84] NOT IN mfd
#
#    OCC_BCTFOCC_tbl['TLF']=line[97:98]
#    OCC_BCTFOCC_tbl['NLATA']=line[98:101]
#    OCC_BCTFOCC_tbl['HOLD_BILL']=line[105:106]
#   
##                                                                 TACCNT,TFGRP
#    OCC_BCTFOCC_tbl['TACCNT_FGRP']=translate_TACCNT_FGRP(line[107:108],line[108:109])  
#    OCC_BCTFOCC_tbl['MAN_BAN_TYPE']=line[218:219]
#    OCC_BCTFOCC_tbl['UNB_SWA_PROV']=line[219:220]
#    OCC_BCTFOCC_tbl['MPB']=line[224:225]
#    OCC_BCTFOCC_tbl['BILL_DATE']=line[71:76]  #jdate
##    OCC_BCTFOCC_tbl['EOBDATEA']=OCC_BCTFOCC_tbl['EOB_DATE'] no tin mfd
#    OCC_BCTFOCC_tbl['EOBDATECC']=OCC_BCTFOCC_tbl['EOB_DATE']
#    OCC_BCTFOCC_tbl['BILLDATECC']=OCC_BCTFOCC_tbl['BILL_DATE']
#    OCC_BCTFOCC_tbl['CAIMS_REL']='B'
#    OCC_BCTFOCC_tbl['INPUT_RECORDS']=str(record_id)
    #NOTE did a record count and BCCBBIL/BALDUE always have an 010100, 050500, and 05100 records
    #so not doing an insert here... defer the inser to the 3rd record(051000)
    writelog(record_id+"--"+"leaving process_0101REC_39_ROOT",output_log) 

   
def process_0505REC_ROOT():
    "BO_CODE"
    global bctfocc_id    
    global  record_id,output_log
    writelog(record_id+"--"+":0505REC_ROOT",output_log)
#FIXFORM X-225 ACNA/5 EOB_DATE/6 BAN/13 X5
#FIXFORM X176 BO_CODE/A4
 
    OCC_BCTFOCC_tbl['ICSC_OFC']=line[211:215].rstrip(' ').lstrip(' ')
    OCC_BCTFOCC_tbl['INPUT_RECORDS']+=","+str(record_id)
   
    bctfocc_id=process_insert_table("CAIMS_OCC_BCTFOCC", OCC_BCTFOCC_tbl,OCC_BCTFOCC_DEFN_DICT,con,schema,"SQ_OCC_BCTFOCC",output_log)
    
    writelog(record_id+"--"+"leaving process_0505REC_ROOT",output_log)


def process_3005REC_OCC_HDR():    
    global bctfocc_id
    global occ_hdr_id
    global pflag
    global unique_cntr
    global sodatecc
    global so_id
    writelog(record_id+"--"+"XXXXX:3005REC_OCC_HDR",output_log)
    "300500"
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
##           [69:77]     [77:89]   [89:105] [105:146] [146:166]
#FIXFORM X39 SODATECC/A8 SO_ID/A12 PON/A16  X41       EXT_EC_RPR_TKT_NO/20
#COMPUTE
#  PFLAG = 'S';
#  SO_MONTHLY=0;
#  SO_FRAC=0;
#  SO_NRC=0;
#  SO_BLD=0;

    unique_cntr+=1
    pflag='S'
    sodatecc=line[69:77]
    so_id=line[77:89]

    
    if bctfocc_id > 0:
        tmpTblRec={}
        tmpTblRec['BCTFOCC_ID']=bctfocc_id
        tmpTblRec['SODATECC']=sodatecc
        tmpTblRec['SO_ID']=so_id       
        hdr=process_check_exists("OCC_OCC_HDR_tbl", tmpTblRec, OCC_OCC_HDR_DEFN_DICT,con,schema,output_log)     
        if hdr>0:
            occ_hdr_id=hdr
            writelog("bypassed in process_3005REC_OCC_HDR",output_log)
        else:
            initialize_tbl('OCC_OCC_HDR_tbl')
            OCC_OCC_HDR_tbl['BCTFOCC_ID']=bctfocc_id
            OCC_OCC_HDR_tbl['SODATECC']=sodatecc
            OCC_OCC_HDR_tbl['SO_ID']=so_id
            OCC_OCC_HDR_tbl['UNIQUE_CNTR']=unique_cntr 
            OCC_OCC_HDR_tbl['PON']=line[89:105]
            OCC_OCC_HDR_tbl['SO_MONTHLY']=0
            OCC_OCC_HDR_tbl['SO_FRAC']=0
            OCC_OCC_HDR_tbl['SO_NRC']=0
            OCC_OCC_HDR_tbl['SO_BLD']=0
            OCC_OCC_HDR_tbl['EXT_EC_RPR_TKT_NO']=line[146:166]
            OCC_OCC_HDR_tbl['INPUT_RECORDS']=str(record_id)
            writelog("created in process_3005REC_OCC_HDR",output_log)
            occ_hdr_id=process_insert_table("CAIMS_OCC_OCC_HDR", OCC_OCC_HDR_tbl,OCC_OCC_HDR_DEFN_DICT,con,schema,"SQ_OCC_OCC_HDR",output_log) 
        del tmpTblRec, hdr    
    else:
        writelog("ERROR:No insert to occ_hdr. No root record",output_log)

#  UNIQUE_CNTR/I5= UNIQUE_CNTR +1;
#MATCH EOB_DATE ACNA BAN  ROOTREC
#   ON MATCH   CONTINUE
# ON NOMATCH TYPE "CASE 3005REC - NOMATCH DATE ACNA BAN - REJECTED"
#   ON NOMATCH REJECT
#MATCH SODATECC SO_ID UNIQUE_CNTR   OCC_HDR
#   ON MATCH   INCLUDE
#   ON NOMATCH INCLUDE

#   FIXFORM ON BCTFOCCI X105 OCCLO/Z11.2 USGLO/Z11.2
   
#    OCC_CRNT2_tbl['OCCLO']=convertnumber(line[105:116],2)
#    OCC_CRNT2_tbl['USGLO']=convertnumber(line[116:127],2)
#    OCC_CRNT2_tbl['INPUT_RECORDS']+=","+str(record_id)
#   
#    
#    if prev_record_id == '051300':
##        if crnt2_id>0:
##            writelog("ERROR: CRNT2 is "+str(crnt2_id)+", we shouldnt be writing another record",output_log)
#        crnt2_id=process_insert_table("CAIMS_OCC_CRNT2", OCC_CRNT2_tbl, OCC_CRNT2_DEFN_DICT,con,schema,"SQ_OCC_CRNT2",output_log)
#        writelog(str(record_id)+"                    CRNT2",output_log)
#    else:
#        process_ERROR_END("ERROR: Previous record should have been a 051300.")
    writelog(record_id+"--"+"leaving process_3005REC_OCC_HDR",output_log)
#def process_stop():
#    return "x"
def process_3025REC_OCC_HDR():    
    global occ_hdr_id
    global bctfocc_id 
    global crnt1_id
    global crnt2_id
    global unique_cntr
    global sodatecc
    global so_id
    writelog(record_id+"--"+"XXXXX:3025REC_OCC_HDR",output_log)
    
#   FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
#           [30:166] [166:177]        [177:188]   
#   FIXFORM X136     SO_MONTHLY/Z11.2 SO_FRAC/Z11.2
#           [188:199]     [199:201]
#   FIXFORM SO_NRC/Z11.2  SO_BLD/Z11.2
 
    initialize_tbl('OCC_OCC_HDR_tbl')
    OCC_OCC_HDR_tbl['BCTFOCC_ID']=bctfocc_id
    OCC_OCC_HDR_tbl['SO_ID']=so_id
    OCC_OCC_HDR_tbl['SODATECC']=sodatecc
    OCC_OCC_HDR_tbl['UNIQUE_CNTR']=unique_cntr
    OCC_OCC_HDR_tbl['SO_MONTHLY']=convertnumber(line[166:177],2)
#    if str(OCC_OCC_HDR_tbl['SO_MONTHLY']) == "-665.70" or str(OCC_OCC_HDR_tbl['SO_MONTHLY']) =="-352.09" or str(OCC_OCC_HDR_tbl['SO_MONTHLY'])=="-133.16":
#        process_stop()
    OCC_OCC_HDR_tbl['SO_FRAC']=convertnumber(line[177:188],2)
    OCC_OCC_HDR_tbl['SO_NRC']=convertnumber(line[188:199],2)
    OCC_OCC_HDR_tbl['SO_BLD']=convertnumber(line[199:210],2)
    OCC_OCC_HDR_tbl['INPUT_RECORDS']=str(record_id)




    if bctfocc_id > 0:        
        tmpTblRec={}
        tmpTblRec['BCTFOCC_ID']=bctfocc_id
        tmpTblRec['SO_ID']=so_id
        tmpTblRec['SODATECC']=sodatecc
        tmpTblRec['UNIQUE_CNTR']=unique_cntr        
#        tmpTblRec['SO_MONTHLY']=0
#        tmpTblRec['SO_FRAC']=0
#        tmpTblRec['SO_NRC']=0
#        tmpTblRec['SO_BLD']=0
#updating 2 records in some cases.   Try passing 0 values to the tmpTblRec        
#just for the check_exists        

       
        if occ_hdr_id > 0 and process_check_exists("CAIMS_OCC_OCC_HDR", tmpTblRec, OCC_OCC_HDR_DEFN_DICT,con,schema,output_log):
            writelog("3025REC_OCC_HDR doing an update",output_log)
#            OCC_OCC_HDR_tbl['UNIQUE_CNTR']=''
#Here's the culprit.  updating more than one record        condition    
            process_update_table("CAIMS_OCC_OCC_HDR", OCC_OCC_HDR_tbl, OCC_OCC_HDR_DEFN_DICT,con,schema,output_log)
#            process_update_table("CAIMS_OCC_OCC_HDR", OCC_OCC_HDR_tbl, OCC_OCC_HDR_DEFN_DICT,con,schema,output_log, \
#            addlwhere="SO_MONTHLY=0 and SO_FRAC=0 and SO_BLD=0 and SO_NRC=0")
        else:
            occ_hdr_id=process_insert_table("CAIMS_OCC_OCC_HDR", OCC_OCC_HDR_tbl,OCC_OCC_HDR_DEFN_DICT,con,schema,"SQ_OCC_OCC_HDR",output_log)
            writelog("3025REC_OCC_HDR doing an INSERT",output_log)
        del tmpTblRec
    else:
        writelog("ERROR?: no insert to occ hdr. due to no root record",output_log)
    
    writelog(record_id+"--"+"leaving process_3025REC_OCC_HDR",output_log)
 
    
def process_3010REC_OCC_FID():    
    global output_log
    global pflag
    global bctfocc_id
    global occ_hdr_id
    global occ_fid1_id
    global occ_fid2_id
    "301000"
    writelog(record_id+"--"+"AAAAA:3010REC_OCC_FID-BRANCH to FID1 OR FID2",output_log)        
#FIXFORM X-160 THEFID/A5 X120 X6 X21  
    
    thefid=line[71:76].strip(' ')
    if thefid in ('CLF','CLS','CLT','CLM','OCL'):
        process_TYPE3010A() 
    elif thefid in ('CKL','CKLT','TSC','TN'):
        process_TYPE3010B()
    elif thefid == 'ASG':
        process_ASGTEMP()
        
    writelog(record_id+"--"+"leaving process_3010REC_OCC_FID",output_log) 
   
   
def process_TYPE3010A():
    global occ_hdr_id
    global occ_fid1_id
    global occ_fid2_id
    global cd_fid1
    global bctfocc_id
    global pflag
    global fid_data1
    global piu
    global fd1
    global piu_src_ind
    global plu
    global pvu_src_ind
    global pct_voip_usg
    global pct_orig_usage         
  
    global fid_cntr
    
    writelog(record_id+"--"+"AAAAAA:TYPE3010A -FID1",output_log)          
#called from process_3010REC_OCC_FID():      
#"301000"
                           
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X4 X7 X2 X1 X7 X10 X10
#        [71:76]    [76:93]          [76:126]       [126:129]
#FIXFORM CD_FID1/A5 FD1/A17 X-17     FID_DATA1/A50 PIU_FID1/A3
#        [129:140]   [140:141]      [141:144] [144:145]
#FIXFORM OCL_LOC/A11 PIU_SRC_IND/A1 PLU/3     PVU_SRC_IND/A1
#        [145:150]         [150:189] [189:192]        [192:223] [223:224]
#FIXFORM PCT_VOIP_USG/Z5.2 X39       PCT_ORIG_USAGE/3 X31       CKT_SUPI_IND/A1
#        [224:225]
#FIXFORM X1
#COMPUTE
#ASG = TASG;
#PFLAG = 'C';
#MATCH FD1   OCC_CK
#  ON NOMATCH INCLUDE
#  ON MATCH CONTINUE
#MATCH CD_FID1   OCC_FID1
#  ON NOMATCH INCLUDE
#  ON MATCH INCLUDE          
    fid_cntr+=1                           
    #This is not in the FOCUS code but trying to increment the unique_cntr
    #in order to avoid duplicates                           

    pflag='C'  
    fd1=line[76:93]
    cd_fid1=line[71:76]
    fid_data1=line[76:126]
    piu=line[126:129]
    piu_src_ind=line[140:141]
    plu=line[141:144]
    pvu_src_ind=line[144:145]
    pct_voip_usg=convertnumber(line[145:150] ,2)
    pct_orig_usage=line[189:192]  
   
  
                 
    if occ_hdr_id > 0:

        initialize_tbl('OCC_OCC_FID1_tbl') 
        OCC_OCC_FID1_tbl['OCC_HDR_ID']=occ_hdr_id                      
        OCC_OCC_FID1_tbl['CD_FID1']=cd_fid1
        #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
        #not part of the original FOCUS logic
        OCC_OCC_FID1_tbl['UNIQUE_CNTR']=fid_cntr
        OCC_OCC_FID1_tbl['FD1']=line[76:93]
        OCC_OCC_FID1_tbl['FID_DATA1']=fid_data1
        OCC_OCC_FID1_tbl['PIU']=piu
        OCC_OCC_FID1_tbl['OCL_LOC']=line[129:140] 
        OCC_OCC_FID1_tbl['PIU_SRC_IND']=piu_src_ind  
        OCC_OCC_FID1_tbl['PLU']=plu
        OCC_OCC_FID1_tbl['PVU_SRC_IND']=pvu_src_ind
        OCC_OCC_FID1_tbl['PCT_VOIP_USG']=pct_voip_usg
        OCC_OCC_FID1_tbl['PCT_ORIG_USAGE']=pct_orig_usage 
        OCC_OCC_FID1_tbl['CKT_SUPI_IND']=line[223:224]
        OCC_OCC_FID1_tbl['INPUT_RECORDS']=str(record_id)
            
        tmpTblRec={}
        tmpTblRec['OCC_HDR_ID']=OCC_OCC_FID1_tbl['OCC_HDR_ID']
        tmpTblRec['CD_FID1']=OCC_OCC_FID1_tbl['CD_FID1'] 
        tmpTblRec['UNIQUE_CNTR']=OCC_OCC_FID1_tbl['UNIQUE_CNTR']
        f1=process_check_exists("OCC_OCC_FID1_tbl", tmpTblRec, OCC_OCC_FID1_DEFN_DICT,con,schema,output_log)     
        if f1>0:    # we want to insert if it exists too...just update the sequence to get around the key
            fid_cntr+=1
            OCC_OCC_FID1_tbl['UNIQUE_CNTR']=fid_cntr
        del tmpTblRec, f1
        
        occ_fid1_id=process_insert_table("CAIMS_OCC_OCC_FID1", OCC_OCC_FID1_tbl,OCC_OCC_FID1_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID1",output_log)

    else:             
        writelog("ERROR: in process_TYPE3010A.  occ_hdr_id is 0" ,output_log)
     
    writelog(record_id+"--"+"leaving process_TYPE3010A",output_log)   


def process_TYPE3010B():
    global pflag
    global bctfocc_id
    global occ_hdr_id
    global occ_fid1_id
    global occ_fid2_id
    global fid_data2
    global fid_cntr
    writelog(record_id+"--"+"AAAAAA:TYPE3010B - FID2",output_log) 
    
    pflag='L'
#called from process_3010REC_OCC_FID():      
#"301000"
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
#            [71:76]    [76:126]      [126:129]  
#FIXFORM X41 CD_FID2/A5 FID_DATA2/A50 PIU_FID1/A3    
#        [129:140]   [140:141]      [141:144] [144:145]
#FIXFORM OCL_LOC/A11 PIU_SRC_IND/A1 PLU/3     PVU_SRC_IND/A1
#    
#FIXFORM PCT_VOIP_USG/Z5.2 X39 PCT_ORIG_USAGE/3 X31 CKT_SUPI_IND/A1
#FIXFORM X1
   
    cd_fid2=line[71:76] 
    fid_data2=line[76:126]
    initialize_tbl('OCC_OCC_FID2_tbl')
    OCC_OCC_FID2_tbl['OCC_FID1_ID']=occ_fid1_id
    OCC_OCC_FID2_tbl['CD_FID2']=cd_fid2
    #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
    #not part of the original FOCUS logic
    OCC_OCC_FID2_tbl['UNIQUE_CNTR']=fid_cntr
    OCC_OCC_FID2_tbl['FID_DATA2']=fid_data2
    OCC_OCC_FID2_tbl['INPUT_RECORDS']=str(record_id)
   
    if occ_fid1_id > 0:
       tmpTblRec={}
       tmpTblRec['OCC_FID1_ID']=occ_fid1_id
       tmpTblRec['CD_FID2']=cd_fid2 
       tmpTblRec['UNIQUE_CNTR']=fid_cntr
       f2=process_check_exists("CAIMS_OCC_OCC_FID2", tmpTblRec, OCC_OCC_FID2_DEFN_DICT,con,schema,output_log)     
       if f2>0:    # we want to insert if it exists too...just update the sequence to get around the key
           fid_cntr+=1
           OCC_OCC_FID2_tbl['UNIQUE_CNTR']=fid_cntr
       del tmpTblRec, f2
       occ_fid2_id=process_insert_table("CAIMS_OCC_OCC_FID2", OCC_OCC_FID2_tbl,OCC_OCC_FID2_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID2",output_log)

    else:
       writelog("ERROR:No insert to OCC_FID2. No FID1 record",output_log)
    
        
#    writelog(str(record_id)+"OCC_FID2",output_log)    
#IF PFLAG NE 'C' OR 'L' GOTO TOP;
#COMPUTE
#PFLAG = 'L';
#MATCH CD_FID2  OCC_FID2
#  ON MATCH INCLUDE
#  ON NOMATCH INCLUDE
#   
    writelog(record_id+"--"+"leaving process_TYPE3010B",output_log)        

def process_ASGTEMP():
    global pflag
    writelog(record_id+"--"+"AAAAAA:ASGTEMP  set pflag to A",output_log)
#called from process_3010REC_OCC_FID():      
#"301000"
    
#FIXFORM X-225 X5 X6 X13 X18
#FIXFORM X6 X17 X5 TASG/6
#COMPUTE
    pflag='A'
    writelog(record_id+"--"+"leaving process_ASGTEMP",output_log)

def process_3015DRVR_50():   
    global bctfocc_id
    global occ_hdr_id
    global occ_fid1_id
    global occ_fid2_id
    global occ_info_id    
    global output_log
    global pflag
    
    "301500"  "301501"
    "theres always even pairs of these records which should result in the creation"
    "of OCC_INFO records"    
    
    writelog(record_id+"--"+"BBBBB:3015DRVR_50 - BRANCH to OCC-INFO",output_log)
      
    if pflag == 'L':
        process_3015REC_50()
    elif pflag == ' ':
        process_3015BAN_50()
    elif pflag == 'S':
        process_3015SO_50()
    elif pflag =='C':
        process_3015CKT_50()
    writelog(record_id+"--"+"leaving process_3015DRVR_50",output_log)
        
def process_3015REC_50(): 
    "301500"
    "301501"
    global bctfocc_id
    global occ_fid1_id
    global occ_fid2_id
    global occ_info_id
    global pflag
    writelog(record_id+"--"+"BBBBBB:3015REC_50",output_log)
    #FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
    #        [30:61]   [61:64]      [64:70]
    #FIXFORM X31       PHRASE_CD/A3 X6
    #        [70:78]       [78:86]       [86:87]         [87:88]
    #FIXFORM OCCFROMDTCC/8 OCCTHRUDTCC/8 FAC_CHG_TYPE/A1 RATEZONEIR/1
    #        [88:89]   
    #FIXFORM RATEZONEIA/1
    #        [89:100]         [100:111]        [111:122]
    #FIXFORM SUP_AMT_IR/Z11.2 SUP_AMT_IA/Z11.2 SUP_AMT_IRIA/Z11.2
    #        [122:133]          [133:144]          [144:180]
    #FIXFORM SUP_AMT_IAIA/Z11.2 SUP_AMT_NONJ/Z11.2 X36
    #        [180:188] [188:191]      [191:218]          [218:221]   
    #FIXFORM X8        PCT_ORIG_USG/3 X1 X1 X1 X15 X8 X1 PIU_INFO/3
    #        [221:222] [222:223]     [223:224]  [224:225]
    #FIXFORM X1        RC_NRC_IND/A1 X1         ACC_TYPE/1 X6

    if record_id == '301500':
        initialize_tbl('OCC_OCC_INFO_tbl')
        OCC_OCC_INFO_tbl['OCC_FID2_ID']=occ_fid2_id
        OCC_OCC_INFO_tbl['PHRASE_CD']=line[61:64]
        OCC_OCC_INFO_tbl['OCCFROMDTCC']=line[70:78]
        OCC_OCC_INFO_tbl['OCCTHRUDTCC']=line[78:86]
        OCC_OCC_INFO_tbl['FAC_CHG_TYPE']=line[86:87]
        OCC_OCC_INFO_tbl['RATEZONEIR']=line[87:88]
        OCC_OCC_INFO_tbl['RATEZONEIA']=line[88:89]
        OCC_OCC_INFO_tbl['SUP_AMT_IR']=convertnumber(line[89:100],2)
        OCC_OCC_INFO_tbl['SUP_AMT_IA']=convertnumber(line[100:111],2) 
        OCC_OCC_INFO_tbl['SUP_AMT_IRIA']=convertnumber(line[111:122],2)
        OCC_OCC_INFO_tbl['SUP_AMT_IAIA']=convertnumber(line[122:133],2)
        OCC_OCC_INFO_tbl['SUP_AMT_NONJ']=convertnumber(line[133:144],2)
        OCC_OCC_INFO_tbl['PCT_ORIG_USG']=convertnumber(line[188:191],0)
        OCC_OCC_INFO_tbl['PIU_INFO']=convertnumber(line[218:221],0)
        OCC_OCC_INFO_tbl['RC_NRC_IND']=line[222:223]
        OCC_OCC_INFO_tbl['ACC_TYPE']=line[224:225]
        OCC_OCC_INFO_tbl['INPUT_RECORDS']=str(record_id)
    elif record_id=='301501':
        #                         [110:121]        [121:122] [122:123]
        #FIXFORM ON BCTFBDTI X110 SUP_AMT_LO/Z11.2 X1        RATEZONEIRIA/1
        #        [123:124]      [124:125]     [125:130]  [130:133]      [133:160]
        #FIXFORM RATEZONEIAIA/1 RATEZONELOC/1 X5         PHCD_PFBAND1/3 X27
        #        [160:163]      [163:179]       [179:180]    [180:181]
        #FIXFORM PHCD_PFBAND2/3 AUDIT_NUMBER/16 PHCD_PFIIR/1 PHCD_PFIIA/1
        #        [181:182]      [182:198] [198:199]      [199:200]
        #FIXFORM PHCD_PFIIAIA/1 X16       BUS_RSDC_IND/1 PHCD_PFIIRIA/1 X25
        OCC_OCC_INFO_tbl['SUP_AMT_LO']=convertnumber(line[110:121],2)
        OCC_OCC_INFO_tbl['RATEZONEIRIA']=line[122:123]
        OCC_OCC_INFO_tbl['RATEZONEIAIA']=line[123:124]
        OCC_OCC_INFO_tbl['RATEZONELOC']=line[124:125]
        OCC_OCC_INFO_tbl['PHCD_PFBAND1']=line[130:133] 
        OCC_OCC_INFO_tbl['PHCD_PFBAND2']=line[160:163]        
        OCC_OCC_INFO_tbl['AUDIT_NUMBER']=line[163:179]
        OCC_OCC_INFO_tbl['PHCD_PFIIR']=line[179:180] 
        OCC_OCC_INFO_tbl['PHCD_PFIIA']=line[180:181]
        OCC_OCC_INFO_tbl['PHCD_PFIIAIA']=line[181:182] 
        OCC_OCC_INFO_tbl['PHCD_PFIIRIA']=line[199:200]
        OCC_OCC_INFO_tbl['BUS_RSDC_IND']=line[198:199]
        OCC_OCC_INFO_tbl['INPUT_RECORDS']+=","+str(record_id)
#MATCH PHRASE_CD   OCC_INFO
#     ON MATCH INCLUDE
#     ON NOMATCH INCLUDE
        "NOTE-according to the log there appears to be an insert for every 301500/301501 set of records."
        if occ_fid2_id > 0:
            occ_info_id=process_insert_table("CAIMS_OCC_OCC_INFO", OCC_OCC_INFO_tbl,OCC_OCC_INFO_DEFN_DICT,con,schema,"SQ_OCC_OCC_INFO",output_log)
        else:
            writelog("ERROR: Cant write OCC_INFO record. - No OCC_FID2 header record", output_log)  
        
    writelog(record_id+"--"+"leaving process_3015REC_50",output_log)
 
def process_3015BAN_50(): 
    global bctfocc_id
    global occ_hdr_id
    global occ_fid1_id
    global occ_fid2_id
    global occ_info_id
    global sodatecc
    global so_id
    global fd1
    global cd_fid1
    global fid_data1
    global piu
    global pflag
    global unique_cntr
    global fid_cntr
    global piu_src_ind
    global plu
    global pvu_src_ind
    global pct_voip_usg
    global pct_orig_usage     
 
    
    "301500"
    "301501"
    writelog(record_id+"--"+"BBBBBB:3015BAN_50",output_log)
#-* **************************************************************
#-*   CASS RECORD ID = ND03
#-*     THIS CASE LOADS BAN LEVEL CHARGES.
#-*     THOSE 103015 RECORDS THAT DON'T HAVE   103005, OR 103010
#-*     RECORDS
#-* *************************************************************
    "same fixform as 3015CKT_50()"
#DEACTIVATE  SODATECC SO_ID PON EXT_EC_RPR_TKT_NO SO_MONTHLY SO_FRAC
#DEACTIVATE  SO_BLD SO_NRC
#COMPUTE
#UNIQUE_CNTR/I5 = UNIQUE_CNTR + 1;
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
#        [30:61]   [61:64]      [64:70]
#FIXFORM X31       PHRASE_CD/A3 X6
#        [70:78]       [78:86]       [86:87]         [87:88]
#FIXFORM OCCFROMDTCC/8 OCCTHRUDTCC/8 FAC_CHG_TYPE/A1 RATEZONEIR/1
#        [88:89]   
#FIXFORM RATEZONEIA/1
#        [89:100]         [100:111]        [111:122]
#FIXFORM SUP_AMT_IR/Z11.2 SUP_AMT_IA/Z11.2 SUP_AMT_IRIA/Z11.2
#        [122:133]          [133:144]          [144:180]
#FIXFORM SUP_AMT_IAIA/Z11.2 SUP_AMT_NONJ/Z11.2 X36
#        [180:188] [188:191]      [191:218]          [218:221]   
#FIXFORM X8        PCT_ORIG_USG/3 X1 X1 X1 X15 X8 X1 PIU_INFO/3
#        [221:222] [222:223]     [223:224]  [224:225]
#FIXFORM X1        RC_NRC_IND/A1 X1         ACC_TYPE/1 X6
    if record_id == '301500':
        unique_cntr+=1
        sodatecc='19000101'
#    MATCH EOB_DATE ACNA BAN  ROOTREC
#    ON NOMATCH TYPE "CASE3015BAN_50 - NOMATCH DATE ACNA BAN - REJECTED"
#      ON NOMATCH REJECT
#      ON MATCH COMPUTE
#                 SODATECC=0;
#                 SO_MONTHLY=0;
#                 SO_FRAC=0;
#                 SO_NRC=0;
#                 SO_BLD=0;
#                 PON=' ';
#                 FD1=' ';
#                 CD_FID1=' ';
#                 FID_DATA1=' ';
#                 PIU_INFO=0;
#                 ASG=' ';
#                 CD_FID2=' ';
#                 FID_DATA2=' ';
#    UNIQUE_CNTR=UNIQUE_CNTR;
#      ON MATCH CONTINUE
#    MATCH SODATECC SO_ID UNIQUE_CNTR  OCC_HDR
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
        so_id='nl'
        fd1='nl'
        cd_fid1='nl'
        cd_fid2='nl'
        initialize_tbl('OCC_OCC_HDR_tbl')
        OCC_OCC_HDR_tbl['BCTFOCC_ID']=bctfocc_id
        OCC_OCC_HDR_tbl['SO_ID']=so_id
        OCC_OCC_HDR_tbl['SODATECC']=sodatecc
        OCC_OCC_HDR_tbl['UNIQUE_CNTR']=unique_cntr 
        OCC_OCC_HDR_tbl['SO_MONTHLY']=0
        OCC_OCC_HDR_tbl['SO_FRAC']=0
        OCC_OCC_HDR_tbl['SO_NRC']=0
        OCC_OCC_HDR_tbl['SO_BLD']=0
        OCC_OCC_HDR_tbl['INPUT_RECORDS']=str(record_id)

        if bctfocc_id > 0:        
            tmpTblRec={}
            tmpTblRec['BCTFOCC_ID']=bctfocc_id
            tmpTblRec['SO_ID']=so_id
            tmpTblRec['SODATECC']=sodatecc
            tmpTblRec['UNIQUE_CNTR']=unique_cntr
            
            hdr=process_check_exists("CAIMS_OCC_OCC_HDR", tmpTblRec, OCC_OCC_HDR_DEFN_DICT,con,schema,output_log)
            del tmpTblRec 
            if hdr>0:
                writelog("bypassed in process_3015BAN_50",output_log)
                occ_hdr_id=hdr
                del hdr
            else:
                writelog("created in process_3015BAN_50",output_log)
                occ_hdr_id=process_insert_table("CAIMS_OCC_OCC_HDR", OCC_OCC_HDR_tbl,OCC_OCC_HDR_DEFN_DICT,con,schema,"SQ_OCC_OCC_HDR",output_log)
                    
        else:
            writelog("ERROR?: no insert to occ hdr. due to no root record",output_log)

    #    MATCH FD1	  OCC_CK
    #      ON NOMATCH INCLUDE
    #      ON MATCH CONTINUE
    
    #    MATCH CD_FID1  OCC_FID1
    #      ON NOMATCH INCLUDE
    #      ON MATCH CONTINUE
        fid_cntr+=1

        if occ_hdr_id > 0:
            tmpTblRec={}
            tmpTblRec['OCC_HDR_ID']=occ_hdr_id
            tmpTblRec['CD_FID1']=cd_fid1
            tmpTblRec['FD1']=fd1
            tmpTblRec['UNIQUE_CNTR']=fid_cntr
            tmpid=process_check_exists("CAIMS_OCC_OCC_FID1", tmpTblRec, OCC_OCC_FID1_DEFN_DICT,con,schema,output_log)        
            del tmpTblRec
            if tmpid>0:
                occ_fid1_id = tmpid
            else:
                initialize_tbl('OCC_OCC_FID1_tbl') 
                OCC_OCC_FID1_tbl['OCC_HDR_ID']=occ_hdr_id                      
                OCC_OCC_FID1_tbl['CD_FID1']=cd_fid1
                #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
                #not part of the original FOCUS logic
                OCC_OCC_FID1_tbl['UNIQUE_CNTR']=fid_cntr
                OCC_OCC_FID1_tbl['FD1']=fd1
                OCC_OCC_FID1_tbl['FID_DATA1']=fid_data1
                OCC_OCC_FID1_tbl['PIU']=piu
       
                OCC_OCC_FID1_tbl['PIU_SRC_IND']=piu_src_ind  
                OCC_OCC_FID1_tbl['PLU']=plu
                OCC_OCC_FID1_tbl['PVU_SRC_IND']=pvu_src_ind
                OCC_OCC_FID1_tbl['PCT_VOIP_USG']=pct_voip_usg
                OCC_OCC_FID1_tbl['PCT_ORIG_USAGE']=pct_orig_usage 
                OCC_OCC_FID1_tbl['INPUT_RECORDS']=str(record_id)
                occ_fid1_id=process_insert_table("CAIMS_OCC_OCC_FID1", OCC_OCC_FID1_tbl,OCC_OCC_FID1_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID1",output_log)
                    
        else:
            writelog("ERROR:insert to occ_fid1.  No OCC_HDR record", output_log)    

        if occ_fid1_id >0:   
            tmpTblRec={}
            tmpTblRec['OCC_FID1_ID']=occ_fid1_id
            tmpTblRec['CD_FID2']=cd_fid2 
            tmpTblRec['UNIQUE_CNTR']=fid_cntr
            fid2=process_check_exists("CAIMS_OCC_OCC_FID2", tmpTblRec, OCC_OCC_FID2_DEFN_DICT,con,schema,output_log)   
            del tmpTblRec
            if fid2>0:
                occ_fid2_id=fid2
            else:
                initialize_tbl('OCC_OCC_FID2_tbl')
                OCC_OCC_FID2_tbl['OCC_FID1_ID']=occ_fid1_id
                OCC_OCC_FID2_tbl['CD_FID2']=cd_fid2 
                #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
                #not part of the original FOCUS logic
                OCC_OCC_FID2_tbl['UNIQUE_CNTR']=fid_cntr
                OCC_OCC_FID2_tbl['INPUT_RECORDS']=str(record_id)
                occ_fid2_id=process_insert_table("CAIMS_OCC_OCC_FID2", OCC_OCC_FID2_tbl,OCC_OCC_FID2_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID2",output_log)  
        else:
            writelog("ERROR:NO fid2 insert.  No OCC_FID1 record", output_log)
         
    #    MATCH CD_FID2  OCC_FID2
    #      ON NOMATCH INCLUDE
    #      ON MATCH CONTINUE

        ###were not inserting the info table here.... 
        #so dont check if exists
        initialize_tbl('OCC_OCC_INFO_tbl')
        OCC_OCC_INFO_tbl['OCC_FID2_ID']=occ_fid2_id
        OCC_OCC_INFO_tbl['PHRASE_CD']=line[61:64]
        OCC_OCC_INFO_tbl['OCCFROMDTCC']=line[70:78]
        OCC_OCC_INFO_tbl['OCCTHRUDTCC']=line[78:86]
        OCC_OCC_INFO_tbl['FAC_CHG_TYPE']=line[86:87]
        OCC_OCC_INFO_tbl['RATEZONEIR']=line[87:88]
        OCC_OCC_INFO_tbl['RATEZONEIA']=line[88:89]
        OCC_OCC_INFO_tbl['SUP_AMT_IR']=convertnumber(line[89:100],2)
        OCC_OCC_INFO_tbl['SUP_AMT_IA']=convertnumber(line[100:111],2) 
        OCC_OCC_INFO_tbl['SUP_AMT_IRIA']=convertnumber(line[111:122],2)
        OCC_OCC_INFO_tbl['SUP_AMT_IAIA']=convertnumber(line[122:133],2)
        OCC_OCC_INFO_tbl['SUP_AMT_NONJ']=convertnumber(line[133:144],2) 
        OCC_OCC_INFO_tbl['PCT_ORIG_USG']=convertnumber(line[188:191],0)
        OCC_OCC_INFO_tbl['PIU_INFO']=convertnumber(line[218:221],0)
        OCC_OCC_INFO_tbl['RC_NRC_IND']=line[222:223]
        OCC_OCC_INFO_tbl['ACC_TYPE']=line[224:225]
        OCC_OCC_INFO_tbl['INPUT_RECORDS']=str(record_id)
        ##DO NOT INSERT>... RECORD 301501 - Next will do the insert

#    MATCH PHRASE_CD  OCC_INFO
#         ON NOMATCH INCLUDE
#         ON MATCH INCLUDE    

    elif record_id=='301501':
        "301501"
        #                         [110:121]        [121:122] [122:123]
        #FIXFORM ON BCTFBDTI X110 SUP_AMT_LO/Z11.2 X1 RATEZONEIRIA/1
        #        [123:124]      [124:125]     [125:130]  [130:133]      [133:160]
        #FIXFORM RATEZONEIAIA/1 RATEZONELOC/1 X5         PHCD_PFBAND1/3 X27
        #        [160:163]      [163:179]       [179:180]    [180:181]
        #FIXFORM PHCD_PFBAND2/3 AUDIT_NUMBER/16 PHCD_PFIIR/1 PHCD_PFIIA/1
        #        [181:182]      [182:198] [198:199]      [199:200]
        #FIXFORM PHCD_PFIIAIA/1 X16       BUS_RSDC_IND/1 PHCD_PFIIRIA/1 X25
        if occ_fid2_id > 0:
            #no INITIALIZE
            OCC_OCC_INFO_tbl['SUP_AMT_LO']=convertnumber(line[110:121],2)
            OCC_OCC_INFO_tbl['RATEZONEIRIA']=line[122:123]
            OCC_OCC_INFO_tbl['RATEZONEIAIA']=line[123:124]
            OCC_OCC_INFO_tbl['RATEZONELOC']=line[124:125]
            OCC_OCC_INFO_tbl['PHCD_PFBAND1']=line[130:133] 
            OCC_OCC_INFO_tbl['PHCD_PFBAND2']=line[160:163]        
            OCC_OCC_INFO_tbl['AUDIT_NUMBER']=line[163:179]
            OCC_OCC_INFO_tbl['PHCD_PFIIR']=line[179:180] 
            OCC_OCC_INFO_tbl['PHCD_PFIIA']=line[180:181]
            OCC_OCC_INFO_tbl['PHCD_PFIIAIA']=line[181:182]         
            OCC_OCC_INFO_tbl['BUS_RSDC_IND']=line[198:199]
            OCC_OCC_INFO_tbl['PHCD_PFIIRIA']=line[199:200]
            OCC_OCC_INFO_tbl['INPUT_RECORDS']+=","+str(record_id)
            occ_info_id=process_insert_table("CAIMS_OCC_OCC_INFO", OCC_OCC_INFO_tbl,OCC_OCC_INFO_DEFN_DICT,con,schema,"SQ_OCC_OCC_INFO",output_log)
        else:
            writelog("ERROR: Cant insert OCC_INFO.   No parent FID2 record records", output_log)
                         
    writelog(record_id+"--"+"leaving process_3015BAN_50",output_log)


def process_3015SO_50():
    global pflag
    global occ_hdr_id
    global occ_fid1_id
    global occ_fid2_id
    global occ_info_id
    global unique_cntr
    global fid_cntr
    global bctfocc_id
    global sodatecc
    global fd1
    global cd_fid1
    global fid_data1
    global piu
    global pflag
    global unique_cntr
    global piu_src_ind
    global plu
    global pvu_src_ind
    global pct_voip_usg
    global pct_orig_usage     
    
    
    
    
    
    
    
    
    writelog(record_id+"--"+"BBBBBB:3015SO_50",output_log)
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
#        [30:61]   [61:64]      [64:70]
#FIXFORM X31       PHRASE_CD/A3 X6
#        [70:78]       [78:86]       [86:87]         [87:88]
#FIXFORM OCCFROMDTCC/8 OCCTHRUDTCC/8 FAC_CHG_TYPE/A1 RATEZONEIR/1
#        [88:89]   
#FIXFORM RATEZONEIA/1
#        [89:100]         [100:111]        [111:122]
#FIXFORM SUP_AMT_IR/Z11.2 SUP_AMT_IA/Z11.2 SUP_AMT_IRIA/Z11.2
#        [122:133]          [133:144]          [144:180]
#FIXFORM SUP_AMT_IAIA/Z11.2 SUP_AMT_NONJ/Z11.2 X36
#        [180:188] [188:191]      [191:218]          [218:221]   
#FIXFORM X8        PCT_ORIG_USG/3 X1 X1 X1 X15 X8 X1 PIU_INFO/3
#        [221:222] [222:223]     [223:224]  [224:225]
#FIXFORM X1        RC_NRC_IND/A1 X1         ACC_TYPE/1 X6


    
    if record_id == '301500':
        initialize_tbl('OCC_OCC_HDR_tbl')
        OCC_OCC_HDR_tbl['BCTFOCC_ID']=bctfocc_id
        OCC_OCC_HDR_tbl['SO_ID']='nl'
        OCC_OCC_HDR_tbl['SODATECC']=sodatecc
        OCC_OCC_HDR_tbl['UNIQUE_CNTR']=unique_cntr 
        OCC_OCC_HDR_tbl['SO_MONTHLY']=0
        OCC_OCC_HDR_tbl['SO_FRAC']=0
        OCC_OCC_HDR_tbl['SO_NRC']=0
        OCC_OCC_HDR_tbl['SO_BLD']=0
        OCC_OCC_HDR_tbl['INPUT_RECORDS']=str(record_id)
        
        if bctfocc_id > 0:
            tmpTblRec={}
            tmpTblRec['BCTFOCC_ID']=bctfocc_id
            tmpTblRec['SO_ID']=OCC_OCC_HDR_tbl['SO_ID']
            tmpTblRec['SODATECC']=sodatecc
            tmpTblRec['UNIQUE_CNTR']=unique_cntr
            fd1='nl'
            hdr=process_check_exists("CAIMS_OCC_OCC_HDR", tmpTblRec, OCC_OCC_HDR_DEFN_DICT,con,schema,output_log)
            del tmpTblRec
            if hdr>0:
                writelog("bypassed in process_3015SO_50",output_log)
                occ_hdr_id=hdr
                del hdr
            else:
                writelog("created in process_3015SO_50",output_log)
                occ_hdr_id=process_insert_table("CAIMS_OCC_OCC_HDR", OCC_OCC_HDR_tbl,OCC_OCC_HDR_DEFN_DICT,con,schema,"SQ_OCC_OCC_HDR",output_log)
                
        else:
            writelog("ERROR?: no insert to occ hdr. due to no root record",output_log)
         
        fid_cntr+=1
        
        initialize_tbl('OCC_OCC_FID1_tbl') 
        OCC_OCC_FID1_tbl['OCC_HDR_ID']=occ_hdr_id                      
        OCC_OCC_FID1_tbl['CD_FID1']='nl' 
        #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
        #not part of the original FOCUS logic
        OCC_OCC_FID1_tbl['UNIQUE_CNTR']=fid_cntr
        OCC_OCC_FID1_tbl['FD1']=fd1
        OCC_OCC_FID1_tbl['FID_DATA1']=fid_data1
        OCC_OCC_FID1_tbl['PIU']=piu     
        OCC_OCC_FID1_tbl['PIU_SRC_IND']=piu_src_ind  
        OCC_OCC_FID1_tbl['PLU']=plu
        OCC_OCC_FID1_tbl['PVU_SRC_IND']=pvu_src_ind
        OCC_OCC_FID1_tbl['PCT_VOIP_USG']=pct_voip_usg
        OCC_OCC_FID1_tbl['PCT_ORIG_USAGE']=pct_orig_usage 
        OCC_OCC_FID1_tbl['INPUT_RECORDS']=str(record_id)
    
        if occ_hdr_id > 0:
            occ_fid1_id=process_insert_table("CAIMS_OCC_OCC_FID1", OCC_OCC_FID1_tbl,OCC_OCC_FID1_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID1",output_log)
        else:
            writelog("ERROR:insert to occfid1 skipped since there was not occ_hdr record", output_log)
     
     
        initialize_tbl('OCC_OCC_FID2_tbl')
        OCC_OCC_FID2_tbl['OCC_FID1_ID']=occ_fid1_id
        OCC_OCC_FID2_tbl['CD_FID2']='nl'
        #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
        #not part of the original FOCUS logic
        OCC_OCC_FID2_tbl['UNIQUE_CNTR']=fid_cntr
        OCC_OCC_FID2_tbl['INPUT_RECORDS']=str(record_id)
#NEED TO CHECK EXISTS HERE           
        if occ_fid1_id >0:   
            occ_fid2_id=process_insert_table("CAIMS_OCC_OCC_FID2", OCC_OCC_FID2_tbl,OCC_OCC_FID2_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID2",output_log)
        else:
            writelog("ERROR: fid2 insert skipped due to no fid1 record", output_log) 
     
        initialize_tbl('OCC_OCC_INFO_tbl')
        OCC_OCC_INFO_tbl['OCC_FID2_ID']=occ_fid2_id
        OCC_OCC_INFO_tbl['PHRASE_CD']=line[61:64]
        OCC_OCC_INFO_tbl['OCCFROMDTCC']=line[70:78]
        OCC_OCC_INFO_tbl['OCCTHRUDTCC']=line[78:86]
        OCC_OCC_INFO_tbl['FAC_CHG_TYPE']=line[86:87]
        OCC_OCC_INFO_tbl['RATEZONEIR']=line[87:88]
        OCC_OCC_INFO_tbl['RATEZONEIA']=line[88:89]
        OCC_OCC_INFO_tbl['SUP_AMT_IR']=convertnumber(line[89:100],2)
        OCC_OCC_INFO_tbl['SUP_AMT_IA']=convertnumber(line[100:111],2)  
        OCC_OCC_INFO_tbl['SUP_AMT_IRIA']=convertnumber(line[111:122],2)
        OCC_OCC_INFO_tbl['SUP_AMT_IAIA']=convertnumber(line[122:133],2)
        OCC_OCC_INFO_tbl['SUP_AMT_NONJ']=convertnumber(line[133:144],2) 
        OCC_OCC_INFO_tbl['PCT_ORIG_USG']=convertnumber(line[188:191],0)
        OCC_OCC_INFO_tbl['PIU_INFO']=convertnumber(line[218:221],0)
        OCC_OCC_INFO_tbl['RC_NRC_IND']=line[222:223]
        OCC_OCC_INFO_tbl['ACC_TYPE']=line[224:225]
        OCC_OCC_INFO_tbl['INPUT_RECORDS']=str(record_id)
    elif record_id=='301501':
        "301501"
        #                         [110:121]        [121:122] [122:123]
        #FIXFORM ON BCTFBDTI X110 SUP_AMT_LO/Z11.2 X1 RATEZONEIRIA/1
        #        [123:124]      [124:125]     [125:130]  [130:133]      [133:160]
        #FIXFORM RATEZONEIAIA/1 RATEZONELOC/1 X5         PHCD_PFBAND1/3 X27
        #        [160:163]      [163:179]       [179:180]    [180:181]
        #FIXFORM PHCD_PFBAND2/3 AUDIT_NUMBER/16 PHCD_PFIIR/1 PHCD_PFIIA/1
        #        [181:182]      [182:198] [198:199]      [199:200]
        #FIXFORM PHCD_PFIIAIA/1 X16       BUS_RSDC_IND/1 PHCD_PFIIRIA/1 X25      

        OCC_OCC_INFO_tbl['SUP_AMT_LO']=convertnumber(line[110:121],2)
        OCC_OCC_INFO_tbl['RATEZONEIRIA']=line[122:123]
        OCC_OCC_INFO_tbl['RATEZONEIAIA']=line[123:124]
        OCC_OCC_INFO_tbl['RATEZONELOC']=line[124:125]
        OCC_OCC_INFO_tbl['PHCD_PFBAND1']=line[130:133] 
        OCC_OCC_INFO_tbl['PHCD_PFBAND2']=line[160:163]        
        OCC_OCC_INFO_tbl['AUDIT_NUMBER']=line[163:179]
        OCC_OCC_INFO_tbl['PHCD_PFIIR']=line[179:180] 
        OCC_OCC_INFO_tbl['PHCD_PFIIA']=line[180:181]
        OCC_OCC_INFO_tbl['PHCD_PFIIAIA']=line[181:182]         
        OCC_OCC_INFO_tbl['BUS_RSDC_IND']=line[198:199]
        OCC_OCC_INFO_tbl['PHCD_PFIIRIA']=line[199:200]

        OCC_OCC_INFO_tbl['INPUT_RECORDS']+=","+str(record_id)
        
        if occ_fid2_id > 0:        
        
            occ_info_id=process_insert_table("CAIMS_OCC_OCC_INFO", OCC_OCC_INFO_tbl,OCC_OCC_INFO_DEFN_DICT,con,schema,"SQ_OCC_OCC_INFO",output_log)
        else:
            writelog("ERROR: NO write of occ-info since there is no fid2 header record", output_log)   
 
#    MATCH EOB_DATE ACNA BAN  ROOTREC
#    ON NOMATCH TYPE "CASE 3015SO_50  - NOMATCH DATE ACNA BAN - REJECTED"
#      ON NOMATCH REJECT
#      ON MATCH COMPUTE
#                 SODATECC=SODATECC;
#                 SO_ID=SO_ID;
#  UNIQUE_CNTR=UNIQUE_CNTR;
#                 FD1=' ';
#                 CD_FID1=' ';
#                 FID_DATA1=' ';
#                 PIU_INFO=0;
#                 ASG=' ';
#                 CD_FID2=' ';
#                 FID_DATA2=' ';
#      ON MATCH CONTINUE
 
#    MATCH SODATECC SO_ID UNIQUE_CNTR  OCC_HDR
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH FD1	OCC_CK
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH CD_FID1  OCC_FID1
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH CD_FID2  OCC_FID2
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH PHRASE_CD  OCC_INFO
#         ON NOMATCH INCLUDE
#         ON MATCH INCLUDE
    writelog(record_id+"--"+"leaving process_3015SO_50",output_log)   
    
def process_3015CKT_50():
    global pflag
    global occ_info_id
    global occ_hdr_id
    global occ_fid1_id
    global occ_fid2_id
    global unique_cntr
    global fid_cntr
    global bctfocc_id
    global sodatecc
    global so_id
    global cd_fid1
    global cd_fid2
    global fid_data2
    global fid_data1   
    global fd1
    global piu
    global piu_src_ind
    global plu
    global pvu_src_ind
    global pct_voip_usg
    global pct_orig_usage       
    
    
    
    
    
    
    "301500"
#    same fixform process_3015BAN_50
    writelog(record_id+"--"+"BBBBBB:3015CKT_50",output_log)
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
#        [30:61]   [61:64]      [64:70]
#FIXFORM X31       PHRASE_CD/A3 X6
#        [70:78]       [78:86]       [86:87]         [87:88]
#FIXFORM OCCFROMDTCC/8 OCCTHRUDTCC/8 FAC_CHG_TYPE/A1 RATEZONEIR/1
#        [88:89]   
#FIXFORM RATEZONEIA/1
#        [89:100]         [100:111]        [111:122]
#FIXFORM SUP_AMT_IR/Z11.2 SUP_AMT_IA/Z11.2 SUP_AMT_IRIA/Z11.2
#        [122:133]          [133:144]          [144:180]
#FIXFORM SUP_AMT_IAIA/Z11.2 SUP_AMT_NONJ/Z11.2 X36
#        [180:188] [188:191]      [191:218]          [218:221]   
#FIXFORM X8        PCT_ORIG_USG/3 X1 X1 X1 X15 X8 X1 PIU_INFO/3
#        [221:222] [222:223]     [223:224]  [224:225]
#FIXFORM X1        RC_NRC_IND/A1 X1         ACC_TYPE/1 X6
    
    if record_id == '301500':
        if bctfocc_id > 0:
    #        set  SODATECC=SODATECC;
    #                 SO_ID=so_id;
    #           UNIQUE_CNTR=UNIQUE_CNTR;
    #                 FD1=fd1;
    #                 cd_fid1=CD_FID1;
             cd_fid2='nl'
             fid_data2=' '
                
             tmpTblRec={}
             tmpTblRec['BCTFOCC_ID']=bctfocc_id
             tmpTblRec['SO_ID']=so_id
             tmpTblRec['SODATECC']=sodatecc
             tmpTblRec['UNIQUE_CNTR']=unique_cntr
             hdr=process_check_exists("CAIMS_OCC_OCC_HDR", tmpTblRec, OCC_OCC_HDR_DEFN_DICT,con,schema,output_log)
             del tmpTblRec
             if hdr>0:
                 writelog("bypassed in process_3015CKT_50",output_log)
                 occ_hdr_id=hdr
                 del hdr
             else:
                 initialize_tbl('OCC_OCC_HDR_tbl')
                 OCC_OCC_HDR_tbl['BCTFOCC_ID']=bctfocc_id
                 OCC_OCC_HDR_tbl['SO_ID']=so_id
                 OCC_OCC_HDR_tbl['SODATECC']=sodatecc
                 OCC_OCC_HDR_tbl['UNIQUE_CNTR']=unique_cntr 
                 occ_hdr_id=process_insert_table("CAIMS_OCC_OCC_HDR", OCC_OCC_HDR_tbl,OCC_OCC_HDR_DEFN_DICT,con,schema,"SQ_OCC_OCC_HDR",output_log)
                 writelog("created in process_3015CKT_50",output_log)
             if occ_hdr_id > 0:
                tmpTblRec={}
                tmpTblRec['OCC_HDR_ID']=occ_hdr_id
                tmpTblRec['CD_FID1']=cd_fid1
                tmpTblRec['UNIQUE_CNTR']=fid_cntr 
                tmpid=process_check_exists("CAIMS_OCC_OCC_FID1", tmpTblRec, OCC_OCC_FID1_DEFN_DICT,con,schema,output_log)  
                del tmpTblRec
                if tmpid>0:
                    occ_fid1_id = tmpid
                else:
                    initialize_tbl('OCC_OCC_FID1_tbl') 
                    OCC_OCC_FID1_tbl['OCC_HDR_ID']=occ_hdr_id                      
                    OCC_OCC_FID1_tbl['CD_FID1']=cd_fid1 
                    #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
                    #not part of the original FOCUS logic
                    OCC_OCC_FID1_tbl['UNIQUE_CNTR']=fid_cntr 
                    OCC_OCC_FID1_tbl['FD1']=fd1
                    OCC_OCC_FID1_tbl['INPUT_RECORDS']=str(record_id)
                    OCC_OCC_FID1_tbl['FID_DATA1']=fid_data1
                    OCC_OCC_FID1_tbl['PIU']=piu     
                    OCC_OCC_FID1_tbl['PIU_SRC_IND']=piu_src_ind  
                    OCC_OCC_FID1_tbl['PLU']=plu
                    OCC_OCC_FID1_tbl['PVU_SRC_IND']=pvu_src_ind
                    OCC_OCC_FID1_tbl['PCT_VOIP_USG']=pct_voip_usg
                    OCC_OCC_FID1_tbl['PCT_ORIG_USAGE']=pct_orig_usage 
                    occ_fid1_id=process_insert_table("CAIMS_OCC_OCC_FID1", OCC_OCC_FID1_tbl,OCC_OCC_FID1_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID1",output_log)              
                    
             else:
                writelog("ERROR:insert to occfid1 skipped since there was no occ_hdr record", output_log)
                
             if occ_fid1_id >0:  
                tmpTblRec={}
                tmpTblRec['OCC_FID1_ID']=occ_fid1_id
                tmpTblRec['CD_FID2']=cd_fid2
                tmpTblRec['UNIQUE_CNTR']=fid_cntr
                tmpid=process_check_exists("CAIMS_OCC_OCC_FID2", tmpTblRec, OCC_OCC_FID2_DEFN_DICT,con,schema,output_log)  
                del tmpTblRec
                if tmpid >0:
                    occ_fid2_id=tmpid
                else:
                    initialize_tbl('OCC_OCC_FID2_tbl')
                    OCC_OCC_FID2_tbl['OCC_FID1_ID']=occ_fid1_id
                    OCC_OCC_FID2_tbl['CD_FID2']=cd_fid2
                    #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
                    #not part of the original FOCUS logic
                    OCC_OCC_FID2_tbl['UNIQUE_CNTR']=fid_cntr
                    OCC_OCC_FID2_tbl['INPUT_RECORDS']=str(record_id)
                    occ_fid2_id=process_insert_table("CAIMS_OCC_OCC_FID2", OCC_OCC_FID2_tbl,OCC_OCC_FID2_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID2",output_log)
             else:
                    writelog("ERROR: fid2 insert skipped due to no fid1 record", output_log) 
             
             if occ_fid2_id > 0:
                initialize_tbl('OCC_OCC_INFO_tbl')
                OCC_OCC_INFO_tbl['OCC_FID2_ID']=occ_fid2_id
                OCC_OCC_INFO_tbl['PHRASE_CD']=line[61:64]
                OCC_OCC_INFO_tbl['OCCFROMDTCC']=line[70:78]
                OCC_OCC_INFO_tbl['OCCTHRUDTCC']=line[78:86]
                OCC_OCC_INFO_tbl['FAC_CHG_TYPE']=line[86:87]
                OCC_OCC_INFO_tbl['RATEZONEIR']=line[87:88]
                OCC_OCC_INFO_tbl['RATEZONEIA']=line[88:89]
                OCC_OCC_INFO_tbl['SUP_AMT_IR']=convertnumber(line[89:100],2)
                OCC_OCC_INFO_tbl['SUP_AMT_IA']=convertnumber(line[100:111],2)  
                OCC_OCC_INFO_tbl['SUP_AMT_IRIA']=convertnumber(line[111:122],2)
                OCC_OCC_INFO_tbl['SUP_AMT_IAIA']=convertnumber(line[122:133],2)
                OCC_OCC_INFO_tbl['SUP_AMT_NONJ']=convertnumber(line[133:144],2)
                OCC_OCC_INFO_tbl['PCT_ORIG_USG']=convertnumber(line[188:191],0)
                OCC_OCC_INFO_tbl['PIU_INFO']=convertnumber(line[218:221],0)
                OCC_OCC_INFO_tbl['RC_NRC_IND']=line[222:223]
                OCC_OCC_INFO_tbl['ACC_TYPE']=line[224:225]
                OCC_OCC_INFO_tbl['INPUT_RECORDS']=str(record_id)
             else:
                writelog("ERROR: no insert to OCC INFO. no parent occ_fid2 record",output_log)        
        else:
           writelog("ERROR: no insert to occ hdr. due to no root record",output_log)    
    elif record_id=='301501':
        "301501"    
         #                         [110:121]        [121:122] [122:123]
        #FIXFORM ON BCTFBDTI X110 SUP_AMT_LO/Z11.2 X1 RATEZONEIRIA/1
        #        [123:124]      [124:125]     [125:130]  [130:133]      [133:160]
        #FIXFORM RATEZONEIAIA/1 RATEZONELOC/1 X5         PHCD_PFBAND1/3 X27
        #        [160:163]      [163:179]       [179:180]    [180:181]
        #FIXFORM PHCD_PFBAND2/3 AUDIT_NUMBER/16 PHCD_PFIIR/1 PHCD_PFIIA/1
        #        [181:182]      [182:198] [198:199]      [199:200]
        #FIXFORM PHCD_PFIIAIA/1 X16       BUS_RSDC_IND/1 PHCD_PFIIRIA/1 X25      

        OCC_OCC_INFO_tbl['SUP_AMT_LO']=convertnumber(line[110:121],2)
        OCC_OCC_INFO_tbl['RATEZONEIRIA']=line[122:123]
        OCC_OCC_INFO_tbl['RATEZONEIAIA']=line[123:124]
        OCC_OCC_INFO_tbl['RATEZONELOC']=line[124:125]
        OCC_OCC_INFO_tbl['PHCD_PFBAND1']=line[130:133] 
        OCC_OCC_INFO_tbl['PHCD_PFBAND2']=line[160:163]        
        OCC_OCC_INFO_tbl['AUDIT_NUMBER']=line[163:179]
        OCC_OCC_INFO_tbl['PHCD_PFIIR']=line[179:180] 
        OCC_OCC_INFO_tbl['PHCD_PFIIA']=line[180:181]
        OCC_OCC_INFO_tbl['PHCD_PFIIAIA']=line[181:182]         
        OCC_OCC_INFO_tbl['BUS_RSDC_IND']=line[198:199]
        OCC_OCC_INFO_tbl['PHCD_PFIIRIA']=line[199:200]

        OCC_OCC_INFO_tbl['INPUT_RECORDS']+=","+str(record_id)

        if occ_fid2_id > 0:        
            occ_info_id=process_insert_table("CAIMS_OCC_OCC_INFO", OCC_OCC_INFO_tbl,OCC_OCC_INFO_DEFN_DICT,con,schema,"SQ_OCC_OCC_INFO",output_log)
        else:
            writelog("Skipping write of occ-info since there is no header record", output_log)        
#    MATCH EOB_DATE ACNA BAN  ROOTREC
#    ON NOMATCH TYPE "CASE 3015CKT_50- NOMATCH DATE ACNA BAN - REJECTED"
#      ON NOMATCH REJECT
#      ON MATCH COMPUTE
#                 SODATECC=SODATECC;
#                 SO_ID=SO_ID;
#  UNIQUE_CNTR=UNIQUE_CNTR;
#                 FD1=FD1;
#                 CD_FID1=CD_FID1;
#                 CD_FID2=' ';
#                 FID_DATA2=' ';
#      ON MATCH CONTINUE
         
#    MATCH SODATECC SO_ID UNIQUE_CNTR  OCC_HDR
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH FD1  OCC_CK
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH CD_FID1  OCC_FID1
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH CD_FID2  OCC_FID2
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH PHRASE_CD  OCC_INFO
#         ON NOMATCH INCLUDE
#         ON MATCH INCLUDE
    writelog(record_id+"--"+"leaving process_3015CKT_50",output_log)   
    
def process_301550DRVR():    
    global pflag
    global occ_pidinf_id
    global bctfocc_id
    global occ_hdr_id
    global occ_info_id
    global occ_fid1_id
    global occ_fid2_id
    global unique_cntr
    global fd1
    "301550" 
    writelog(record_id+"--"+"CCCCC:301550DRVR  - BRANCH TO OCC-INFO - PIDINF",output_log)
      
    if pflag == 'L':
        writelog("CALLING 301550REC",output_log )
        process_301550REC()
    elif pflag == ' ':
        writelog("CALLING 301550BAN",output_log )
        process_301550BAN()
    elif pflag == 'S':
        writelog("CALLING 301550SO",output_log )
        process_301550SO()
    elif pflag =='C':
        writelog("CALLING 301550CKT",output_log )
        process_301550CKT()
    
    writelog(record_id+"--"+"leaving process_301550DRVR",output_log) 

def process_301550REC():
    global bctfocc_id
    global occ_info_id
    global occ_fid2_id
    global occ_pidinf_id
    writelog(record_id+"--"+"CCCCCCCCCC:301550REC  OCC_PIDINF",output_log)
  
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
#        [30:61] [61:64]      [64:90]       [90:95]
#FIXFORM X31     PHRASE_CD/A3 D_PLAN_ID/A26 D_DIS_PERCNT/Z5.2
#        [95:106]                   [106:107]
#FIXFORM D_DIS_PLAN_TOT_RVNUE/Z11.2 D_VOL_PLN_TYP_IND/A1
#        [107:108]             [108:109]
#FIXFORM D_DIS_INTERVAL_IND/A1 D_DISC_APPL_IND/A1
#        [109:110]               [110:122]     [122:225]
#FIXFORM D_PRICE_PLAN_TYP_IND/A1 SO_NUMBER/A12 X103
    
    #dont check if exists... we need and OCC_INFO record for each record in the ip file.
    #dont check if exists... we need and OCC_INFO record for each record in the ip file.
    #dont check if exists... we need and OCC_INFO record for each record in the ip file.
    #dont check if exists... we need and OCC_INFO record for each record in the ip file.
    if occ_fid2_id > 0:
        initialize_tbl('OCC_OCC_INFO_tbl')
        OCC_OCC_INFO_tbl['OCC_FID2_ID']=occ_fid2_id
        OCC_OCC_INFO_tbl['PHRASE_CD']=line[61:64]

        occ_info_id=process_insert_table("CAIMS_OCC_OCC_INFO", OCC_OCC_INFO_tbl,OCC_OCC_INFO_DEFN_DICT,con,schema,"SQ_OCC_OCC_INFO",output_log)
    else:
        writelog("ERROR: NO FID2 record.  Can insert OCC_INFO",output_log)

    initialize_tbl('OCC_PIDINF_tbl')
    OCC_PIDINF_tbl['OCC_INFO_ID']=occ_info_id
    OCC_PIDINF_tbl['D_PLAN_ID']=line[64:90]
    OCC_PIDINF_tbl['D_DIS_PERCNT']=convertnumber(line[90:95],2)
    OCC_PIDINF_tbl['D_DIS_PLAN_TOT_RVNUE']=convertnumber(line[95:106],2)
    OCC_PIDINF_tbl['D_VOL_PLN_TYP_IND']=line[106:107] 
    OCC_PIDINF_tbl['D_DIS_INTERVAL_IND']=line[107:108]
    OCC_PIDINF_tbl['D_DISC_APPL_IND']=line[108:109]
    OCC_PIDINF_tbl['D_PRICE_PLAN_TYP_IND']=line[109:110]
    OCC_PIDINF_tbl['SO_NUMBER']=line[110:122] 
    OCC_PIDINF_tbl['INPUT_RECORDS']=str(record_id)
    
    if occ_info_id > 0:
        occ_pidinf_id=process_insert_table("CAIMS_OCC_PIDINF", OCC_PIDINF_tbl,OCC_PIDINF_DEFN_DICT,con,schema,"SQ_OCC_PIDINF",output_log)
    else:
        writelog("ERROR:No Parent record. Skipping OCC_PIDINF insert. ", output_log)

#    MATCH PHRASE_CD  OCC_INFO
#         ON NOMATCH INCLUDE
#         ON MATCH CONTINUE
#    MATCH D_PLAN_ID  PIDINF
#         ON NOMATCH INCLUDE
#         ON MATCH INCLUDE
    writelog(record_id+"--"+"leaving process_301550REC",output_log)  
  
def process_301550BAN():
    global bctfocc_id
    global occ_hdr_id
    global occ_info_id   
    global occ_pidinf_id
    global unique_cntr
    global fid_cntr
    global occ_fid1_id
    global occ_fid2_id
    global fd1
    writelog(record_id+"--"+"CCCCCCCCCC:301550BAN PIDINF",output_log)
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
##       [30:61] [61:64]      [64:90]       [90:95]
#FIXFORM X31     PHRASE_CD/A3 D_PLAN_ID/A26 D_DIS_PERCNT/Z5.2
##        [95:106]                   [106:107]
#FIXFORM D_DIS_PLAN_TOT_RVNUE/Z11.2 D_VOL_PLN_TYP_IND/A1
##        [107:108]             [108:109]
#FIXFORM D_DIS_INTERVAL_IND/A1 D_DISC_APPL_IND/A1
##        [109:110]               [110:122]     [122:225]
#FIXFORM D_PRICE_PLAN_TYP_IND/A1 SO_NUMBER/A12 X103
     
    initialize_tbl('OCC_OCC_HDR_tbl')
    OCC_OCC_HDR_tbl['BCTFOCC_ID']=bctfocc_id
    OCC_OCC_HDR_tbl['SO_ID']='nl'
    OCC_OCC_HDR_tbl['SODATECC']='19000101'  #set to 0
    OCC_OCC_HDR_tbl['UNIQUE_CNTR']=unique_cntr 
    OCC_OCC_HDR_tbl['SO_MONTHLY']=0
    OCC_OCC_HDR_tbl['SO_FRAC']=0
    OCC_OCC_HDR_tbl['SO_NRC']=0
    OCC_OCC_HDR_tbl['SO_BLD']=0
    OCC_OCC_HDR_tbl['INPUT_RECORDS']=str(record_id)

    if bctfocc_id > 0:
        tmpTblRec={}
        tmpTblRec['BCTFOCC_ID']=bctfocc_id
        tmpTblRec['SO_ID']=OCC_OCC_HDR_tbl['SO_ID']
        tmpTblRec['SODATECC']=OCC_OCC_HDR_tbl['SODATECC']        
        fd1='nl'
        hdr=process_check_exists("CAIMS_OCC_OCC_HDR", tmpTblRec, OCC_OCC_HDR_DEFN_DICT,con,schema,output_log)
        del tmpTblRec 
        if hdr>0: 
            writelog("bypassed in process_301550BAN",output_log)
            occ_hdr_id=hdr
        else:
            writelog("created in process_301550BAN",output_log)
            occ_hdr_id=process_insert_table("CAIMS_OCC_OCC_HDR", OCC_OCC_HDR_tbl,OCC_OCC_HDR_DEFN_DICT,con,schema,"SQ_OCC_OCC_HDR",output_log)
        del hdr    
    else:
        writelog("ERROR?: no insert to occ hdr. due to no root record",output_log)  

    if occ_hdr_id > 0:
        tmpTblRec={}
        tmpTblRec['OCC_HDR_ID']=occ_hdr_id
        tmpTblRec['CD_FID1']='nl'
        tmpTblRec['UNIQUE_CNTR']=fid_cntr
        tmpTblRec['FD1']=fd1
        tmpid=process_check_exists("CAIMS_OCC_OCC_FID1", tmpTblRec, OCC_OCC_FID1_DEFN_DICT,con,schema,output_log)        
        del tmpTblRec
        if tmpid>0:
            writelog("BYPASSING create of FID1",output_log)
            occ_fid1_id = tmpid
        else:
            initialize_tbl('OCC_OCC_FID1_tbl') 
            OCC_OCC_FID1_tbl['OCC_HDR_ID']=occ_hdr_id                      
            OCC_OCC_FID1_tbl['CD_FID1']='nl'
            #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
            #not part of the original FOCUS logic
            OCC_OCC_FID1_tbl['UNIQUE_CNTR']=fid_cntr
            OCC_OCC_FID1_tbl['FD1']=fd1
            OCC_OCC_FID1_tbl['INPUT_RECORDS']=str(record_id)
            occ_fid1_id=process_insert_table("CAIMS_OCC_OCC_FID1", OCC_OCC_FID1_tbl,OCC_OCC_FID1_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID1",output_log)
    else:
        writelog("ERROR:insert to occfid1 skipped since there was no occ_hdr record", output_log)
        
    if occ_fid1_id >0:  
        tmpTblRec={}
        tmpTblRec['OCC_FID1_ID']=occ_fid1_id
        tmpTblRec['UNIQUE_CNTR']=fid_cntr
        tmpTblRec['CD_FID2']='nl'        
        fid2=process_check_exists("CAIMS_OCC_OCC_FID2", tmpTblRec, OCC_OCC_FID2_DEFN_DICT,con,schema,output_log)   
        del tmpTblRec
        if fid2>0:
            occ_fid2_id=fid2
        else:
            initialize_tbl('OCC_OCC_FID2_tbl')
            OCC_OCC_FID2_tbl['OCC_FID1_ID']=occ_fid1_id
            OCC_OCC_FID2_tbl['CD_FID2']='nl' 
            #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
            #not part of the original FOCUS logic
            OCC_OCC_FID2_tbl['UNIQUE_CNTR']=fid_cntr
            OCC_OCC_FID2_tbl['INPUT_RECORDS']=str(record_id)    
            occ_fid2_id=process_insert_table("CAIMS_OCC_OCC_FID2", OCC_OCC_FID2_tbl,OCC_OCC_FID2_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID2",output_log)  
    else:
        writelog("ERROR:NO fid2 insert due to no fid1 record", output_log)
   
    tmpTblRec={}
    tmpTblRec['OCC_FID2_ID']=occ_fid2_id
    tmpTblRec['PHRASE_CD']=line[61:64]        
    inf=process_check_exists("CAIMS_OCC_OCC_INFO", tmpTblRec, OCC_OCC_INFO_DEFN_DICT,con,schema,output_log)     
    if inf>0:
        occ_info_id=inf
    else:
        writelog("ERROR: OCCINFO not found?", output_log) 
    del tmpTblRec, inf
    
    if occ_info_id >0:
        initialize_tbl('OCC_PIDINF_tbl')
        OCC_PIDINF_tbl['OCC_INFO_ID']=occ_info_id
        OCC_PIDINF_tbl['D_PLAN_ID']=line[64:90]
        OCC_PIDINF_tbl['D_DIS_PERCNT']=convertnumber(line[90:95],2)
        OCC_PIDINF_tbl['D_DIS_PLAN_TOT_RVNUE']=convertnumber(line[95:106],2)
        OCC_PIDINF_tbl['D_VOL_PLN_TYP_IND']=line[106:107] 
        OCC_PIDINF_tbl['D_DIS_INTERVAL_IND']=line[107:108]
        OCC_PIDINF_tbl['D_DISC_APPL_IND']=line[108:109]
        OCC_PIDINF_tbl['D_PRICE_PLAN_TYP_IND']=line[109:110]
        OCC_PIDINF_tbl['SO_NUMBER']=line[110:122] 
        OCC_PIDINF_tbl['INPUT_RECORDS']=str(record_id)          
        occ_pidinf_id=process_insert_table("CAIMS_OCC_PIDINF", OCC_PIDINF_tbl,OCC_PIDINF_DEFN_DICT,con,schema,"SQ_OCC_PIDINF",output_log)
        
    else:
        writelog("ERROR: cant write pidinf.  No FID2 PARENT RECORD",output_log)

#    MATCH EOB_DATE ACNA BAN  ROOTREC
#     ON NOMATCH TYPE "CASE 301550BAN - NOMATCH DATE ACNA BAN - REJECT"
#      ON NOMATCH REJECT
#      ON MATCH COMPUTE
#                 SODATECC=0;
#                 SO_ID=' ';
#                 SO_MONTHLY=0;
#                 SO_FRAC=0;
#                 SO_NRC=0;
#                 SO_BLD=0;
#                 PON=' ';
#                 FD1=' ';
#                 CD_FID1=' ';
#                 FID_DATA1=' ';
#                 PIU_INFO=0;
#                 ASG=' ';
#                 CD_FID2=' ';
#                 FID_DATA2=' ';
#    UNIQUE_CNTR=UNIQUE_CNTR;
#      ON MATCH CONTINUE
#    MATCH SODATECC SO_ID UNIQUE_CNTR  OCC_HDR
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH FD1  OCC_CK
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH CD_FID1  OCC_FID1
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH CD_FID2  OCC_FID2
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH PHRASE_CD  OCC_INFO
#         ON NOMATCH INCLUDE
#         ON MATCH CONTINUE
#    MATCH D_PLAN_ID   PIDINF
#         ON NOMATCH INCLUDE
#         ON MATCH INCLUDE
    writelog(record_id+"--"+"leaving process_301550BAN",output_log)

def process_301550SO():
    global bctfocc_id
    global occ_hdr_id    
    global occ_fid1_id
    global occ_fid2_id
    global occ_info_id   
    global occ_pidinf_id
    global fid_cntr
    global unique_cntr
    global cd_fid1
    global cd_fid2
    global fd1
    global sodatecc
    global so_id
    writelog(record_id+"--"+"CCCCCCCCCC:301550SO PIDINF",output_log)
#-* **************************************************************
#-*   CASS RECORD ID = ND03
#-*     THIS CASE LOADS SERVICE ORDER LEVEL CHARGES THAT DON'T HAVE A
#-*     CIRCUIT OR LOCATION RECORD .
#-* **************************************************************
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
#        [30:61] [61:64]      [64:90]       [90:95]
#FIXFORM X31     PHRASE_CD/A3 D_PLAN_ID/A26 D_DIS_PERCNT/Z5.2
#        [95:106]                   [106:107]
#FIXFORM D_DIS_PLAN_TOT_RVNUE/Z11.2 D_VOL_PLN_TYP_IND/A1
#        [107:108]             [108:109]
#FIXFORM D_DIS_INTERVAL_IND/A1 D_DISC_APPL_IND/A1
#        [109:110]               [110:122]     [122:225]
#FIXFORM D_PRICE_PLAN_TYP_IND/A1 SO_NUMBER/A12 X103    
#        set  SODATECC=SODATECC;
#                 SO_ID=so_id;
#           UNIQUE_CNTR=UNIQUE_CNTR;
#                 FD1=fd1;
    if bctfocc_id > 0:    
         cd_fid1='nl'
         cd_fid2='nl'
         fd1='nl'
            
         tmpTblRec={}
         tmpTblRec['BCTFOCC_ID']=bctfocc_id
         tmpTblRec['SO_ID']=so_id
         tmpTblRec['SODATECC']=sodatecc
         tmpTblRec['UNIQUE_CNTR']=unique_cntr
         hdr=process_check_exists("CAIMS_OCC_OCC_HDR", tmpTblRec, OCC_OCC_HDR_DEFN_DICT,con,schema,output_log)
         del tmpTblRec
         if hdr>0:
             writelog("bypassed in process_301550SO",output_log)
             occ_hdr_id=hdr
             del hdr
         else:
             writelog("created in process_301550SO",output_log)
             initialize_tbl('OCC_OCC_HDR_tbl')
             OCC_OCC_HDR_tbl['BCTFOCC_ID']=bctfocc_id
             OCC_OCC_HDR_tbl['SO_ID']=so_id
             OCC_OCC_HDR_tbl['SODATECC']=sodatecc
             OCC_OCC_HDR_tbl['UNIQUE_CNTR']=unique_cntr 
             occ_hdr_id=process_insert_table("CAIMS_OCC_OCC_HDR", OCC_OCC_HDR_tbl,OCC_OCC_HDR_DEFN_DICT,con,schema,"SQ_OCC_OCC_HDR",output_log)
          
         if occ_hdr_id > 0:
            tmpTblRec={}
            tmpTblRec['OCC_HDR_ID']=occ_hdr_id
            tmpTblRec['CD_FID1']=cd_fid1
            tmpTblRec['UNIQUE_CNTR']=fid_cntr
            tmpid=process_check_exists("CAIMS_OCC_OCC_FID1", tmpTblRec, OCC_OCC_FID1_DEFN_DICT,con,schema,output_log)  
            del tmpTblRec
            if tmpid>0:
                occ_fid1_id = tmpid
            else:
                initialize_tbl('OCC_OCC_FID1_tbl') 
                OCC_OCC_FID1_tbl['OCC_HDR_ID']=occ_hdr_id                      
                OCC_OCC_FID1_tbl['CD_FID1']=cd_fid1 
                #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
                #not part of the original FOCUS logic
                OCC_OCC_FID1_tbl['UNIQUE_CNTR']=fid_cntr
                OCC_OCC_FID1_tbl['FD1']=fd1
                OCC_OCC_FID1_tbl['INPUT_RECORDS']=str(record_id)
                occ_fid1_id=process_insert_table("CAIMS_OCC_OCC_FID1", OCC_OCC_FID1_tbl,OCC_OCC_FID1_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID1",output_log)
         else:
            writelog("ERROR:insert to occfid1 skipped since there was no occ_hdr record", output_log)
            
         if occ_fid1_id >0:  
            tmpTblRec={}
            tmpTblRec['OCC_FID1_ID']=occ_fid1_id
            tmpTblRec['CD_FID2']=cd_fid2
            tmpTblRec['UNIQUE_CNTR']=fid_cntr
            tmpid=process_check_exists("CAIMS_OCC_OCC_FID2", tmpTblRec, OCC_OCC_FID2_DEFN_DICT,con,schema,output_log)  
            del tmpTblRec
            if tmpid >0:
                occ_fid2_id=tmpid
            else:
                initialize_tbl('OCC_OCC_FID2_tbl')
                OCC_OCC_FID2_tbl['OCC_FID1_ID']=occ_fid1_id
                OCC_OCC_FID2_tbl['CD_FID2']=cd_fid2
                #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
                #not part of the original FOCUS logic
                OCC_OCC_FID2_tbl['UNIQUE_CNTR']=fid_cntr
                OCC_OCC_FID2_tbl['INPUT_RECORDS']=str(record_id)
                occ_fid2_id=process_insert_table("CAIMS_OCC_OCC_FID2", OCC_OCC_FID2_tbl,OCC_OCC_FID2_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID2",output_log)
         else:
                writelog("ERROR: fid2 insert skipped due to no fid1 record", output_log) 

         tmpTblRec={}
         tmpTblRec['OCC_FID2_ID']=occ_fid2_id
         tmpTblRec['PHRASE_CD']=line[61:64]        
         inf=process_check_exists("CAIMS_OCC_OCC_INFO", tmpTblRec, OCC_OCC_INFO_DEFN_DICT,con,schema,output_log)     
         if inf>0:
             occ_info_id=inf
         else:
             writelog("ERROR: OCCINFO not found?", output_log) 
         del tmpTblRec, inf

         if occ_info_id > 0:
             initialize_tbl('OCC_PIDINF_tbl')
             OCC_PIDINF_tbl['OCC_INFO_ID']=occ_info_id
             OCC_PIDINF_tbl['D_PLAN_ID']=line[64:90]
             OCC_PIDINF_tbl['D_DIS_PERCNT']=convertnumber(line[90:95],2)
             OCC_PIDINF_tbl['D_DIS_PLAN_TOT_RVNUE']=convertnumber(line[95:106],2)
             OCC_PIDINF_tbl['D_VOL_PLN_TYP_IND']=line[106:107] 
             OCC_PIDINF_tbl['D_DIS_INTERVAL_IND']=line[107:108]
             OCC_PIDINF_tbl['D_DISC_APPL_IND']=line[108:109]
             OCC_PIDINF_tbl['D_PRICE_PLAN_TYP_IND']=line[109:110]
             OCC_PIDINF_tbl['SO_NUMBER']=line[110:122] 
             OCC_PIDINF_tbl['INPUT_RECORDS']=str(record_id)  
             
             tmpTblRec={}
             tmpTblRec['OCC_INFO_ID']=OCC_PIDINF_tbl['OCC_INFO_ID']
             tmpTblRec['D_PLAN_ID']=OCC_PIDINF_tbl['D_PLAN_ID']
             if process_check_exists("CAIMS_OCC_PIDINF", tmpTblRec, OCC_PIDINF_DEFN_DICT,con,schema,output_log)>0:
                 process_update_table("CAIMS_OCC_PIDINF", OCC_PIDINF_tbl, OCC_PIDINF_DEFN_DICT,con,schema,output_log)
             else:
                 occ_pidinf_id=process_insert_table("CAIMS_OCC_PIDINF", OCC_PIDINF_tbl,OCC_PIDINF_DEFN_DICT,con,schema,"SQ_OCC_PIDINF",output_log)
             del tmpTblRec
         else:
             writelog("no inserting occ_pidinf.  no parent occ_info record", output_log)
#TYPE "*** CASE 301550SOCHRG  PFLAG = <PFLAG"
#    MATCH EOB_DATE ACNA BAN   ROOTREC
#    ON NOMATCH TYPE "CASE 301550SO  - NOMATCH DATE ACNA BAN - REJECT"
#      ON NOMATCH REJECT
#      ON MATCH COMPUTE
#                 SODATECC=SODATECC;
#                 SO_ID=SO_ID;
#  UNIQUE_CNTR=UNIQUE_CNTR;
#                 FD1=' ';
#                 CD_FID1=' ';
#                 FID_DATA1=' ';
#                 PIU_INFO=0;
#                 ASG=' ';
#                 CD_FID2=' ';
#                 FID_DATA2=' ';
#      ON MATCH CONTINUE
#    MATCH SODATECC SO_ID UNIQUE_CNTR  OCC_HDR
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH FD1  OCC_CK
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH CD_FID1  OCC_FID1
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH CD_FID2  OCC_FID2
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH PHRASE_CD  OCC_INFO
#         ON NOMATCH INCLUDE
#         ON MATCH CONTINUE
#    MATCH D_PLAN_ID   PIDINF
#         ON NOMATCH INCLUDE
#         ON MATCH INCLUDE
    writelog(record_id+"--"+"leaving process_301550SO",output_log)
    
    
def process_301550CKT():
    global bctfocc_id
    global occ_hdr_id    
    global occ_fid1_id
    global occ_fid2_id
    global occ_info_id   
    global occ_pidinf_id
    global fid_cntr
    global unique_cntr
    global cd_fid1
    global cd_fid2
    global fd1
    global sodatecc
    global so_id
    writelog(record_id+"--"+"CCCCCCCCCC:301550CKT  PIDINF",output_log)
#-* **************************************************************
#-*   CASS RECORD ID = ND03
#-*     THIS CASE LOADS CIRCUIT LEVEL CHARGES THAT DON'T HAVE A
#-*     LOCATION RECORD .
#-* **************************************************************
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
#        [30:61] [61:64]      [64:90]       [90:95]
#FIXFORM X31     PHRASE_CD/A3 D_PLAN_ID/A26 D_DIS_PERCNT/Z5.2
#        [95:106]                   [106:107]
#FIXFORM D_DIS_PLAN_TOT_RVNUE/Z11.2 D_VOL_PLN_TYP_IND/A1
#        [107:108]             [108:109]
#FIXFORM D_DIS_INTERVAL_IND/A1 D_DISC_APPL_IND/A1
#        [109:110]               [110:122]     [122:225]
#FIXFORM D_PRICE_PLAN_TYP_IND/A1 SO_NUMBER/A12 X103

        
#           SODATECC=SODATECC;
#                 SO_ID=SO_ID;
#  UNIQUE_CNTR=UNIQUE_CNTR;
#                 FD1=FD1;
#                 CD_FID1=CD_FID1;
#                 CD_FID2=' ';
#                 FID_DATA2=' ';
    if bctfocc_id > 0:        
         cd_fid1='nl'
         cd_fid2='nl'
       
         tmpTblRec={}
         tmpTblRec['BCTFOCC_ID']=bctfocc_id
         tmpTblRec['SO_ID']=so_id
         tmpTblRec['SODATECC']=sodatecc
         tmpTblRec['UNIQUE_CNTR']=unique_cntr
         hdr=process_check_exists("CAIMS_OCC_OCC_HDR", tmpTblRec, OCC_OCC_HDR_DEFN_DICT,con,schema,output_log)
         del tmpTblRec
         if hdr>0:
             writelog("bypassed in process_301550CKT",output_log)
             occ_hdr_id=hdr
             del hdr
         else:
             initialize_tbl('OCC_OCC_HDR_tbl')
             writelog("created in process_301550CKT",output_log)
             OCC_OCC_HDR_tbl['BCTFOCC_ID']=bctfocc_id
             OCC_OCC_HDR_tbl['SO_ID']=so_id
             OCC_OCC_HDR_tbl['SODATECC']=sodatecc
             OCC_OCC_HDR_tbl['UNIQUE_CNTR']=unique_cntr 
             occ_hdr_id=process_insert_table("CAIMS_OCC_OCC_HDR", OCC_OCC_HDR_tbl,OCC_OCC_HDR_DEFN_DICT,con,schema,"SQ_OCC_OCC_HDR",output_log)
          
         if occ_hdr_id > 0:
            tmpTblRec={}
            tmpTblRec['OCC_HDR_ID']=occ_hdr_id
            tmpTblRec['CD_FID1']=cd_fid1
            tmpTblRec['UNIQUE_CNTR']=fid_cntr 
            tmpid=process_check_exists("CAIMS_OCC_OCC_FID1", tmpTblRec, OCC_OCC_FID1_DEFN_DICT,con,schema,output_log)  
            del tmpTblRec
            if tmpid>0:
                occ_fid1_id = tmpid
            else:
                initialize_tbl('OCC_OCC_FID1_tbl') 
                OCC_OCC_FID1_tbl['OCC_HDR_ID']=occ_hdr_id                      
                OCC_OCC_FID1_tbl['CD_FID1']=cd_fid1 
                #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
                #not part of the original FOCUS logic
                OCC_OCC_FID1_tbl['UNIQUE_CNTR']=fid_cntr 
                OCC_OCC_FID1_tbl['FD1']=fd1
                OCC_OCC_FID1_tbl['INPUT_RECORDS']=str(record_id)
                occ_fid1_id=process_insert_table("CAIMS_OCC_OCC_FID1", OCC_OCC_FID1_tbl,OCC_OCC_FID1_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID1",output_log)
         else:
            writelog("ERROR:insert to occfid1 skipped since there was no occ_hdr record", output_log)

         if occ_fid1_id >0:  
            tmpTblRec={}
            tmpTblRec['OCC_FID1_ID']=occ_fid1_id
            tmpTblRec['CD_FID2']=cd_fid2
            tmpTblRec['UNIQUE_CNTR']=fid_cntr
            tmpid=process_check_exists("CAIMS_OCC_OCC_FID2", tmpTblRec, OCC_OCC_FID2_DEFN_DICT,con,schema,output_log)  
            del tmpTblRec
            if tmpid >0:
                occ_fid2_id=tmpid
            else:
                initialize_tbl('OCC_OCC_FID2_tbl')
                OCC_OCC_FID2_tbl['OCC_FID1_ID']=occ_fid1_id
                OCC_OCC_FID2_tbl['CD_FID2']=cd_fid2
                #Note UNIQUE_CNTR is used as part of the key for fid1 and fid2 tables
                #not part of the original FOCUS logic
                OCC_OCC_FID2_tbl['UNIQUE_CNTR']=fid_cntr 
                OCC_OCC_FID2_tbl['INPUT_RECORDS']=str(record_id)
                occ_fid2_id=process_insert_table("CAIMS_OCC_OCC_FID2", OCC_OCC_FID2_tbl,OCC_OCC_FID2_DEFN_DICT,con,schema,"SQ_OCC_OCC_FID2",output_log)
         else:
                writelog("ERROR: fid2 insert skipped due to no fid1 record", output_log) 
         
         tmpTblRec={}
         tmpTblRec['OCC_FID2_ID']=occ_fid2_id
         tmpTblRec['PHRASE_CD']=line[61:64]        
         inf=process_check_exists("CAIMS_OCC_OCC_INFO", tmpTblRec, OCC_OCC_INFO_DEFN_DICT,con,schema,output_log)     
         if inf>0:
             occ_info_id=inf
         else:
             writelog("ERROR: OCCINFO not found?", output_log) 
         del tmpTblRec, inf

         if occ_info_id > 0:
             initialize_tbl('OCC_PIDINF_tbl')
             OCC_PIDINF_tbl['OCC_INFO_ID']=occ_info_id
             OCC_PIDINF_tbl['D_PLAN_ID']=line[64:90]
             OCC_PIDINF_tbl['D_DIS_PERCNT']=convertnumber(line[90:95],2)
             OCC_PIDINF_tbl['D_DIS_PLAN_TOT_RVNUE']=convertnumber(line[95:106],2)
             OCC_PIDINF_tbl['D_VOL_PLN_TYP_IND']=line[106:107] 
             OCC_PIDINF_tbl['D_DIS_INTERVAL_IND']=line[107:108]
             OCC_PIDINF_tbl['D_DISC_APPL_IND']=line[108:109]
             OCC_PIDINF_tbl['D_PRICE_PLAN_TYP_IND']=line[109:110]
             OCC_PIDINF_tbl['SO_NUMBER']=line[110:122] 
             OCC_PIDINF_tbl['INPUT_RECORDS']=str(record_id)  
             
             tmpTblRec={}
             tmpTblRec['OCC_INFO_ID']=OCC_OCCD_INF_tbl['OCC_INFO_ID']
             tmpTblRec['D_PLAN_ID']=OCC_PIDINF_tbl['D_PLAN_ID']
             if process_check_exists("CAIMS_OCC_PIDINF", tmpTblRec, OCC_PIDINF_DEFN_DICT,con,schema,output_log)>0:
                 process_update_table("CAIMS_OCC_PIDINF", OCC_PIDINF_tbl, OCC_PIDINF_DEFN_DICT,con,schema,output_log)
             else:
                 occ_pidinf_id=process_insert_table("CAIMS_OCC_PIDINF", OCC_PIDINF_tbl,OCC_PIDINF_DEFN_DICT,con,schema,"SQ_OCC_PIDINF",output_log)
             del tmpTblRec
         else:
             writelog("no inserting occ_pidinf.  no parent occ_info record", output_log)
#    MATCH EOB_DATE ACNA BAN  ROOTREC
#    ON NOMATCH TYPE "CASE 301550CKT - NOMATCH DATE ACNA BAN - REJECT"
#      ON NOMATCH REJECT
#      ON MATCH COMPUTE
#                 SODATECC=SODATECC;
#                 SO_ID=SO_ID;
#  UNIQUE_CNTR=UNIQUE_CNTR;
#                 FD1=FD1;
#                 CD_FID1=CD_FID1;
#                 CD_FID2=' ';
#                 FID_DATA2=' ';
#      ON MATCH CONTINUE
#    MATCH SODATECC SO_ID UNIQUE_CNTR  OCC_HDR
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH FD1  OCC_CK
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH CD_FID1  OCC_FID1
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH CD_FID2  OCC_FID2
#      ON NOMATCH INCLUDE
#      ON MATCH CONTINUE
#    MATCH PHRASE_CD  OCC_INFO
#         ON NOMATCH INCLUDE
#         ON MATCH CONTINUE
#    MATCH D_PLAN_ID   PIDINF
#         ON NOMATCH INCLUDE
#         ON MATCH INCLUDE
    writelog(record_id+"--"+"leaving process_301550CKT",output_log)
      
def process_3020REC_34():    
    global output_log
    global bctfocc_id
    global occ_info_id
    global occd_inf_id
    "302000"
    "302001"  
    writelog(record_id+"--"+"XXXXXXXXXX:3020REC_34 - OCCD_INF",output_log)
#-*******************************************************
#-*THIS LOOKS AT THE 103020 RECORD FOR THE OCC AMOUNTS
#-*USOC RELATED INFORMATION. FOR BOS VER GE 34.
#-*******************************************************
#   FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13
#           [30:61]  [61:66] [66:68]  [68:75] [75:81]     [81:82]
#   FIXFORM X31      USOC/A5 STATE/A2 X7      QTY_USOC/A6 FAC_CHG_IND/A1
#           [82:83] [83:84]       [84:87]
#   FIXFORM X1      ACC_TYP_IND/1 BIP/3
#           [87:96]  [96:104]
#   FIXFORM X9       RAT_FACT/Z8.7
#           [104:115]        [115:126]        [126:137]
#   FIXFORM OCC_AMT_IR/Z11.2 OCC_AMT_IA/Z11.2 OCC_AMT_IRIA/Z11.2
#           [137:148]          [148:159]
#   FIXFORM OCC_AMT_IAIA/Z11.2 OCC_AMT_NONJ/Z11.2
#           [159:160] [160:161]    [161:162]    [162:163]      [163:166]
#   FIXFORM X1        USOC_PFIIR/1 USOC_PFIIA/1 USOC_PFIIAIA/1 USOC_PFBAND1/3
#           [166:177]        [177:180] [180:183]
#   FIXFORM OCC_AMT_LO/Z11.2 X3        RATE_BAND/A3
#           [183:215]       [215:216]    
#   FIXFORM X18 X1 X9 X3 X1 RATE_ZN_IR/1
#           [216:217]    [217:218]      [218:219]      [219:220]  
#   FIXFORM RATE_ZN_IA/1 RATE_ZN_IRIA/1 RATE_ZN_IAIA/1 RATE_ZN_LOC/1
#           [220:225]
#   FIXFORM X3 X1 X1
    if record_id == '302000':
        initialize_tbl('OCC_OCCD_INF_tbl') 
        OCC_OCCD_INF_tbl['OCC_INFO_ID']=occ_info_id
        OCC_OCCD_INF_tbl['USOC']=line[61:66]
        OCC_OCCD_INF_tbl['STATE']=line[66:68]
        OCC_OCCD_INF_tbl['QTY_USOC']=convertnumber(line[75:81],0) 
        OCC_OCCD_INF_tbl['FAC_CHG_IND']=line[81:82]
        OCC_OCCD_INF_tbl['ACC_TYP_IND']=line[83:84] 
        OCC_OCCD_INF_tbl['BIP']=line[84:87]
        OCC_OCCD_INF_tbl['RAT_FACT']=convertnumber(line[96:104],7)
        OCC_OCCD_INF_tbl['OCC_AMT_IR']=convertnumber(line[104:115],2)
        OCC_OCCD_INF_tbl['OCC_AMT_IA']=convertnumber(line[115:126],2)
        OCC_OCCD_INF_tbl['OCC_AMT_IRIA']=convertnumber(line[126:137],2)
        OCC_OCCD_INF_tbl['OCC_AMT_IAIA']=convertnumber(line[137:148],2)
        OCC_OCCD_INF_tbl['OCC_AMT_NONJ']=convertnumber(line[148:159],2)
        OCC_OCCD_INF_tbl['USOC_PFIIR']=line[160:161] 
        OCC_OCCD_INF_tbl['USOC_PFIIA']=line[161:162] 
        OCC_OCCD_INF_tbl['USOC_PFIIAIA']=line[162:163] 
        OCC_OCCD_INF_tbl['USOC_PFBAND1']=line[163:166]
        OCC_OCCD_INF_tbl['OCC_AMT_LO']=convertnumber(line[166:177],2)
        OCC_OCCD_INF_tbl['RATE_BAND']=line[180:183]
        OCC_OCCD_INF_tbl['RATE_ZN_IR']=line[215:216]
        OCC_OCCD_INF_tbl['RATE_ZN_IA']=line[216:217] 
        OCC_OCCD_INF_tbl['RATE_ZN_IRIA']=line[217:218] 
        OCC_OCCD_INF_tbl['RATE_ZN_IAIA']=line[218:219]
        OCC_OCCD_INF_tbl['RATE_ZN_LOC']=line[219:220]
        OCC_OCCD_INF_tbl['INPUT_RECORDS']=str(record_id)
    elif record_id== '302001':
        #                         [107:108]      [108:111]      [111:139] [139:145]
        #FIXFORM ON BCTFBDTI X107 USOC_PFIIRIA/1 USOC_PFBAND2/3 X28       UNCOMP_MILE/6    
        OCC_OCCD_INF_tbl['USOC_PFIIRIA']=line[107:108]
        OCC_OCCD_INF_tbl['USOC_PFBAND2']=line[108:111] 
        OCC_OCCD_INF_tbl['UNCOMP_MILE']=convertnumber(line[139:145],0)
        OCC_OCCD_INF_tbl['INPUT_RECORDS']+=','+str(record_id)
        if occ_info_id > 0:
            occd_inf_id=process_insert_table("CAIMS_OCC_OCCD_INF", OCC_OCCD_INF_tbl,OCC_OCCD_INF_DEFN_DICT,con,schema,"SQ_OCC_OCCD_INF",output_log)
            "NOTE-according to the log there appears to be an insert for every 302000/302001 set of records."
#            tmpTblRec={}
#            tmpTblRec['OCC_INFO_ID']=OCC_OCCD_INF_tbl['OCC_INFO_ID']
#            tmpTblRec['USOC']=OCC_OCCD_INF_tbl['USOC']
#            if process_check_exists("CAIMS_OCC_OCCD_INF", tmpTblRec, OCC_OCCD_INF_DEFN_DICT,con,schema,output_log)>0:
#                process_update_table("CAIMS_OCC_OCCD_INF", OCC_OCCD_INF_tbl, OCC_OCCD_INF_DEFN_DICT,con,schema,output_log)
#            else:
#                occd_inf_id=process_insert_table("CAIMS_OCC_OCCD_INF", OCC_OCCD_INF_tbl,OCC_OCCD_INF_DEFN_DICT,con,schema,"SQ_OCC_OCCD_INF",output_log)
#            del tmpTblRec
        else:
            writelog("ERROR:no insert to occd_inf.  no parent occ_info record", output_log) 
#TYPE "*** CASE 3020REC  USOC=<USOC>"
#    MATCH USOC    OCCD_INF
#          ON NOMATCH INCLUDE
#          ON MATCH INCLUDE 
    writelog(record_id+"--"+"leaving process_3020REC_34",output_log)

def initialize_tbl(tbl):
    global current_abbd_rec_key
      
    if  tbl == 'OCC_BCTFOCC_tbl':
        OCC_BCTFOCC_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        OCC_BCTFOCC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        OCC_BCTFOCC_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in OCC_BCTFOCC_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                OCC_BCTFOCC_tbl[key]=''        
    elif tbl == 'OCC_OCC_HDR_tbl':
        for key,value in OCC_OCC_HDR_tbl.items() :
            OCC_OCC_HDR_tbl[key]='' 
    elif tbl == 'OCC_OCC_FID1_tbl':
        for key,value in OCC_OCC_FID1_tbl.items() :
            OCC_OCC_FID1_tbl[key]=''
    elif tbl == 'OCC_OCC_FID2_tbl':
        for key,value in OCC_OCC_FID2_tbl.items() :
            OCC_OCC_FID2_tbl[key]=''            
    elif tbl == 'OCC_OCC_INFO_tbl':
        for key,value in OCC_OCC_INFO_tbl.items() :
            OCC_OCC_INFO_tbl[key]=''  
    elif tbl == 'OCC_OCCD_INF_tbl':
        for key,value in OCC_OCCD_INF_tbl.items() :
            OCC_OCCD_INF_tbl[key]=''             
    elif tbl == 'OCC_PIDINF_tbl':
        for key,value in OCC_PIDINF_tbl.items() :
            OCC_PIDINF_tbl[key]=''
    else:
        process_ERROR_END("ERROR: No initialization code for "+tbl+" in the initialize_tbl method.")

 
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
    global record_counts, unknown_record_counts, OCC_KEY_cnt
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
    
    writelog("TOTAL ACNA/EOB_DATE/BAN's processed:"+str(OCC_KEY_cnt),output_log)
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
    global occ_input
    global output_log
        
    occ_input.close();
    output_log.close()
    
def endProg(msg):
    global output_log
 
    con.commit()
    con.close()
    
    process_write_program_stats()
    endTM=datetime.datetime.now()
    
    writelog("\nStart time: %s" % (startTM),output_log)   
    writelog("  End time: %s" % (endTM),output_log)  
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
