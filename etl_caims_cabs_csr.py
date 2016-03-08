# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
PROGRAM NAME:     etl_caims_cabs_csr.py                                  
LOCATION:                      
PROGRAMMER(S):    Dan VandeRiet                                 
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


###IMPORT COMMON/SHARED UTILITIES
from etl_caims_cabs_utility import  process_insert_table, process_update_table, writelog, \
                                   createTableTypeDict,setDictFields,convertnumber  


settings = ConfigParser.ConfigParser()
settings.read('settings.ini')
settings.sections()

"SET ORACLE CONNECTION"
con=cx_Oracle.connect(settings.get('OracleSettings','OraCAIMSUser'),settings.get('OracleSettings','OraCAIMSPw'),settings.get('OracleSettings','OraCAIMSSvc'))

"CONSTANTS"
#Set Debug Trace Below - Set to trun to turn on
#DEBUGISON=True

if str(platform.system()) == 'Windows': 
    OUTPUT_DIR = settings.get('GlobalSettings','WINDOWS_LOG_DIR');
else:
    OUTPUT_DIR = settings.get('GlobalSettings','LINUX_LOG_DIR');    
 
#if DEBUGISON:
#    DEBUG_LOG=open(os.path.join(OUTPUT_DIR,settings.get('CSRSettings','CSR_DEBUG_FILE_NM')),"w")
#

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
baldue_rec=False
swsplchg_rec=False
baldtl_rec=False
dispdtl_rec=False
pmntadj_rec=False
adjmtdtl_rec=False
crnt1_051200_rec=False
crnt1_055000_rec=False
crnt2_rec=False    
CSR_CKT_exists=False 
 
"GLOBAL VARIABLES"
record_id='987654'
prev_record_id='456789'  
badKey=False
current_abbd_rec_key={'ACNA':'x','EOB_DATE':'1','BAN':'x'}
prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}   #EOB_DATE,ACNA,BAN key   
 
record_counts={}
unknown_record_counts={}
CSR_REC='40' 
STAR_LINE='*********************************************************************************************'  
   
"TRANSLATERS"

#def debug(msg):
#    if DEBUGISON:
#        DEBUG_LOG.write("\n"+str(msg))
       
    
def init():
#    debug("****** procedure==>  "+whereami()+" ******")
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
    
def main():
#    debug("****** procedure==>  "+whereami()+" ******")
    
    global record_type
    global line
    global Level
    global record_counts
    global unknown_record_counts
    global output_log
    "Counters"

    global inputLineCnt, CSR_KEY_cnt

    "TABLE Dictionaries - for each segment"
     
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
    
      
    global CSR_column_names
    global record_id
    global prev_record_id
    global current_abbd_rec_key 
    global prev_abbd_rec_key   
    global goodKey      
    
#DECLARE TABLE ARRAYS AND DICTIONARIES
    
#    CSR_BCCBSPL_tbl=collections.OrderedDict() 
    prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}
    

    CSR_BCCBSPL_DEFN_DICT=createTableTypeDict('CAIMS_CSR_BCCBSPL',con,output_log) 
    CSR_BCCBSPL_tbl=setDictFields('CAIMS_CSR_BCCBSPL', CSR_BCCBSPL_DEFN_DICT) 

    CSR_BILLREC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_BILLREC',con,output_log)
    CSR_BILLREC_tbl=setDictFields('CAIMS_CSR_BILLREC', CSR_BILLREC_DEFN_DICT)

    CSR_ACTLREC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_ACTLREC',con,output_log)
    CSR_ACTLREC_tbl=setDictFields('CAIMS_CSR_ACTLREC', CSR_ACTLREC_DEFN_DICT)

    CSR_CKT_DEFN_DICT=createTableTypeDict('CAIMS_CSR_CKT',con,output_log)    
    CSR_CKT_tbl=setDictFields('CAIMS_CSR_CKT', CSR_CKT_DEFN_DICT)

    CSR_LOC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_LOC',con,output_log)    
    CSR_LOC_tbl=setDictFields('CAIMS_CSR_LOC', CSR_LOC_DEFN_DICT)

    CSR_LFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_LFID',con,output_log) 
    CSR_LFID_tbl=setDictFields('CAIMS_CSR_LFID', CSR_LFID_DEFN_DICT)

    CSR_COSFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_COSFID',con,output_log) 
    CSR_COSFID_tbl=setDictFields('CAIMS_CSR_COSFID', CSR_COSFID_DEFN_DICT)

    CSR_UFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_UFID',con,output_log) 
    CSR_UFID_tbl=setDictFields('CAIMS_CSR_UFID', CSR_UFID_DEFN_DICT)

    CSR_USOC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_USOC',con,output_log) 
    CSR_USOC_tbl=setDictFields('CAIMS_CSR_USOC', CSR_USOC_DEFN_DICT)
        
    CSR_CUFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_CUFID',con,output_log) 
    CSR_CUFID_tbl=setDictFields('CAIMS_CSR_CUFID', CSR_CUFID_DEFN_DICT)

    CSR_CFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_CFID',con,output_log) 
    CSR_CFID_tbl=setDictFields('CAIMS_CSR_CFID', CSR_CFID_DEFN_DICT)
    
    CSR_CKTUSOC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_CKTUSOC',con,output_log) 
    CSR_CKTUSOC_tbl=setDictFields('CAIMS_CSR_CKTUSOC', CSR_CFID_DEFN_DICT)   
   
    
    
    
    "COUNTERS"
    inputLineCnt=0
    CSR_KEY_cnt=0
    status_cnt=0
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
#        print "line count is " +str(inputLineCnt  )
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
            else:
                if current_abbd_rec_key != prev_abbd_rec_key:
#                    debug("************************** NEW ACNA BAN EOBDATE**************************")
#                    debug(current_abbd_rec_key)
                    
                    CSR_KEY_cnt+=1
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
       
 
###############################################main end#########################################################
 


def log_footer_rec_info(): 
#    debug("****** procedure==>  "+whereami()+" ******")
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
#    debug("****** procedure==>  "+whereami()+" ******")
    global record_id
    global Level
    global tstx
    global verno
    global tsty
    global uact
          
# 01      S3
#**************            VERIFIED
#*EOB_DATE    **I
#*ACNA        **I
#*BAN         **I
#*BILL_DATE   **I
#*            **
#***************
# **************
#       I
#       +-----------------+-----------------+--------------------+
#       I                 I                 I                    I
#       I CUSTSEG         I PSMSEG-VERIFIED I BILLREC-VERIFIED   I ACTLREC
# 02    I KU        03    I S1        13    I S1           14    I S1
#..............    **************    **************       **************
#:BAN         :K   *PSM         **   *BILLNAME    **      *ACTL_NUM    **
#:CUST_GROUP  :    *            **   *BADDR1      **      *CUST_NAME   **
#:MKT_ID      :    *            **   *BADDR2      **      *ACTL        **
#:SPLRID      :    *            **   *BADDR3      **      *ACTLADDR1   **
#:            :    *            **   *            **      *            **
#:............:    ***************   ***************      ***************
# JOINED  BCCBCACB  **************    **************       **************
#                         I
#                         I
#                         I
#                         I CKTSEG
#                   04    I S1
#                  **************
#                  *CIRCUIT     **
#                  *FID         **
#                  *CDINSTCC    **
#                  *CACT        **
#                  *            **
#                  ***************
#                   **************
#                         I
#       +-----------------+-----------------+-----------------+
#       I                 I                 I                 I
#       I LOCSEG          I CFIDSEG         I COSFID          I CKTUSOC
# 05    I S1        09    I S0        10    I S0        11    I S1
#**************    **************    **************    **************
#*LOC         **   *CFID        **   *COSFID      **   *CUSOC       **
#*LSO         **I  *FID_DATA    **   *COSFID_DATA **   *CUSOCQTY    **
#*FSO         **I  *            **   *            **   *CUDINSTCC   **
#*CKLFID      **   *            **   *            **   *CUACT       **
#*            **   *            **   *            **   *            **
#***************   ***************   ***************   ***************
# **************    **************    **************    **************
#       I                                                     I
#       +-----------------+                                   I
#       I                 I                                   I
#       I USOCSEG         I LFIDSEG                           I CUFIDSEG
# 06    I S1        08    I S0                          12    I S0
#**************    **************                      **************
#*USOC        **   *LOCFID      **                     *CUFID       **
#*USOC_CNT    **   *LF_DATA     **                     *CUFID_DATA  **
#*QUSOC       **   *            **                     *            **
#*UCODE       **   *            **                     *            **
#*            **   *            **                     *            **
#***************   ***************                     ***************
# **************    **************                      **************
#       I
#       I
#       I
#       I UFIDSEG
# 07    I S0
#**************
#*UFID        **
#*UFID_DATA   **
#*            **
#*            **
#*            **
#***************
# **************
#  
#SECTION 01
#             STRUCTURE OF FOCUS    FILE BCCBFOS  ON 11/06/12 AT 11.55.17
#
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
#       I CUSTSEG         I PSMSEG
# 02    I KU        04    I S1
#..............    **************
#:BAN         :K   *PSM         **
#:CUST_GROUP  :    *            **
#:MKT_ID      :    *            **
#:SPLRID      :    *            **
#:            :    *            **
#:............:    ***************
# JOINEDI BCCBCACB  **************
#       I                 I
#       I                 I
#       I                 I
#       I CUSTSEG2        I CKTSEG
# 03    I KU        05    I S1
#..............    **************
#:CUST_GROUP  :K   *EC_CKT_ID   **
#:CUST_GRP_NAM:    *ST_ID       **
#:BUS_OFC_ID  :    *ST_LV_CC    **
#:            :    *HICAP_IND   **
#:            :    *            **
#:............:    ***************
# JOINED  BCTFCACG  **************


 
     
    dtst=line[224:225]  
    unknownRecord=False
    if record_id == '010100':                                  #ROOTREC VERIFIED
        process_TYP0101_HEADREC()                              #ROOTREC VERIFIED
    elif record_id == '050500':                                #ROOTREC VERIFIED
        process_ROOTREC_TYP0505()   #INSERT  CSR_BCCBSPL_tbl   #ROOTREC VERIFIED
    elif record_id in ('051000','051100'):                     #ROOTREC VERIFIED
        process_ROOTREC_CHEKFID() #UPDATE   CSR_BCCBSPL_tbl    #ROOTREC VERIFIED
    elif record_id == '100500':                                #BILLREC VERIFIED
        process_BILLREC_BILLTO()                               #BILLREC VERIFIED
    elif record_id == '101000':                                #0 records in current input file
        process_ACTLREC_BILLTO()                               #0 records in current input file
    elif record_id == '101500':       #udpate root record      #ROOTREC VERIFIED
        process_ROOTREC_UPROOT()    #UPDATE   CSR_BCCBSPL_tbl  #ROOTREC VERIFIED
    elif record_id == '150500' and dtst !='D':           #       check dtst 
        process_TYP1505_CKT_LOC()       
    elif record_id in ('150600','151001') and dtst !='D':#       check dtst 
        process_FIDDRVR()     
    elif record_id == '151000' and dtst !='D':           #       check dtst 
        process_USOCDRVR_36()
    elif record_id == '151600' and dtst !='D':           #       check dtst 
        process_USOCDRVR_TAX()    
    elif record_id == '151700' and dtst !='D':           #       check dtst 
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
#    debug("****** procedure==>  "+whereami()+" ******")
    global badKey
    
    if line[6:11].rstrip(' ') == '' or line[17:30].rstrip(' ') == '' or line[11:17].rstrip(' ') == '':
        badKey=True
    else:
        badKey=False
        
    return { 'ACNA':line[6:11],'EOB_DATE':line[11:17],'BAN':line[17:30]}
     
    
def reset_record_flags():
#    debug("****** procedure==>  "+whereami()+" ******")    
    "These flags are used to check if a record already exists for the current acna/ban/eob_date"
    "The first time a record is processed, if the count == 0, then an insert will be performed,"
    "else an update."
    global csr_tape_val   
    csr_tape_val=''    
    
    global root_rec
    global baldue_rec
    global swsplchg_rec
    global baldtl_rec
    global pmntadj_rec
    global adjmtdtl_rec
    global crnt1_051200_rec
    global crnt1_055000_rec
    global crnt2_rec
    global dispdtl_rec
    global Level
    global CSR_CKT_exists
    CSR_CKT_exists=False 
    Level=''    
    
    
    
    root_rec=False
    baldue_rec=False
    swsplchg_rec=False
    baldtl_rec=False
    dispdtl_rec=False
    pmntadj_rec=False
    crnt1_051200_rec=False
    crnt1_055000_rec=False
    crnt2_rec=False
   
    
def process_TYP0101_HEADREC():
#    debug("****** procedure==>  "+whereami()+" ******")
#    debug("Header Record:"+record_id)
    global verno
    global Level
    global output_log
     
#   
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
#    debug("****** procedure==>  "+whereami()+" ******")
    "050500"
#    debug("Root Record:"+record_id)
    global root_rec
    global Level
    global record_id
    global output_log
    global banlock
    global grplock
#    debug("Beginning of ROOT RECORD:" +record_id)
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
        grplock='N'
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
    else:
        grplock='Y'
        pass
    
    
     
    
    
        
    

    "we already know ACNA, EOB_DATE, and BAN are the same"   

#    process_insert_table("CAIMS_CSR_BCCBSPL", CSR_BCCBSPL_tbl, CSR_BCCBSPL_DEFN_DICT,con,output_log)
    root_rec=True
         
    "no flag to set - part of root"

def process_ROOTREC_CHEKFID():
#    debug("****** procedure==>  "+whereami()+" ******")
    "051000,051100"
    global record_id
    global output_log
#    debug("Root Record CheckFID:"+record_id)
#    FIXFORM X-225 X5 X6 X13 X31 X32 X32 X32 X32
#    FIXFORM BILLFID/4 BILLFID_DATA/28
    
    if line[189:193] == 'CCNA':
        #do UPCCNA
        CSR_BCCBSPL_tbl['CCNA']=line[193:196]
        CSR_BCCBSPL_tbl['INPUT_RECORDS']+=","+str(record_id)
    elif line[189:192] == 'MCN':
        #do this
        CSR_BCCBSPL_tbl['MCN']=line[193:221]
        CSR_BCCBSPL_tbl['INPUT_RECORDS']+=","+str(record_id)
    else:
        #continue on, skip
        pass

    if root_rec == True:
        if CSR_BCCBSPL_tbl['MCN'].rstrip(' ') == '' and CSR_BCCBSPL_tbl['CCNA'].rstrip(' ') == '':
            pass   #values are blank nothing to update
#        else:
#            process_update_table("CAIMS_CSR_BCCBSPL", CSR_BCCBSPL_tbl, CSR_BCCBSPL_DEFN_DICT,con,output_log)
    else:
        process_ERROR_END("ERROR: No root record for CHEKFID record "+str(record_id)+".  Not updating MCN or CCNA.")
        
   
def process_ROOTREC_UPROOT():
#    debug("****** procedure==>  "+whereami()+" ******")
    global record_id
    global output_log
    "ROOTREC  -  101500"    
#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 TAPE/A1 X128       
 
#    debug("Root Record UPROOT:"+record_id)
    
    if root_rec==True:
        if line[61:62].rstrip(' ') != '':
            CSR_BCCBSPL_tbl['TAPE']=line[61:62]
            CSR_BCCBSPL_tbl['INPUT_RECORDS']+=","+str(record_id)

        else:
            writelog("No TAPE value on input data for ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'],output_log)
                 ##ONCE WE HIT THIS RECORD WE CAN PURGE/WRITE OUT the ROOT RECORD:
        process_insert_table("CAIMS_CSR_BCCBSPL", CSR_BCCBSPL_tbl, CSR_BCCBSPL_DEFN_DICT,con,output_log)
    else:
        process_ERROR_END("ERROR: Encountered UPROOT record (record id "+record_id+") but no root record has been created.")

 

def process_BILLREC_BILLTO():   
#    debug("****** procedure==>  "+whereami()+" ******")
    global record_id
    global output_log
    "100500"
#    debug("BILLREC:"+record_id)
#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X35 BILLNAME/A32
#    FIXFORM BADDR1/A32 BADDR2/A32 BADDR3/A32 BADDR4/A32

#    -************************************************************
#    -*   PROCESS DATA FOR THE BILLREC SEGMENT OF DATABASE
#    -*    USE '401005' RECORDS FOR BOS VER 22.
#    -************************************************************
      
        
    initialize_tbl('CSR_BILLREC_tbl')    
#    initialize_BILLREC_tbl()
    CSR_BILLREC_tbl['BILLNAME']=line[65:97]
    CSR_BILLREC_tbl['BADDR1']=line[97:129]
    CSR_BILLREC_tbl['BADDR2']=line[129:161]
    CSR_BILLREC_tbl['BADDR3']=line[161:193]
    CSR_BILLREC_tbl['BADDR4']=line[193:225]
    CSR_BILLREC_tbl['INPUT_RECORDS']=str(record_id)
    
    process_insert_table("CAIMS_CSR_BILLREC", CSR_BILLREC_tbl, CSR_BILLREC_DEFN_DICT,con,output_log)

def process_ACTLREC_BILLTO(): 
#    debug("****** procedure==>  "+whereami()+" ******")
#    debug("CSR_ACTLREC_tbl:"+record_id)
    global record_id
    global output_log
    "ACTLREC  -  101000"    
    
    initialize_tbl('CSR_ACTLREC_tbl')
#    initialize_ACTLREC_tbl()
    CSR_ACTLREC_tbl['ACTL_NUM']=line[61:65]
    
    CSR_ACTLREC_tbl['ACTL']=line[65:76]
    CSR_ACTLREC_tbl['ACTLADDR1']=line[76:106]
    CSR_ACTLREC_tbl['ACTLADDR2']=line[106:136]
    CSR_ACTLREC_tbl['ACTLADDR3']=line[136:166]
    CSR_ACTLREC_tbl['CUST_NAME']=line[167:197]
    CSR_ACTLREC_tbl['FGRP']=line[217:218]
    CSR_ACTLREC_tbl['INPUT_RECORDS']=str(record_id)
    
    process_insert_table("CAIMS_CSR_ACTLREC", CSR_ACTLREC_tbl, CSR_ACTLREC_DEFN_DICT,con,output_log)
 

def process_TYP1505_CKT_LOC():
#    debug("****** procedure==>  "+whereami()+" ******")    
    global Level
    global usoccnt
    global record_id
    global output_log
    global CSR_CKT_exists
    "150500"
#    debug("process_TYP1505_CKT_LOC:"+record_id)
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
            #build CKTSEG segment
#            debug("initializing CKT")
            initialize_tbl('CSR_CKT_tbl')
#            initialize_CKT_tbl()
            ##FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
            #FIXFORM CDINSTCC/8 X3 FID/A4 X1 PSM/16 X-16 CIRCUIT/A53 X67
            #FIXFORM X1 CKT_LTP/A4 X1 CKT_MAN_BAN_UNITS/Z7.4 X6
            #FIXFORM CDACTCC/8 CACT/A1
            Level = 'C'
#            debug("populating CKT")
            CSR_CKT_tbl['CDINSTCC']=line[61:69]
            CSR_CKT_tbl['FID']=tfid
            CSR_CKT_tbl['PSM']=line[77:93]
            CSR_CKT_tbl['CIRCUIT']=line[77:130]
            CSR_CKT_tbl['CKT_LTP']=line[198:202]
            CSR_CKT_tbl['CKT_MAN_BAN_UNITS']=convertnumber(line[203:210],4)
            CSR_CKT_tbl['CDACTCC']=line[216:224]
            CSR_CKT_tbl['CACT']=line[224:225]
            CSR_CKT_tbl['INPUT_RECORDS']=str(record_id) 
            process_insert_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)

            CSR_CKT_exists=True
            
            #initialize all fields except the key
            CSR_CKT_tbl['CDINSTCC']=''
            CSR_CKT_tbl['FID']=''
            CSR_CKT_tbl['CKT_LTP']=''
            CSR_CKT_tbl['CKT_MAN_BAN_UNITS']=''
            CSR_CKT_tbl['CDACTCC']=''
            CSR_CKT_tbl['CACT']=''

            
        elif tfid in  ('CKL','CKLT'):
            #build LOCSEG setment
            initialize_tbl('CSR_LOC_tbl')
#            initialize_LOC_tbl()
            Level = 'L'
            #FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
            #FIXFORM LDINSTCC/8 X3 CKLFID/A4 X1 LOC_DATA/A60 X60
            #FIXFORM X1 LOC_LTP/A4 X1 LOC_MAN_BAN_UNITS/Z7.4 X6
            #FIXFORM LDACTCC/8 LACT/A1
            #COMPUTE
            #usoccnt/I2=0 
            #below:It would ignore the first two characters and get the 3rd and 4th.
            #TLOC = EDIT(LOC_DATA,'$$99$') 

            usoccnt=0
#            debug("CSR_LOC_tbl:"+record_id)
            CSR_LOC_tbl['LDINSTCC']=line[61:69]
            CSR_LOC_tbl['CKLFID']=line[72:76]
            CSR_LOC_tbl['LOC_DATA']=line[77:137]
            CSR_LOC_tbl['TLOC']=line[79:81]
            CSR_LOC_tbl['LOC_LTP']=line[198:202]
            CSR_LOC_tbl['LOC_MAN_BAN_UNITS']=line[203:210]
            CSR_LOC_tbl['LDACTCC']=line[216:224]
            CSR_LOC_tbl['LACT']=line[224:225]
            
            
            if CSR_LOC_tbl['TLOC'] == '1-':
                CSR_LOC_tbl['LOC'] ='1'
            elif CSR_LOC_tbl['TLOC'] == '2-':
                CSR_LOC_tbl['LOC'] ='2'
            elif CSR_LOC_tbl['TLOC'] == '3-':
                CSR_LOC_tbl['LOC'] ='3'
            elif CSR_LOC_tbl['TLOC'] == '4-':
                CSR_LOC_tbl['LOC'] ='4'
            elif CSR_LOC_tbl['TLOC'] == '5-':
                CSR_LOC_tbl['LOC'] ='5'
            elif CSR_LOC_tbl['TLOC'] == '6-':
                CSR_LOC_tbl['LOC'] ='6'
            elif CSR_LOC_tbl['TLOC'] == '7-':
                CSR_LOC_tbl['LOC'] ='7'
            elif CSR_LOC_tbl['TLOC'] == '8-':
                CSR_LOC_tbl['LOC'] ='8'
            elif CSR_LOC_tbl['TLOC'] == '9-':
                CSR_LOC_tbl['LOC'] ='9'
            else:
                CSR_LOC_tbl['LOC'] =CSR_LOC_tbl['TLOC'] 
                
            CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
            
            process_insert_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)            
        else:
            pass #SKIP record
            
       
def process_FIDDRVR():
#    debug("****** procedure==>  "+whereami()+" ******")    
    "150600, 151001 records"
#    -************************************************************
#    -* PROCESS '401506' AND '401511' RECORDS. THESE ARE FLOATING FIDS
#    -* ONLY.  LEFT-HANDED FIDS ARE PROCESSED IN THE TYP1505 CASE
#    -************************************************************
    global Level
    global tfid
    global record_id
    global output_log
    "150600, 151001"
    #FIXFORM X-159 TFID/4 X121 X6 X21 ACTVY_IND/1    
    
    tfid=line[72:76] 
    actvy_ind=line[224:225]
    
    #ACTVY_IND=line[224:225]
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
        #GOTO  CUSOCFID  
        #FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X42
        #FIXFORM CUFID/A4 X1 CUFID_DATA/36 X85
        #FIXFORM FGRP/1 X26
        initialize_tbl('CSR_CUFID_tbl')
#        initialize_CUFID_tbl()
#        debug("CSR_CUFID_tbl:"+record_id)
        CSR_CUFID_tbl['CUFID']=tfid
        CSR_CUFID_tbl['CUFID_DATA']=line[77:113]
        CSR_CUFID_tbl['FGRP']=line[198:199]
        CSR_CUFID_tbl['INPUT_RECORDS']=str(record_id)
        
        process_insert_table("CAIMS_CSR_CUFID", CSR_CUFID_tbl, CSR_CUFID_DEFN_DICT,con,output_log)
        
    elif Level == 'CO':
        process_COSFID()
    elif Level == 'L':
        process_LOCFIDRVR()
    elif Level == 'LU':
#        GOTO LUSOCFID 
#        -* THIS CASE READS THE  FLOATED FID AND LOADS THE CORRESPONDING
#        -* FIELD IN THE UFIDSEG SEGMENT OF THE DATABASE.
#        -************************************************************
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X42
#        FIXFORM UFID/A4 X1 UFID_DATA/A36 X85
#        FIXFORM FGRP/1 X26
               
        ufid=line[72:76]
        if ufid == 'ASG':
            pass #go to top
        else:
            initialize_tbl('CSR_UFID_tbl') 
#            initialize_UFID_tbl()
#            debug("CSR_UFID_tbl:"+record_id)
            CSR_UFID_tbl['UFID']=ufid
            CSR_UFID_tbl['UFID_DATA']=line[77:113]
            CSR_UFID_tbl['FGRP']=line[198:199]
            CSR_UFID_tbl['INPUT_RECORDS']=str(record_id)
            
            process_insert_table("CAIMS_CSR_UFID", CSR_UFID_tbl, CSR_UFID_DEFN_DICT,con,output_log)   
    else:
        pass #go to top

def process_LOCFIDRVR():
#    debug("****** procedure==>  "+whereami()+" ******")    
    global tfid, Level
    global record_id
    global output_log

#    initialize_LOC_tbl()    
    #No initialize... These should all be updates.
#    debug("CSR_LOC_tbl:"+record_id)
    if tfid == 'LSO ':                      
#         GOTO LOCLSOUP (UPDATE THE LSO FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LSO/7
        CSR_LOC_tbl['LSO']=line[77:84]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'FSO ':  
        
#         GOTO LOCFSOUP (UPDATE THE FSO FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 FSO/7
        CSR_LOC_tbl['FSO']=line[77:84]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'NCI ': 
#         GOTO LOCNCIUP (UPDATE THE NCI FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 NCI/13
        CSR_LOC_tbl['NCI']=line[77:90]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'NC  ': 
#         GOTO LOCLNCUP (UPDATE THE LNCODE FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LNCODE/4
        CSR_LOC_tbl['LNCODE']=line[77:81]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'ICO ': 
#        GOTO LOCICOUP (UPDATE THE ICO FIELD IN THE LOC SEGMENT )
#        LOC = LOC 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 ICO/4
        CSR_LOC_tbl['ICO']=line[77:81]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'SGN ': 
#         GOTO LOCSGNUP (UPDATE THE SGN FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SGN/3
        CSR_LOC_tbl['SGN']=line[77:80]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'TAR ': 
#         GOTO LOCTARUP  (UPDATE THE LTAR FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LTAR/4
        CSR_LOC_tbl['LTAR']=line[77:81]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'RTAR': 
#         GOTO LOCRTARUP (UPDATE THE LRTAR FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LRTAR/4
        CSR_LOC_tbl['LRTAR']=line[77:81] 
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'DES ': 
#         GOTO LOCLDESUP (UPDATE THE LDES  FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LDES/36
        CSR_LOC_tbl['LDES']=line[77:113] 
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'HSO ': 
#         GOTO LOCHSOUP  (UPDATE THE HSO FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 HSO/7
        CSR_LOC_tbl['HSO']=line[77:84]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'CFA ': 
#         GOTO LOCCFAUP  (UPDATE THE CFA FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 CFA/40
        CSR_LOC_tbl['CFA']=line[77:117]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'XR  ': 
#         GOTO LOCXRUP   (UPDATE THE XR  FIELD IN THE LOC SEGMENT )
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 XR/4
        CSR_LOC_tbl['XR']=line[77:81]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    elif tfid == 'SN  ': 
#         GOTO LOCSNUP (UPDATE THE SN  FIELD IN THE LOC SEGMENT ) 
#         LOC = LOC 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SN/30
        CSR_LOC_tbl['SN']=line[77:107]
        CSR_LOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT,con,output_log)
    else:  
        process_LOCFID()               
 

def process_LOCFID():
#    debug("****** procedure==>  "+whereami()+" ******")
    global record_id
    global output_log
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
    CSR_LFID_tbl['FGRP']=line[198:199]
    CSR_LFID_tbl['INPUT_RECORDS']=str(record_id)
    process_insert_table("CAIMS_CSR_LFID", CSR_LFID_tbl, CSR_LFID_DEFN_DICT,con,output_log)
 
def process_COSFID():   
#    debug("****** procedure==>  "+whereami()+" ******")
    global tfid, Level,record_id
    global output_log
    " "
#-************************************************************
#-* PROCESS '401511' RECORDS FROM CASE FIDDRVR WHEN Level EQUALS 'CO'.
#-* THIS CASE READS THE FLOATED FID AND LOADS THE COSFID & COSFID_DATA
#-* FIELD IN COSFID   SEGMENT OF THE DATABASE.
#-************************************************************
  
#    if CSR_COSFID_tbl['COSFID']==line[72:76] and CSR_COSFID_tbl['COSFID_DATA']==line[77:113]:
#        pass
#    else:
#    debug("CSR_COSFID_tbl:"+record_id)
    initialize_tbl('CSR_COSFID_tbl')    
#    initialize_COSFID_tbl() 
            #GOTO COSFID sEGMENT
#                FIXFORM X-225 X5 X6 X13 X42
#                FIXFORM COSFID/A4 X1 COSFID_DATA/36 X85
#                FIXFORM X1 X26
    CSR_COSFID_tbl['COSFID']=line[72:76]
    CSR_COSFID_tbl['COSFID_DATA']=line[77:113]
    CSR_COSFID_tbl['INPUT_RECORDS']=str(record_id)
    process_insert_table("CAIMS_CSR_COSFID", CSR_COSFID_tbl, CSR_COSFID_DEFN_DICT,con,output_log)

def process_CKTFIDRVR():
#    debug("****** procedure==>  "+whereami()+" ******")  
    global Level
    global tfid
    global  record_id
    global output_log
    
#    debug("CSR_CKT_tbl:"+record_id)
    if tfid == 'NC  ': 
#         GOTO CKTNCUP (LOADS THE NCODE FIELD IN THE CKTSEG OF THE DATABASE)
          #CIRCUIT = CIRCUIT ?????
         #FIXFORM X-225 X5 X6 X13 X42 X4 X1 NCODE/4
         #MATCH CIRCUIT? update ncode
         CSR_CKT_tbl['NCODE']=line[77:81]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'PIU ': 
#        GOTO CKTPIUUP (LOADS THE PIU FIELD IN THE CKTSEG OF THE DATABASE)
#        CIRCUIT = CIRCUIT 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 PIU/3
         #MATCH CIRCUIT? update piu
         CSR_CKT_tbl['PIU']=line[77:80]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'ASG ': 
#         GOTO CKTASGUP (LOADS THE ASG FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 ASG/6
          #MATCH CIRCUIT? update asg
         CSR_CKT_tbl['ASG']=line[77:83]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'LCC ': 
#         GOTO CKTLCCUP (LOADS THE LCC FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LCC/3
#         #MATCH CIRCUIT? update lcc
         CSR_CKT_tbl['LCC']=line[77:80]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'BAND': 
#         GOTO CKTBNDUP (LOADS THE BAND FIELD IN THECKTSEG OF THE DATABASE)
#        CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 BAND/1
         #MATCH CIRCUIT? update band
         CSR_CKT_tbl['BAND']=line[77:78]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'TAR ': 
#        GOTO CKTTARUP (LOADS THE TAR FIELD IN THE CKTSEG OF THE DATABASE)
#        CIRCUIT = CIRCUIT 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 TAR/4
         #MATCH CIRCUIT? update TAR
         CSR_CKT_tbl['TAR']=line[77:81]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'RTAR': 
#         GOTO CKTRTARUP (CRTAR  FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 RTAR/4
         #MATCH CIRCUIT? update RTAR
         CSR_CKT_tbl['RTAR']=line[77:81]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'LLN ': 
#         GOTO CKTLLNUP (LOADS THE LLN FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LLN/12
         #MATCH CIRCUIT? update LLN
         CSR_CKT_tbl['LLN']=line[77:89]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'TLI ': 
#         GOTO CKTTLIUP (LOADS THE TLI FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 TLI/12
         #MATCH CIRCUIT? update tli
         CSR_CKT_tbl['TLI']=line[77:89]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'PICX': 
#         GOTO CKTPICXUP (LOADS THE PICX FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 PICX/3
         #MATCH CIRCUIT? update picx
         CSR_CKT_tbl['PICX']=line[77:80]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'HML ': 
#         GOTO CKTHMLUP (LOADS THE HML FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 HML/40
          #MATCH CIRCUIT? update hml
         CSR_CKT_tbl['HML']=line[77:117]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'HTG ': 
#         GOTO CKTHTGUP (LOADS THE HTG FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 HTG/40
          #MATCH CIRCUIT? update htg
         CSR_CKT_tbl['HTG']=line[77:117]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'TN  ': 
#         GOTO CKTTNUP (LOADS THE TN FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 TN/12
          #MATCH CIRCUIT? update tn
         CSR_CKT_tbl['TN']=line[77:89]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'TER ': 
#         GOTO CKTTERUP (LOADS THE TER FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 TER/4
          #MATCH CIRCUIT? update ter
         CSR_CKT_tbl['TER']=line[77:81]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'STN ': 
#         GOTO CKTSTNUP (LOADS THE STN FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 STN/24
          #MATCH CIRCUIT? update stn
         CSR_CKT_tbl['STN']=line[77:91]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'SFN ': 
#         GOTO CKTSFNUP (LOADS THE SFN FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SFN/4
          #MATCH CIRCUIT? update sfn
         CSR_CKT_tbl['SFN']=line[77:81]  
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'SFG ': 
#         GOTO CKTSFGUP (LOADS THE SFG FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SFG/6
          #MATCH CIRCUIT? update sfg
         CSR_CKT_tbl['SFG']=line[77:83] 
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'CKR ': 
#         GOTO CKTCKRUP (LOADS THE CKR FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 CKR/36
         #MATCH CIRCUIT? update CKR
         CSR_CKT_tbl['CKR']=line[77:113]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'GSZ ': 
#         GOTO CKTGSZUP logic below
#        -************************************************************
#        -* PROCESS '401506' RECORDS FROM CASE CKTFID WHEN CFID CONTAINS 'GSZ'
#        -* THIS CASE READS THE FLOATED FID AND LOADS THE GSZ FIELD IN THE
#        -* CKTSEG OF THE DATABASE
#        -************************************************************
#        CIRCUIT = CIRCUIT 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 GSZ/3
        #7+5+6+13+42+4+1  =78 
         CSR_CKT_tbl['GSZ']=line[77:80]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'NHN ': 
#         GOTO CKTNHNUP logic below:
#        -************************************************************
#        -* PROCESS '401506' RECORDS FROM CASE CKTFID WHEN CFID CONTAINS 'NHN'
#        -* THIS CASE READS THE FLOATED FID AND LOADS THE NHN FIELD IN THE
#        -* CKTSEG OF THE DATABASE
#        -************************************************************
#        COMPUTE
#        CIRCUIT = CIRCUIT 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 NHN/12
         CSR_CKT_tbl['NHN']=line[77:89]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'PTN ': 
#         GOTO CKTPTNUP  logic below
#        -************************************************************
#        -* PROCESS '401506' RECORDS FROM CASE CKTFID WHEN CFID CONTAINS 'PTN'
#        -* THIS CASE READS THE FLOATED FID AND LOADS THE PTN FIELD IN THE
#        -* CKTSEG OF THE DATABASE
#        -************************************************************
#        COMPUTE
#        CIRCUIT = CIRCUIT 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 PTN/12
         CSR_CKT_tbl['PTN']=line[77:89] 
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    elif tfid == 'SBN ': 
#         GOTO CKTSBNUP  logic below 
#        -************************************************************
#        -* PROCESS '401506' RECORDS FROM CASE CKTFID WHEN CFID CONTAINS 'SBN'
#        -* THIS CASE READS THE FLOATED FID AND LOADS THE SBN FIELD IN THE
#        -* CKTSEG OF THE DATABASE
#        -************************************************************
#        COMPUTE
#        CIRCUIT = CIRCUIT 
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 SBN/40
         CSR_CKT_tbl['SBN']=line[77:117]
         CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
    else:
#         GOTO CKTFID logic below
#        -************************************************************
#        -* PROCESS '401506' RECORDS FROM CASE FIDDRVR WHEN Level EQUALS 'C'.
#        -* THIS CASE READS THE FLOATED FID AND LOADS THE CFID AND FID_DATA
#        -* FIELD IN CFIDREC SEGMENT OF THE DATABASE.
#        -************************************************************
#        COMPUTE
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X42
#        FIXFORM CFID/A4 X1 FID_DATA/36
#    SEGMENT=CFIDSEG
        if Level == 'C':
            initialize_tbl('CSR_CFID_tbl')
#            initialize_CFID_tbl()
#            debug("CSR_CFID_tbl:"+record_id)
            CSR_CFID_tbl['CFID']=line[72:76]
            CSR_CFID_tbl['FID_DATA']=line[77:113]
            CSR_CFID_tbl['INPUT_RECORDS']+=","+str(record_id)
            process_update_table("CAIMS_CSR_CFID", CSR_CFID_tbl, CSR_CFID_DEFN_DICT,con,output_log)
#                     INSERT data here                 
 

def process_USOCDRVR_36():
#    debug("****** procedure==>  "+whereami()+" ******")  
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
#    debug("****** procedure==>  "+whereami()+" ******")  
#    UPDATES THE CKTUSOC SEGMENT
    global Level
    global record_id
    global cuact
    global output_log
#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
#    FIXFORM CUDINSTCC/8 CUSOCQTY/Z6 CURES1/2 CUSOC/5 X121
#    FIXFORM CURES2/6 X7 CUDACTCC/8 CUACT/1

    
    if Level in ('L', 'LU'):
#        GOTO LOCUSOC_36 logic below
    #    -************************************************************
    #    -*  THIS CASE PROCESSES THE 1510 LOCATION Level USOCS FROM CASE CKTUSOC
    #    -*   IF Level EQUALS 'L' OR 'LU'
    #    -************************************************************
#    SEGMENT=USOCSEG  =LOCUSOC_36
        process_LOCUSOC_36()
    else:      #end LOCUSOC LOGIC
#    SEGMENT=CKTUSOC    
        initialize_tbl('CSR_CKTUSOC_tbl')
#        initialize_CKTUSOC_tbl()
        Level = 'CU'
    #    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
    #    FIXFORM CUDINSTCC/8 CUSOCQTY/Z5 CUSOC/5 X137
    #    FIXFORM CUDACTCC/8 CUACT/1
#        debug("CSR_CKTUSOC_tbl:"+record_id)
        CSR_CKTUSOC_tbl['CUDINSTCC']=line[61:69]
        CSR_CKTUSOC_tbl['CUSOCQTY']=convertnumber(line[69:74],0)
        CSR_CKTUSOC_tbl['CUSOC']=line[74:79]
        CSR_CKTUSOC_tbl['CUDACTCC']=line[216:224]
        cuact=line[224:225]
        CSR_CKTUSOC_tbl['CUACT']=cuact
        CSR_CKTUSOC_tbl['INPUT_RECORDS']=str(record_id) 
        
        if cuact == 'D':
            pass #goto top
        else:
            process_insert_table("CAIMS_CSR_CKTUSOC", CSR_CKTUSOC_tbl, CSR_CKTUSOC_DEFN_DICT,con,output_log)          
    
def process_COSUSOC_36():    
#    debug("****** procedure==>  "+whereami()+" ******")  
    global Level, record_id
    global output_log
    global CSR_CKT_exists
#    FIXFORM X-225 X5 X6 X13 X31
#    FIXFORM COSDINSTCC/8 COSUSOCQTY/Z6 COSRES1/2 COS_USOC/5 X121
#    FIXFORM COSRES2/6 X7 COSDACTCC/8 COSACT/1

#    -************************************************************
#    -*  THIS CASE PROCESSES CIRCUIT Level USOC RECORDS 401510. AND
#    -*  UPDATES THE CKTUSOC SEGMENT. SET Level TO 'CO'.
#    -*  CHECK TO SEE IF THIS IS A CIRCUIT Level RECORD OR LOCATION Level
#    -************************************************************
    if Level in ('L','LU'):
         process_LOCUSOC_36()  
#        -************************************************************
#        -*  THIS CASE PROCESSES THE 1510 LOCATION Level USOCS FROM CASE CKTUSOC
#        -*   IF Level EQUALS 'L' OR 'LU'
#        -************************************************************
 

    else:
        Level = 'CO'
        
        #check cosact
        if line[224:225] == 'D':
            pass
        else:
            
            CSR_CKT_tbl['COSDINSTCC']=line[61:69]
            CSR_CKT_tbl['COSUSOCQTY']=convertnumber(line[69:75],0)
            CSR_CKT_tbl['COSRES1']=line[75:77]
            CSR_CKT_tbl['COS_USOC']=line[77:82]
            CSR_CKT_tbl['COSRES2']=line[203:209]
            CSR_CKT_tbl['COSDACTCC']=line[216:224]
            CSR_CKT_tbl['COSACT']=line[224:225]
            CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
#        else:
#            initialize_CKT_tbl();
#            CSR_CKT_tbl['COSDINSTCC']=line[61:69]
#            CSR_CKT_tbl['COSUSOCQTY']=line[69:75]
#            CSR_CKT_tbl['COSRES1']=line[75:77]
#            CSR_CKT_tbl['COS_USOC']=line[77:82]
#            CSR_CKT_tbl['COSRES2']=line[203:209]
#            CSR_CKT_tbl['COSDACTCC']=line[216:224]
#            CSR_CKT_tbl['COSACT']=line[224:225]
#            CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
        

        #ON MATCH UPDATE 
        #ON NOMATCH REJECT

#        if CSR_CKT_tbl['COSACT'] == 'D':
#            pass #GOTO TOP
#        else:  
#            if CSR_CKT_exists:
#                process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
#                
#            else: 
#                writelog("ERROR: No CKT record exists for current update to CAIMS_CSR_CKT")
#                #doesnt exist
##                process_insert_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
#                 
#                
#            MATCH CIRCUIT
#              ON MATCH UPDATE COSDINSTCC COSUSOCQTY COSRES1
#              ON MATCH UPDATE COS_USOC COSRES2 COSDACTCC COSACT
# 
def process_LOCUSOC_36():     
#    debug("****** procedure==>  "+whereami()+" ******")
    global Level, usoccnt, uact
    global record_ID
    global output_log
    global usoccnt,ucode,usoc_cnt, Level
 #USOC
    initialize_tbl('CSR_USOC_tbl')
#    initialize_USOC_tbl()
    usoccnt = usoccnt + 1
    usoc_cnt = usoccnt
    ucode = '  '
    Level = 'LU'
 
#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
#    FIXFORM UDINSTCC/8 QUSOC/Z6 USOCRES1/2 USOC/5 X121
#    FIXFORM USOCRES2/6 X7 UDACTCC/8 UACT/A1
#    debug("CSR_USOC_tbl:"+record_id)

    uact=line[224:225]

    if uact == 'D':
        pass #GOTO TOP
    else:     
        CSR_USOC_tbl['UDINSTCC']=line[61:69]
        CSR_USOC_tbl['QUSOC']=convertnumber(line[69:75],0)
        CSR_USOC_tbl['USOCRES1']=line[75:76]
        CSR_USOC_tbl['USOC']=line[77:82]
        CSR_USOC_tbl['USOC_CNT']=usoc_cnt
        CSR_USOC_tbl['USOCRES2']=line[203:209]
        CSR_USOC_tbl['UDACTCC']=line[216:224]
        CSR_USOC_tbl['UACT']=uact
        CSR_USOC_tbl['INPUT_RECORDS']=str(record_id) 
#        process_insert_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT,con,output_log)
            
def process_USOCDRVR_TAX():
#    debug("****** procedure==>  "+whereami()+" ******")
    "151600"
    global x_tbl
    global Level,record_id
    global usoc_cnt, ucode, cuact, cusocqty, uact, Level, circuit,cosact
    global output_log
    
#        FIXFORM X-225 X5 X6 X13 X31 X11 TUSOC/5 X121 X9
#        FIXFORM X3 X2 X4 X1 X8
    tusoc=line[72:77]
    if Level in ('L','LU'):
#        GOTO LOCUSOC_TAX  logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES THE 1516 LOCATION Level USOCS FROM CASE CKTUSOC
#        -*   IF Level EQUALS 'L' OR 'LU'
#        -************************************************************
#SEGMENT=USOCSEG        
#        usoc_cnt = usoc_cnt
#        QUSOC = QUSOC
        ucode = '  '
        Level = 'LU'
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM USOC/5 X121 USO_FEDTAX/1 USO_STTAX/1 USO_CITYTAX/1
#        FIXFORM USO_CNTYTAX/1 USO_STSLSTAX/1 USO_LSLSTAX/1 USO_SURTAX/1
#        FIXFORM USO_FRANTAX/1 USO_OTHERTAX/1 X3 X2 X4 X1 X8
#        debug("CSR_USOC_tbl:"+record_id)
        CSR_USOC_tbl['USOC']=line[72:77]
        CSR_USOC_tbl['USO_FEDTAX']=line[198:199]
        CSR_USOC_tbl['USO_STTAX']=line[199:200]
        CSR_USOC_tbl['USO_CITYTAX']=line[200:201]
        CSR_USOC_tbl['USO_CNTYTAX']=line[201:202]
        CSR_USOC_tbl['USO_STSLSTAX']=line[202:203]
        CSR_USOC_tbl['USO_LSLSTAX']=line[203:204]
        CSR_USOC_tbl['USO_SURTAX']=line[204:205]
        CSR_USOC_tbl['USO_FRANTAX']=line[205:206]
        CSR_USOC_tbl['USO_OTHERTAX']=line[206:207]
        CSR_USOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        
        if uact == 'D':
            pass #GOTO TOP
#        else:
#            process_update_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT,con,output_log)
#            update record

   
    elif tusoc.rstrip(' ') in ('UDP','U3P','U6P'):
#        GOTO CKTUSOC_TAX  logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES CIRCUIT Level USOC RECORDS 401516. AND
#        -*  UPDATES THE CKTUSOC SEGMENT. SET Level TO 'CU'.
#        -*  CHECK TO SEE IF THIS IS A CIRCUIT Level RECORD OR LOCATION Level
#        -************************************************************
        Level = 'CU'
        cuact = cuact
        cusocqty = cusocqty
#        SEGMENT=CKTUSOC
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM CUSOC/5 X121 CU_FEDTAX/1 CU_STTAX/1 CU_CITYTAX/1
#        FIXFORM CU_CNTYTAX/1 CU_STSLSTAX/1 CU_LSLSTAX/1 CU_SURTAX/1
#        FIXFORM CU_FRANTAX/1 CU_OTHERTAX/1 X3 X2 X4 X1 X8
#        debug("CSR_CKTUSOC_tbl:"+record_id)
        CSR_CKTUSOC_tbl['CUSOC']=line[72:77]
        CSR_CKTUSOC_tbl['CU_FEDTAX']=line[198:199]
        CSR_CKTUSOC_tbl['CU_STTAX']=line[199:200]
        CSR_CKTUSOC_tbl['CU_CITYTAX']=line[200:201]
        CSR_CKTUSOC_tbl['CU_CNTYTAX']=line[201:202]
        CSR_CKTUSOC_tbl['CU_STSLSTAX']=line[202:203]
        CSR_CKTUSOC_tbl['CU_LSLSTAX']=line[203:204]
        CSR_CKTUSOC_tbl['CU_SURTAX']=line[204:205]
        CSR_CKTUSOC_tbl['CU_FRANTAX']=line[205:206]
        CSR_CKTUSOC_tbl['CU_OTHERTAX']=line[206:207]
        CSR_CKTUSOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        
        if cuact == 'D':
            pass #GOTO TOP
        else:
            process_update_table("CAIMS_CSR_CKTUSOC", CSR_CKTUSOC_tbl, CSR_CKTUSOC_DEFN_DICT,con,output_log)
#            update record
    
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
        
#        debug("CSR_CKT_tbl:"+record_id)
        CSR_CKT_tbl['COS_USOC']=line[72:77]
        CSR_CKT_tbl['COS_FEDTAX']=line[198:199]
        CSR_CKT_tbl['COS_STTAX']=line[199:200]
        CSR_CKT_tbl['COS_CITYTAX']=line[200:201]
        CSR_CKT_tbl['COS_CNTYTAX']=line[201:202]
        CSR_CKT_tbl['COS_STSLSTAX']=line[202:203]
        CSR_CKT_tbl['COS_LSLSTAX']=line[203:204]
        CSR_CKT_tbl['COS_SURTAX']=line[204:205]
        CSR_CKT_tbl['COS_FRANTAX']=line[205:206]
        CSR_CKT_tbl['COS_OTHERTAX']=line[206:207]        
        CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
        
        if cosact == 'D':
            pass #GOTO TOP
        else:
            process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)        
#            update record


def process_USOCDRVR_PLN():    
#    debug("****** procedure==>  "+whereami()+" ******")
    "151700"
    global x_tbl
    global output_log
    global current_abbd_rec_key   
    global Level, record_id
    global usoccnt,usoc_cnt,ucode
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
#        debug("CSR_USOC_tbl:"+record_id)
        CSR_USOC_tbl['USOC']=line[72:77]
        CSR_USOC_tbl['PLAN_ID']=line[77:103]
        CSR_USOC_tbl['RQRD_PL_QTY']=convertnumber(line[103:108],0)
        CSR_USOC_tbl['VAR_PL_QTY']=convertnumber(line[108:113],0)
        CSR_USOC_tbl['TRM_STRT_DAT']=line[113:121]
        CSR_USOC_tbl['TRM_END_DAT']=line[121:129]
        CSR_USOC_tbl['LNGTH_OF_TRM']=convertnumber(line[129:132],0)
        CSR_USOC_tbl['PL_TYP_INDR']=convertnumber(line[132:133],0)
        CSR_USOC_tbl['DSCNT_PCTGE']=convertnumber(line[133:138],2)
        CSR_USOC_tbl['SYS_CPCTY']=convertnumber(line[138:140],0)
        CSR_USOC_tbl['ADD_DIS_PL_IND']=line[140:141]
        CSR_USOC_tbl['SPL_OFR_PCTGE']=convertnumber(line[141:146],2)
        CSR_USOC_tbl['SPL_OFR_IDENT']=line[146:172]
        CSR_USOC_tbl['S_OFR_END_DAT']=line[172:180]
        CSR_USOC_tbl['PL_TAR_TYP_INDR']=line[180:181]
        CSR_USOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        
        if uact == 'D':
            pass #GOTO TOP
#        else:
#            process_update_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT,con,output_log)
#            update record

    elif TUSOC.rstrip(' ') in ('UDP', 'U3P','U6P'):
#        GOTO CKTUSOC_PLN  logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES CIRCUIT Level USOC RECORDS 401517. AND
#        -*  UPDATES THE CKTUSOC SEGMENT. SET Level TO 'CU'.
#        -*  CHECK TO SEE IF THIS IS A CIRCUIT Level RECORD OR LOCATION Level
#        -************************************************************
#SEGMENT=CKTUSOC
        Level = 'CU' 
        cuact = cuact 
        cusocqty = cusocqty 
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM CUSOC/5 CU_PLAN_ID/26 CU_RQRD_PL_QTY/5 CU_VAR_PL_QTY/5
#        FIXFORM CU_TRM_STRT_DAT/8 CU_TRM_END_DAT/8 CU_LNGTH_OF_TRM/3
#        FIXFORM CU_PL_TYP_INDR/1 CU_DSCNT_PCTGE/Z5.2 CU_SYS_CPCTY/2
#        FIXFORM CU_ADD_DIS_PL_IND/1 CU_SPL_OFR_PCTGE/Z5.2 CU_SPL_OFR_IDENT/26
#        FIXFORM CU_S_OFR_END_DAT/8 CU_PL_TAR_TYP_INDR/1 X44
#        debug("CSR_CKTUSOC_tbl:"+record_id)
        CSR_CKTUSOC_tbl['CUSOC']=line[72:77]
        CSR_CKTUSOC_tbl['CU_PLAN_ID']=line[77:103]
        CSR_CKTUSOC_tbl['CU_RQRD_PL_QTY']=convertnumber(line[103:108],0)
        CSR_CKTUSOC_tbl['CU_VAR_PL_QTY']=convertnumber(line[108:113],0)
        CSR_CKTUSOC_tbl['CU_TRM_STRT_DAT']=line[113:121]
        CSR_CKTUSOC_tbl['CU_TRM_END_DAT']=line[121:129]
        CSR_CKTUSOC_tbl['CU_LNGTH_OF_TRM']=convertnumber(line[129:132],0)
        CSR_CKTUSOC_tbl['CU_PL_TYP_INDR']=line[132:133]
        CSR_CKTUSOC_tbl['CU_DSCNT_PCTGE']=convertnumber(line[133:138],2)
        CSR_CKTUSOC_tbl['CU_SYS_CPCTY']=convertnumber(line[138:140],0)
        CSR_CKTUSOC_tbl['CU_ADD_DIS_PL_IND']=line[140:141]
        CSR_CKTUSOC_tbl['CU_SPL_OFR_PCTGE']=convertnumber(line[141:146],2)
        CSR_CKTUSOC_tbl['CU_SPL_OFR_IDENT']=line[146:172]
        CSR_CKTUSOC_tbl['CU_S_OFR_END_DAT']=line[172:180]
        CSR_CKTUSOC_tbl['CU_PL_TAR_TYP_INDR']=line[180:181]
        CSR_CKTUSOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        
        if cuact == 'D':
            pass  #GOTO TOP
        else:
            process_update_table("CAIMS_CSR_CKTUSOC", CSR_CKTUSOC_tbl, CSR_CKTUSOC_DEFN_DICT,con,output_log)
#            update record
 
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
        circuit = circuit 
        cosact = cosact 
#        FIXFORM X-225 X5 X6 X13 X31 X11
#        FIXFORM COS_USOC/5 CO_PLAN_ID/26 CO_RQRD_PL_QTY/5 CO_VAR_PL_QTY/5
#        FIXFORM CO_TRM_STRT_DAT/8 CO_TRM_END_DAT/8 CO_LNGTH_OF_TRM/3
#        FIXFORM CO_PL_TYP_INDR/1 CO_DSCNT_PCTGE/Z5.2 CO_SYS_CPCTY/2
#        FIXFORM CO_ADD_DIS_PL_IND/1 CO_SPL_OFR_PCTGE/Z5.2 CO_SPL_OFR_IDENT/26
#        FIXFORM CO_S_OFR_END_DAT/8 CO_PL_TAR_TYP_INDR/1 X44
        
#        debug("CSR_CKT_tbl:"+record_id)
        CSR_CKT_tbl['COS_USOC']=line[72:77]
        CSR_CKT_tbl['CO_PLAN_ID']=line[77:103]
        CSR_CKT_tbl['CO_RQRD_PL_QTY']=convertnumber(line[103:108],0)
        CSR_CKT_tbl['CO_VAR_PL_QTY']=convertnumber(line[108:113],0)
        CSR_CKT_tbl['CO_TRM_STRT_DAT']=line[113:121]
        CSR_CKT_tbl['CO_TRM_END_DAT']=line[121:129]
        CSR_CKT_tbl['CO_LNGTH_OF_TRM']=convertnumber(line[129:132],0)
        CSR_CKT_tbl['CO_PL_TYP_INDR']=line[132:133]
        CSR_CKT_tbl['CO_DSCNT_PCTGE']=convertnumber(line[133:138],2)
        CSR_CKT_tbl['CO_SYS_CPCTY']=convertnumber(line[138:140],0)
        CSR_CKT_tbl['CO_ADD_DIS_PL_IND']=line[140:141]
        CSR_CKT_tbl['CO_SPL_OFR_PCTGE']=convertnumber(line[141:146],2)
        CSR_CKT_tbl['CO_SPL_OFR_IDENT']=line[146:172]
        CSR_CKT_tbl['CO_S_OFR_END_DAT']=line[172:180]
        CSR_CKT_tbl['CO_PL_TAR_TYP_INDR']=line[180:181]
        CSR_CKT_tbl['INPUT_RECORDS']+=","+str(record_id)
            
            
        if cosact == 'D':
            pass  #GOTO TOP
        else:
            process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT,con,output_log)
#            update data
 
def  process_TYP1520():
    #    debug("****** procedure==>  "+whereami()+" ******")
    "152000"
    global x_tbl
    global output_log
    global current_abbd_rec_key   
    global Level, record_id
    global usoccnt,usoc_cnt,ucode
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
         CSR_USOC_tbl['UCODE']='  '  #spaces   
         CSR_USOC_tbl['IRPCT']=convertnumber(line[67:73] ,3)
         CSR_USOC_tbl['IRRATE']=convertnumber(line[73:84],4)
         CSR_USOC_tbl['IRMRC']=convertnumber(line[84:95],2)
         CSR_USOC_tbl['IAPCT']=convertnumber(line[95:101],3)
         CSR_USOC_tbl['IARATE']=convertnumber(line[101:112],4)
         CSR_USOC_tbl['IAMRC']=convertnumber(line[112:123],2) 
         CSR_USOC_tbl['MBIPFIX']=line[123:126]         
         CSR_USOC_tbl['UFAC_CHG']=line[126:127]         
         CSR_USOC_tbl['RATE_ZN_IR']=line[127:128]             
         CSR_USOC_tbl['USOC_RATE_SZN_IND_INT']=line[128:129]              
         CSR_USOC_tbl['RAT_FCTR']=convertnumber(line[145:153],7)              
         CSR_USOC_tbl['USOC_PFBAND2']=line[153:156]
         CSR_USOC_tbl['USOC_PFIIRIA']=line[156:157]
         CSR_USOC_tbl['USOC_PFIIR']=line[157:158]
         CSR_USOC_tbl['USOC_PFIIA']=line[158:159]
         CSR_USOC_tbl['USOC_PFIIAIA']=line[159:160] 
         CSR_USOC_tbl['USOC_PFBAND1']=line[160:163]     
         CSR_USOC_tbl['RATE_ZN_IA']=line[163:164]                  
         CSR_USOC_tbl['LORATE']=convertnumber(line[180:191],4)   
         CSR_USOC_tbl['LOMRC']=convertnumber(line[191:202],2)
         CSR_USOC_tbl['USO_O_LOC_PCT']=convertnumber(line[202:205],0)
         CSR_USOC_tbl['ACC_TYPE']=line[214:215]  
         CSR_USOC_tbl['LOPCT']=convertnumber(line[215:221],3)
         CSR_USOC_tbl['RATE_ZN_LOC']=line[221:222]   
         CSR_USOC_tbl['MBIPVAR']=line[222:225] 
         CSR_USOC_tbl['INPUT_RECORDS']+=","+str(record_id)
         process_insert_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT,con,output_log)
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
        CSR_USOC_tbl['IRIA_PCT']=convertnumber(line[61:67],3) 
        CSR_USOC_tbl['IRIARATE']=convertnumber(line[67:78],4) 
        CSR_USOC_tbl['IRIAMRC']=convertnumber(line[78:89],2)
        CSR_USOC_tbl['IAIA_PCT']=convertnumber(line[89:95],3) 
        CSR_USOC_tbl['IAIARATE']=convertnumber(line[95:106],4)
        CSR_USOC_tbl['IAIAMRC']=convertnumber(line[106:117],2)
        CSR_USOC_tbl['DSCT_FAC_LOC']=convertnumber(line[145:150],4)
        CSR_USOC_tbl['RATE_BAND']=line[153:156]
        #        Many of the below do not match
        #        Many of the below do not match
        aloc_uamt=line[156:167]      
        if aloc_uamt.rstrip(' ') =='':
            CSR_USOC_tbl['LOC_ADL_UAMT']='0.00'
        else:
            x=convertnumber(aloc_uamt,2)
            if str(x) =='0.00':
                CSR_USOC_tbl['LOC_ADL_UAMT']='0.00'
            else:
                y=float(x)*.01
                CSR_USOC_tbl['LOC_ADL_UAMT']=convertnumber(y,2)
 
            
        CSR_USOC_tbl['PCT_ORIG_USG']=convertnumber(line[167:170],0) 
        CSR_USOC_tbl['SPDF_INTERST']=convertnumber(line[171:176],4)
        CSR_USOC_tbl['SPDF_INTRAST']=convertnumber(line[176:182],4) 
        CSR_USOC_tbl['DISC_MONEY_INTERST']=convertnumber(line[182:186],2)
        CSR_USOC_tbl['DISC_MONEY_INTRAST']=convertnumber(line[186:192],2) 
        CSR_USOC_tbl['DISC_MONEY_LOCAL']=convertnumber(line[192:198],2)
        CSR_USOC_tbl['RATE_ZN_IRIA']=line[221:222]      
        CSR_USOC_tbl['RATE_ZN_IAIA']=line[222:223]   
        CSR_USOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_insert_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT,con,output_log)
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
            
        CSR_USOC_tbl['QUSOC']=convertnumber(line[61:67],0)
        CSR_USOC_tbl['IRPCT']=convertnumber(line[67:73],3)
        CSR_USOC_tbl['IRRATE']=convertnumber(line[73:84],4)
        CSR_USOC_tbl['IR_U_ML_RT']=convertnumber(line[84:95],4)
        CSR_USOC_tbl['IRMRC']=convertnumber(line[95:106],2)
        CSR_USOC_tbl['IAPCT']=convertnumber(line[106:112],3)
        CSR_USOC_tbl['IARATE']=convertnumber(line[112:123],4)
        CSR_USOC_tbl['IA_U_ML_RT']=convertnumber(line[123:134],4)
        CSR_USOC_tbl['IAMRC']=convertnumber(line[134:145],2)
        CSR_USOC_tbl['MBIPFIX']=line[145:148]
        CSR_USOC_tbl['MBIPVAR']=line[148:151]
        CSR_USOC_tbl['UCODE']='40'
        CSR_USOC_tbl['UFAC_CHG']=line[151:152] 
        CSR_USOC_tbl['RATE_ZN_IR']=line[152:153]
        CSR_USOC_tbl['ACC_TYPE']=line[153:154]
        CSR_USOC_tbl['RATE_ZN_IA']=line[154:155]
        CSR_USOC_tbl['RAT_FCTR']=convertnumber(line[173:181],7)
        CSR_USOC_tbl['USOC_PFBAND1']=line[203:206]                    
        CSR_USOC_tbl['USOC_PFIIR']=line[206:207]
        CSR_USOC_tbl['USOC_PFIIA']=line[207:208]
        CSR_USOC_tbl['USOC_PFIIAIA']=line[208:209]
        CSR_USOC_tbl['USOC_PFIIRIA']=line[209:210]
        CSR_USOC_tbl['USOC_PFBAND2']=line[210:213]
        a_pcfpct=line[216:219]  #used in 1521001 record
        CSR_USOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_insert_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT,con,output_log)
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
 
        CSR_USOC_tbl['IRIA_PCT']=convertnumber(line[61:67],3)
        CSR_USOC_tbl['IRIARATE']=convertnumber(line[67:78],4)
        CSR_USOC_tbl['IRIA_U_ML_RT']=convertnumber(line[78:89],4)
        CSR_USOC_tbl['IRIAMRC']=convertnumber(line[89:100],2)
        CSR_USOC_tbl['IAIA_PCT']=convertnumber(line[100:106],3)
        CSR_USOC_tbl['IAIARATE']=convertnumber(line[106:117],4)
        CSR_USOC_tbl['IAIA_U_ML_RT']=convertnumber(line[117:128],4)
        CSR_USOC_tbl['IAIAMRC']=convertnumber(line[128:139],2)
        CSR_USOC_tbl['LOPCT']=convertnumber(line[139:145],3)
        CSR_USOC_tbl['LORATE']=convertnumber(line[145:156],4)
        CSR_USOC_tbl['LO_U_ML_RT']=convertnumber(line[156:167],4)
        CSR_USOC_tbl['LOMRC']=convertnumber(line[167:178],2)
        CSR_USOC_tbl['RATE_ZN_IRIA']=line[178:179] 
        CSR_USOC_tbl['RATE_ZN_IAIA']=line[179:180] 
        CSR_USOC_tbl['RATE_ZN_LOC']=line[180:181]
        CSR_USOC_tbl['DSCT_FAC_LOC']=convertnumber(line[181:186],4)
#        CSR_USOC_tbl['RATE_BAND']=line[x:x] ???????????????????????????????
        CSR_USOC_tbl['SPDF_INTERST']=convertnumber(line[207:212],4)
        CSR_USOC_tbl['SPDF_INTRAST']=convertnumber(line[212:217],4)
        
        CSR_USOC_tbl['AFLEX_PC_PCT']=a_pcfpct  #comes from 1521000 record
        CSR_USOC_tbl['INPUT_RECORDS']+=","+str(record_id)
        process_insert_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT,con,output_log)  
 
 
  
 

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
        CSR_ACTLREC_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_ACTLREC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_ACTLREC_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_ACTLREC_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_ACTLREC_tbl[key]='' 
    elif tbl == 'CSR_CKT_tbl':
        CSR_CKT_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_CKT_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_CKT_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_CKT_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_CKT_tbl[key]=''   
    elif tbl == 'CSR_LOC_tbl':
        CSR_LOC_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_LOC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_LOC_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_LOC_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_LOC_tbl[key]='' 
    elif tbl == 'CSR_LFID_tbl':
        CSR_LFID_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_LFID_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_LFID_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_LFID_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_LFID_tbl[key]='' 
    elif tbl == 'CSR_COSFID_tbl':
        CSR_COSFID_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_COSFID_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_COSFID_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_COSFID_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_COSFID_tbl[key]='' 
    elif tbl == 'CSR_UFID_tbl':
        CSR_UFID_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_UFID_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_UFID_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_UFID_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_UFID_tbl[key]=''
    elif tbl == 'CSR_USOC_tbl':
        CSR_USOC_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_USOC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_USOC_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_USOC_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_USOC_tbl[key]=''
    elif tbl == 'CSR_CUFID_tbl':
        CSR_CUFID_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_CUFID_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_CUFID_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_CUFID_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_CUFID_tbl[key]=''
    elif tbl == 'CSR_CFID_tbl':
        CSR_CFID_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_CFID_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_CFID_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_CFID_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_CFID_tbl[key]=''
    elif tbl == 'CSR_BILLREC_tbl':
        CSR_BILLREC_tbl['ACNA']=current_abbd_rec_key['ACNA']    
        CSR_BILLREC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
        CSR_BILLREC_tbl['BAN']=current_abbd_rec_key['BAN'] 
        for key,value in CSR_BILLREC_tbl.items() :
            if key in ('ACNA', 'BAN', 'EOB_DATE'):
                pass
            else:
                CSR_BILLREC_tbl[key]=''                
    else:
        process_ERROR_END("ERROR: No initialization code for "+tbl+" in the initialize_tbl method.")

    
   
####END INITIALIZE PROCEDURES
    
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
#    debug("****** procedure==>  "+whereami()+" ******")
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
#    debug("****** procedure==>  "+whereami()+" ******")    
    global output_log
    writelog("ERROR:"+msg,output_log)
#    debug("ERROR:"+msg)
    con.commit()
    con.close()
    process_close_files()
#    raise Exception("ERROR:"+msg)
    
    
def process_close_files():
#    debug("****** procedure==>  "+whereami()+" ******")
    global csr_input,  output_log
    global output_log
    
#    if DEBUGISON:
#        DEBUG_LOG.close()
        
    csr_input.close() 
    output_log.close()
   
    
    
def endProg(msg):
#    debug("****** procedure==>  "+whereami()+" ******")
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
#try:
main()
#except Exception as e:
#    process_ERROR_END(e.message)
#else:
endProg("-END OF PROGRAM-")
