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
 
    
"CONSTANTS"
#Set Debug Trace Below - Set to trun to turn on
#DEBUGISON=False

if str(platform.system()) == 'Windows': 
    OUTPUT_DIR = settings.get('GlobalSettings','WINDOWS_LOG_DIR');
else:
    OUTPUT_DIR = settings.get('GlobalSettings','LINUX_LOG_DIR');
    
#set to true to get debug statements
#if DEBUGISON:
#    DEBUG_LOG=open(os.path.join(OUTPUT_DIR,settings.get('BDTSettings','BDT_DEBUG_FILE_NM')),"w")



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

root_rec=False
baldue_rec=False
swsplchg_rec=False
baldtl_rec=False
dispdtl_rec=False
pmntadj_rec=False
adjmtdtl_rec=False
crnt1_051200_rec=False
crnt1_055000_rec=False
 
 
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
    #debug("****** procedure==>  "+whereami()+" ******")
    #BDT_config.initialize_BDT() 
    global record_type
    global line,output_log
    global record_counts, unknown_record_counts

    "Counters"


#    record_counts={'010100':0,'050500':0,'051000':0,'051200':0,'055000':0,'051300':0,'055100':0,'051301':0,'051400':0,'051500':0,'051600':0,'055200':0,'055300':0,'055400':0,'150500':0,'200500':0,'':0,'200501':0,'250500':0}

    
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
    BDT_BCCBBIL_tbl=dict() 
    BDT_BCCBBIL_DEFN_DICT=createTableTypeDict('CAIMS_BDT_BCCBBIL',con,output_log)
    BDT_BALDTL_tbl=dict()
    BDT_BALDTL_DEFN_DICT=createTableTypeDict('CAIMS_BDT_BALDTL',con,output_log)
    BDT_CRNT1_tbl=dict()
    BDT_CRNT1_DEFN_DICT=createTableTypeDict('CAIMS_BDT_CRNT1',con,output_log)  
    BDT_CRNT2_tbl=dict()
    BDT_CRNT2_DEFN_DICT=createTableTypeDict('CAIMS_BDT_CRNT2',con,output_log)
    BDT_SWSPLCHG_tbl=dict()
    BDT_SWSPLCHG_DEFN_DICT=createTableTypeDict('CAIMS_BDT_SWSPLCHG',con,output_log)       
    BDT_PMNTADJ_tbl=dict()
    BDT_PMNTADJ_DEFN_DICT=createTableTypeDict('CAIMS_BDT_PMNTADJ',con,output_log)        
    BDT_ADJMTDTL_tbl=dict()
    BDT_ADJMTDTL_DEFN_DICT=createTableTypeDict('CAIMS_BDT_ADJMTDTL',con,output_log)         
         

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
#        print "line count is " +str(inputLineCnt  )
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

#            
#def log_header_rec_info():
#    debug("****** procedure==>  "+whereami()+" ******")
#    global bdt_input
#    global record_id
#    
#    system_yy=line[6:8]
#    system_mmdd=line[8:12]
#    system_time=line[12:21]
#    writelog("Input file: "+str(bdt_input))
#    writelog("Log file: "+str(output_log))
#    writelog("Header Record Info:")
#    writelog("     Record ID: "+record_id+" YY: "+system_yy+", MMDD: "+system_mmdd+", TIME: "+str(system_time))
#    
#    
#    count_record(record_id,False)
 



def log_footer_rec_info(): 
    #debug("****** procedure==>  "+whereami()+" ******")
    global record_id,output_log
    
    BAN_cnt=line[6:12]
    REC_cnt=line[13:22]
    writelog(" ",output_log)
    writelog("Footer Record Info:",output_log)
    writelog("     Record ID "+record_id+" BAN Count: "+BAN_cnt+" RECORD CNT: "+REC_cnt,output_log)
    writelog("The total number of lines counted in input file is : "+ str(inputLineCnt),output_log)
    writelog(" ",output_log)
    
    count_record(record_id,False)



def process_bill_records():
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_BCCBBIL_tbl
    global record_id,output_log
    global firstBillingRec
    global tstx, verno, tsty
   
    
    tstx=line[77:79]
    verno=line[82:84]
    tstz=line[161:163]
    
    if firstBillingRec:
        "FIRST RECORD ONLY"
        #write BOS version number to log
        writelog("**--------------------------**",output_log)
        writelog("** BOS VERSION NUMBER IS "+verno+" ** ",output_log)
        writelog("**--------------------------**",output_log)
        firstBillingRec=False
        
      
    unknownRecord=False
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
        process_TYP2005_ADJMTDTL()
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
    #debug("****** procedure==>  "+whereami()+" ******")
    
    if line[6:11].rstrip(' ') == '' or line[17:30].rstrip(' ') == '' or line[11:17].rstrip(' ') == '':
        badKey=True
    else:
        badKey=False
        
    return { 'ACNA':line[6:11],'EOB_DATE':line[11:17],'BAN':line[17:30]}
     
    
def reset_record_flags():
    "These flags are used to check if a record already exists for the current acna/ban/eob_date"
    "The first time a record is processed, if the count == 0, then an insert will be performed,"
    "else an update."
    global root_rec,baldue_rec,swsplchg_rec,baldtl_rec,pmntadj_rec,adjmtdtl_rec,crnt1_051200_rec,crnt1_055000_rec
    global dispdtl_rec
    
    root_rec=False
    baldue_rec=False
    swsplchg_rec=False
    baldtl_rec=False
    dispdtl_rec=False
    pmntadj_rec=False
    crnt1_051200_rec=False
    crnt1_055000_rec=False
   
    
def process_TYP0101_ROOT():
    #debug("****** procedure==>  "+whereami()+" ******")
    global firstBillingRec,output_log
    global verno
#    global BDT_BCCBBIL_tbl,  BDT_BCCBBIL_DEFN_DICT  
    global root_rec, record_id
    
    initialize_BCCBBIL_tbl()
#    "record_id doesn't need populated"
    
    "we already know ACNA, EOB_DATE, and BAN are the same"   
    BDT_BCCBBIL_tbl['JDATE']=line[71:76]
#    BDT_BCCBBIL_tbl['BANLOCK']='N'    #defaulted db value
    BDT_BCCBBIL_tbl['VERSION_NBR']=line[82:84]

    BDT_BCCBBIL_tbl['TLF']=line[97:98]
    BDT_BCCBBIL_tbl['NLATA']=line[98:101]
    BDT_BCCBBIL_tbl['HOLD_BILL']=line[105:106]
    BDT_BCCBBIL_tbl['TACCNT']=line[107:108]
    BDT_BCCBBIL_tbl['TFGRP']=line[108:109]   
    BDT_BCCBBIL_tbl['TACCNT_FGRP']=translate_TACCNT_FGRP(BDT_BCCBBIL_tbl['TACCNT'],BDT_BCCBBIL_tbl['TFGRP'])  
    BDT_BCCBBIL_tbl['MAN_BAN_TYPE']=line[218:219]
    BDT_BCCBBIL_tbl['UNB_SWA_PROV']=line[219:220]
    BDT_BCCBBIL_tbl['MPB']=line[224:225]
    BDT_BCCBBIL_tbl['BILL_DATE']=BDT_BCCBBIL_tbl['JDATE']
    BDT_BCCBBIL_tbl['EOBDATEA']=BDT_BCCBBIL_tbl['EOB_DATE']
    BDT_BCCBBIL_tbl['EOBDATECC']=BDT_BCCBBIL_tbl['EOBDATEA']
    BDT_BCCBBIL_tbl['BILLDATECC']=BDT_BCCBBIL_tbl['BILL_DATE']
    BDT_BCCBBIL_tbl['CAIMS_REL']='B'
    BDT_BCCBBIL_tbl['INPUT_RECORDS']=str(record_id)
 
    process_insert_table("CAIMS_BDT_BCCBBIL", BDT_BCCBBIL_tbl,BDT_BCCBBIL_DEFN_DICT,con,output_log)
    
    root_rec=True
 
    
def process_TYP0505_BO_CODE(): 
    #debug("****** procedure==>  "+whereami()+" ******")
    "BO_CODE"
    
#    global BDT_BCCBBIL_tbl  
    global root_rec, record_id,output_log
    
  
    initialize_BCCBBIL_tbl()
    "record_id doesn't need populated"
    
    "we already know ACNA, EOB_DATE, and BAN are the same"   

#    get bo_code
    BDT_BCCBBIL_tbl['BO_CODE']=line[211:215].rstrip(' ').lstrip(' ')
    BDT_BCCBBIL_tbl['INPUT_RECORDS']=str(record_id)
    
#    process_update_bccbbil() 
    if root_rec:
        process_update_table("CAIMS_BDT_BCCBBIL", BDT_BCCBBIL_tbl, BDT_BCCBBIL_DEFN_DICT,con,output_log)
    else:
#        process_insert_table("CAIMS_BDT_BCCBBIL", BDT_BCCBBIL_tbl, BDT_BCCBBIL_DEFN_DICT)
         process_ERROR_END("Trying to update BO_CODE but there is no root record. record_id: "+str(record_id),con)
         
    "no flag to set - part of root"

def process_TYP0510_BALDUE():
    #debug("****** procedure==>  "+whereami()+" ******")
    "BALDUE"   
#    global BDT_BCCBBIL_tbl 
    global baldue_rec,output_log
    global root_rec, record_id
  
 
    initialize_BCCBBIL_tbl()

    #CURR_INVOICE
    BDT_BCCBBIL_tbl['REF_NUM']=line[79:89]   
    
    
    #populate astate
    if line[37:39] == '  ':
        BDT_BCCBBIL_tbl['ASTATE']='XX'
    else:
        BDT_BCCBBIL_tbl['ASTATE']=line[37:39] 
    
    BDT_BCCBBIL_tbl['INVDATECC']=line[89:94]    
    BDT_BCCBBIL_tbl['PREVBAL']=convertnumber(line[97:108],2)
    BDT_BCCBBIL_tbl['PAYMNT']=convertnumber(line[108:119],2)
    BDT_BCCBBIL_tbl['ADJT']=convertnumber(line[119:130],2)
    BDT_BCCBBIL_tbl['ADJIR']=convertnumber(line[130:141],2)
    BDT_BCCBBIL_tbl['ADJIA']=convertnumber(line[141:152],2)
    BDT_BCCBBIL_tbl['ADJUS']=convertnumber(line[152:163],2)
    BDT_BCCBBIL_tbl['BAL']=convertnumber(line[163:174],2)
    BDT_BCCBBIL_tbl['ADLOC']=convertnumber(line[178:185],2) 
    BDT_BCCBBIL_tbl['INPUT_RECORDS']=str(record_id)
   
#    process_update_bccbbil() 
     
    if root_rec:
        process_update_table("CAIMS_BDT_BCCBBIL", BDT_BCCBBIL_tbl, BDT_BCCBBIL_DEFN_DICT,con,output_log)
    else:
        process_ERROR_END("BALDUE record needs a root record. Record id:"+str(record_id))  
        
    baldue_rec=True
 
 

def process_TYP0512_CRNT1():    
    #debug("****** procedure==>  "+whereami()+"(record id:"+str(record_id)+") ******")
#    global BDT_CRNT1_tbl 
    global BDT_CRNT1_DEFN_DICT, BDT_BALDTL_DEFN_DICT 
    global crnt1_051200_rec,crnt1_055000_rec,output_log
 
    #?not sure why record_id is recognized in this para when it is not declared global???
    
    #If Substate == 'XX' GOTO TOP    
    if line[77:79] =='XX':
        pass
    else:
    
        initialize_CRNT1_tbl()
        
        if not root_rec or not baldue_rec:
            writelog("ERROR???: Writing CRNT1 record but missing parent records.",output_log)
     
    #    BDT_CRNT1_tbl
    #    BUILD pieces here
        BDT_CRNT1_tbl['REF_NUM']=line[61:71]
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
            BDT_CRNT1_tbl['STLVCC']=convertnumber(line[221:225],2) 
            crnt1_051200_rec+=1
        elif record_id == '055000':
            BDT_CRNT1_tbl['MRCLO']=convertnumber(line[169:180],2)
            BDT_CRNT1_tbl['STLVCC']=convertnumber(line[217:221],2)         
            crnt1_055000_rec+=1
        else:
            process_ERROR_END("ERROR: Expected record_id 051200 or 055000 but recieved a record_id of "+str(record_id))
    
        BDT_CRNT1_tbl['INPUT_RECORDS']=str(record_id)
        
        
    #NEW CODE TO FIX BALDTL
    
     
        if baldtl_rec == False:

            initialize_BALDTL_tbl()
            BDT_BALDTL_tbl['DINVDATECC']=BDT_CRNT1_tbl['INVDT1CC'] 
            if BDT_CRNT1_tbl['SUBSTATE'].rstrip(' ') == '':
                BDT_BALDTL_tbl['DSTATE'] = 'nl'
            else:
                BDT_BALDTL_tbl['DSTATE']=BDT_CRNT1_tbl['SUBSTATE']  
             
            BDT_BALDTL_tbl['DPREVBAL']=0;
            BDT_BALDTL_tbl['DPAYMNT']=0;
            BDT_BALDTL_tbl['DINV_REF']=BDT_CRNT1_tbl['REF_NUM']
            BDT_BALDTL_tbl['DADJT']=0;
            BDT_BALDTL_tbl['DBAL']=0;
            BDT_BALDTL_tbl['LPC_APPLIED']=0;
            BDT_BALDTL_tbl['LPC_INV_IR']=0;
            BDT_BALDTL_tbl['LPC_INV_IA']=0;
            BDT_BALDTL_tbl['LPC_INV_ND']=0;
            BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id);
            process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,output_log)
            baldtl_rec ==True
        
    #END NEW CODE TO FIX BALDTL
        process_insert_table("CAIMS_BDT_CRNT1", BDT_CRNT1_tbl, BDT_CRNT1_DEFN_DICT,con,output_log)
                   
 
    
def process_TYP0513_CRNT2():    
    #debug("****** procedure==>  "+whereami()+"(record id:"+str(record_id)+") ******")
#    global BDT_CRNT2_tbl 
#    global BDT_CRNT2_DEFN_DICT 
    global output_log
    #?not sure why record_id is recognized in this para when it is not declared global???    
    
    initialize_CRNT2_tbl()
    
    if not root_rec or not baldue_rec:
        writelog("ERROR???: Writing crnt2 record but missing parent root record and baldue record.",output_log)
 
#REF_NUM/A10=' ';
#   FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X2 X6 X6 X12
#   FIXFORM REF_NUM/A10 INVDT2/A5 X1 SUBSTATE2/A2
#   FIXFORM TOT_OCC/Z11.2 OCCIR/Z11.2 OCCIA/Z11.2 OCCUS/Z11.2
#   FIXFORM TOT_USG/Z11.2 USGIR/Z11.2 USGIA/Z11.2 TOT_TAX/Z11.2
#   FIXFORM CURNTCHG/Z11.2 X12 TOT_SURCHG/Z11.2 X5 STLVCC2/A4
#   FIXFORM ON BCTFBDTI X105 OCCLO/Z11.2 USGLO/Z11.2

   
#    BDT_CRNT2_tbl
#    BUILD pieces here
    BDT_CRNT2_tbl['REF_NUM']=line[61:71]
    BDT_CRNT2_tbl['INVDT2CC']=line[71:76]
    BDT_CRNT2_tbl['SUBSTATE2']=line[77:79]
    BDT_CRNT2_tbl['TOT_OCC']=convertnumber(line[79:90],2)
    BDT_CRNT2_tbl['OCCIR']=convertnumber(line[90:101],2)
    BDT_CRNT2_tbl['OCCIA']=convertnumber(line[101:112],2)
    BDT_CRNT2_tbl['OCCUS']=convertnumber(line[112:123],2)
    BDT_CRNT2_tbl['TOT_USG']=convertnumber(line[123:134],2)
    BDT_CRNT2_tbl['USGIR']=convertnumber(line[134:145],2)
    BDT_CRNT2_tbl['USGIA']=convertnumber(line[145:156],2)
    BDT_CRNT2_tbl['TOT_TAX']=convertnumber(line[156:167],2)
    BDT_CRNT2_tbl['CRNTCHG']=convertnumber(line[167:178],2)
    BDT_CRNT2_tbl['INPUT_RECORDS']=str(record_id)
    if record_id == '051300':
        BDT_CRNT2_tbl['TOT_SURCHG']=convertnumber(line[190:201],2)
        BDT_CRNT2_tbl['STLVCC2']=convertnumber(line[206:210],2)
        "DONT UPDATE OR INSERT A RECORD, the 051300 should always be followed by a 051301 record"
        " to populate OCCLO and USGLO records, so return will go back to read another record"
        " and OCCLO and USGLO will be populated by the TYP05131 module"
    if record_id == '055100':
        BDT_CRNT2_tbl['OCCLO']=convertnumber(line[178:189],2)
        BDT_CRNT2_tbl['STLVCC2']=convertnumber(line[205:209],2)
        BDT_CRNT2_tbl['USGLO']=convertnumber(line[209:220],2)
        process_insert_table("CAIMS_BDT_CRNT2", BDT_CRNT2_tbl, BDT_CRNT2_DEFN_DICT,con,output_log)


    #NEW CODE TO FIX BALDTL
    #Not sure if this code is ever hit?
        if baldtl_rec == False:
            initialize_BALDTL_tbl()
            BDT_BALDTL_tbl['DINVDATECC']=BDT_CRNT2_tbl['INVDT2CC']
            if BDT_CRNT2_tbl['SUBSTATE2'].rstrip(' ') == '':
                BDT_BALDTL_tbl['DSTATE'] = 'nl'
            else:
                BDT_BALDTL_tbl['DSTATE']=BDT_CRNT2_tbl['SUBSTATE2']
            BDT_BALDTL_tbl['DPREVBAL']=0;
            BDT_BALDTL_tbl['DPAYMNT']=0;
            BDT_BALDTL_tbl['DINV_REF']=BDT_CRNT2_tbl['REF_NUM']
            BDT_BALDTL_tbl['DADJT']=0;
            BDT_BALDTL_tbl['DBAL']=0;
            BDT_BALDTL_tbl['LPC_APPLIED']=0;
            BDT_BALDTL_tbl['LPC_INV_IR']=0;
            BDT_BALDTL_tbl['LPC_INV_IA']=0;
            BDT_BALDTL_tbl['LPC_INV_ND']=0;
            BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id);
            process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,output_log) 
            baldtl_rec ==True
    #END NEW CODE TO FIX BALDTL




    
   
def process_TYP05131_CRNT2():    
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_CRNT2_tbl 
#    global BDT_CRNT2_DEFN_DICT 
    global output_log
    #?not sure why record_id is recognized in this para when it is not declared global??? 

    
    "Dont initialize.  Initialization was done in TYPE0513 para"
#    initialize_CRNT2_tbl()

#   FIXFORM ON BCTFBDTI X105 OCCLO/Z11.2 USGLO/Z11.2

#    BDT_CRNT2_tbl
#    BUILD pieces here
   
    BDT_CRNT2_tbl['OCCLO']=line[105:116]
    BDT_CRNT2_tbl['USGLO']=line[116:127]
    BDT_CRNT2_tbl['INPUT_RECORDS']+="*"+str(record_id)
    
    if prev_record_id == '051300':
        process_insert_table("CAIMS_BDT_CRNT2", BDT_CRNT2_tbl, BDT_CRNT2_DEFN_DICT,con,output_log)
    else:
        process_ERROR_END("ERROR: Previous record should have been a 051300.")
                  
    
def process_TYP0514_SWSPLCHG():  
    #debug("****** procedure==>  "+whereami()+"(record id:"+str(record_id)+") ******")
#    global BDT_SWSPLCHG_tbl       
#    global BDT_SWSPLCHG_DEFN_DICT 
    global swsplchg_rec 
    global output_log
    
    #?not sure why record_id is recognized in this para when it is not declared global???     
    
    initialize_SWSPLCHG_tbl()
  
    state_ind=line[76:78]
    if state_ind == 'XX':
        pass
    else:
        
        if not root_rec or not baldue_rec:
            writelog("ERROR???: Writing crnt2 record but missing parent records.",output_log)
    
    
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
         
        if record_id in ('051400','051600','055200'):
            BDT_SWSPLCHG_tbl['MACND']=0
            BDT_SWSPLCHG_tbl['MACIRIA']=convertnumber(line[126:137],2)
            BDT_SWSPLCHG_tbl['MACIAIA']=convertnumber(line[137:148],2)
            BDT_SWSPLCHG_tbl['MACLOC']=convertnumber(line[148:159],2)
            
        if record_id in ('051500','055300'):
            BDT_SWSPLCHG_tbl['MACND']=line[126:137]
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
     
        process_insert_table("CAIMS_BDT_SWSPLCHG", BDT_SWSPLCHG_tbl, BDT_SWSPLCHG_DEFN_DICT,con,output_log)
        
        swsplchg_rec=True
        
    

def process_TYP1505_PMNTADJ():
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_PMNTADJ_tbl
    global pmntadj_rec
#    global BDT_PMNTADJ_DEFN_DICT
    global record_id
    global output_log

#FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X5 X2 X6 X18
#FIXFORM AINV_REF/A10 AINV_DATE/A5
#FIXFORM DATERCVCC/8 X48 APSTATE/A2 X15
#FIXFORM AMOUNT/Z11.2

    initialize_PMNTADJ_tbl()
    
    if record_id == '150500':
        BDT_PMNTADJ_tbl['PORA']='P'        
    BDT_PMNTADJ_tbl['AINV_REF']=line[61:71]
    BDT_PMNTADJ_tbl['AINV_DATE']=line[71:76]
    BDT_PMNTADJ_tbl['DATERCVCC']=line[76:84]
    BDT_PMNTADJ_tbl['APSTATE']=line[132:133]
    BDT_PMNTADJ_tbl['AMOUNT']=convertnumber(line[149:160],2)
    BDT_PMNTADJ_tbl['INPUT_RECORDS']=str(record_id)
    
    #NEW CODE TO FIX BALDTL
    #Not sure if this code is ever hit?
    if baldtl_rec == False:
        initialize_BALDTL_tbl()
        BDT_BALDTL_tbl['DINVDATECC']=BDT_PMNTADJ_tbl['AINV_DATE'] 
        if BDT_PMNTADJ_tbl['APSTATE'].rstrip(' ') == '':
            BDT_BALDTL_tbl['DSTATE'] = 'nl'
        else:
            BDT_BALDTL_tbl['DSTATE']=BDT_PMNTADJ_tbl['APSTATE']
        
        BDT_BALDTL_tbl['DPREVBAL']=0;
        BDT_BALDTL_tbl['DPAYMNT']=0;
        BDT_BALDTL_tbl['DINV_REF']=BDT_PMNTADJ_tbl['AINV_REF']
        BDT_BALDTL_tbl['DADJT']=0;
        BDT_BALDTL_tbl['DBAL']=0;
        BDT_BALDTL_tbl['LPC_APPLIED']=0;
        BDT_BALDTL_tbl['LPC_INV_IR']=0;
        BDT_BALDTL_tbl['LPC_INV_IA']=0;
        BDT_BALDTL_tbl['LPC_INV_ND']=0;
        BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id);
        process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,output_log)
        baldtl_rec ==True
    #END NEW CODE TO FIX BALDTL





    process_insert_table("CAIMS_BDT_PMNTADJ", BDT_PMNTADJ_tbl, BDT_PMNTADJ_DEFN_DICT,con,output_log) 

    pmntadj_rec=True

def process_TYP2005_ADJMTDTL():                    
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_ADJMTDTL_tbl
    global adjmtdtl_rec, record_id
 
    
#FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X9 X8 ADJMT_SER_NO/10
#FIXFORM X4 AINV_REF/A10 AINV_DATE/A5
#FIXFORM DATERCVCC/8 PHRASE_CD/3 X2 X45 AUDITNUM/16
#FIXFORM INTER_INTRA/1 AMOUNT/Z11.2 X45 X2 X9 APSTATE/2 APSTLVCC/A4 X7
#  
    initialize_BDT_ADJMTDTL_tbl()
    
    BDT_ADJMTDTL_tbl['ADJMT_SER_NO']=line[47:57]  
    BDT_ADJMTDTL_tbl['PORA']='A'
    BDT_ADJMTDTL_tbl['AINV_REF']=line[61:71]
    BDT_ADJMTDTL_tbl['AINV_DATE']=line[71:76]
    BDT_ADJMTDTL_tbl['DATERCVCC']=line[76:84]
    BDT_ADJMTDTL_tbl['PHRASE_CD']=line[84:87]
    BDT_ADJMTDTL_tbl['AUDITNUM']=line[134:150]
    BDT_ADJMTDTL_tbl['INTER_INTRA']=line[150:151]
    BDT_ADJMTDTL_tbl['AMOUNT']=line[151:162]
    BDT_ADJMTDTL_tbl['APSTATE']=line[218:220]
    BDT_ADJMTDTL_tbl['APSTLVCC']=line[221:225]
    
    BDT_ADJMTDTL_tbl['INPUT_RECORDS']=str(record_id)
 
    adjmtdtl_rec=True  
    
    
def process_TYP20051_ADJMTDTL():
    #debug("****** procedure==>  "+whereami()+" ******") 
#    global BDT_ADJMTDTL_tbl
    global adjmtdtl_rec
#    global BDT_ADJMTDTL_DEFN_DICT
    global record_id
    global output_log
#FIXFORM ON BCTFBDTI X91 ADJ_FACTYP/1 X3 PF_IND/1 X13
#FIXFORM ADJ_FRDTCC/8 ADJ_THRUDTCC/8 CKT_ID/A47 X39 ADJ_ACCTYP/1
#FIXFORM X2 BUS_RSDC_IND/A1 PF_BAND1/3 PF_BAND2/3
#COMPUTE
#AINVDATE0/I5=EDIT(AINV_DATE);
#AINVDATEA/I6YMD=GREGDT(AINVDATE0, 'I6');
#AINVDATEB/YMD=AINVDATEA;
#AINVDATECC=AINVDATEB;
#DINVDATECC=AINVDATECC;
#DTL_INVOICE/A15=AINV_REF|AINV_DATE;
#DSTATE=APSTATE;    
    
    BDT_ADJMTDTL_tbl['ADJ_FACTYP']=line[91:92]
    BDT_ADJMTDTL_tbl['PF_IND']=line[96:97]
    BDT_ADJMTDTL_tbl['ADJ_FRDTCC']=line[109:117]
    BDT_ADJMTDTL_tbl['ADJ_THRUDTCC']=line[117:125]
    BDT_ADJMTDTL_tbl['CKT_ID']=line[125:172]
    BDT_ADJMTDTL_tbl['ADJ_ACCTYP']=line[211:212]
    BDT_ADJMTDTL_tbl['BUS_RSDC_IND']=line[214:215]
    BDT_ADJMTDTL_tbl['PF_BAND1']=line[215:218]
    BDT_ADJMTDTL_tbl['PF_BAND2']=line[218:221]
    
    
    BDT_ADJMTDTL_tbl['INPUT_RECORDS']+="*"+str(record_id)
    
    #NEW CODE TO FIX BALDTL
    #Not sure if this code is ever hit?
    if baldtl_rec == False:
        initialize_BALDTL_tbl()
        BDT_BALDTL_tbl['DINVDATECC']=BDT_ADJMTDTL_tbl['AINV_DATE'] 
        if BDT_ADJMTDTL_tbl['APSTATE'] .rstrip(' ') == '':
            BDT_BALDTL_tbl['DSTATE'] = 'nl'
        else:
            BDT_BALDTL_tbl['DSTATE']=BDT_ADJMTDTL_tbl['APSTATE']        
        
        
        BDT_BALDTL_tbl['DSTATE']=BDT_ADJMTDTL_tbl['APSTATE']
        BDT_BALDTL_tbl['DPREVBAL']=0;
        BDT_BALDTL_tbl['DPAYMNT']=0;
        BDT_BALDTL_tbl['DINV_REF']=BDT_ADJMTDTL_tbl['AINV_REF']
        BDT_BALDTL_tbl['DADJT']=0;
        BDT_BALDTL_tbl['DBAL']=0;
        BDT_BALDTL_tbl['LPC_APPLIED']=0;
        BDT_BALDTL_tbl['LPC_INV_IR']=0;
        BDT_BALDTL_tbl['LPC_INV_IA']=0;
        BDT_BALDTL_tbl['LPC_INV_ND']=0;
        BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id);
        process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,output_log)
        baldtl_rec ==True
    #END NEW CODE TO FIX BALDTL
    
    
    
    
    
    if prev_record_id == '200500':
        process_insert_table("CAIMS_BDT_ADJMTDTL", BDT_ADJMTDTL_tbl, BDT_ADJMTDTL_DEFN_DICT,con,output_log)
    else:
        process_ERROR_END("ERROR: previous record should have been a 200500.")
               
    adjmtdtl_rec=True   
    
def process_TYP2505_BALDTL():                
    #debug("****** procedure==>  "+whereami()+" ******")
    "250500"
    global BDT_BALDTL_tbl 
    global baldtl_rec
    global record_id
    global output_log
    
    initialize_BALDTL_tbl()    
    
     #DTL_INVOICE-concatenation of DINV_REF and DINV_DATE
#FIXFORM X-225 ACNA/A5 EOB_DATE/6 BAN/A13 X13 X5 X2 X1 X6 X4
#FIXFORM DINV_REF/A10 DINV_DATE/A5 DSTATE/A2 X38
#FIXFORM DPREVBAL/Z11.2 DPAYMNT/Z11.2 DADJT/Z11.2 DBAL/Z11.2
#FIXFORM X4 LPC_APPLIED/Z11.2
#    COMPUTE
#    INVDATECC=INVDATECC
#    ASTATE=ASTATE   (ROOT SEGMENT)
     
#    BDT_BCCBBIL_tbl['ASTATE'] 
#    BDT_BCCBBIL_tbl['INVDATECC']
    
    BDT_BALDTL_tbl['DINV_REF']=line[61:71]
    BDT_BALDTL_tbl['DINVDATECC']=line[71:76]
    if line[76:78].rstrip(' ') == '':
        BDT_BALDTL_tbl['DSTATE']='nl'        
    else:
        BDT_BALDTL_tbl['DSTATE']=line[76:78]
        
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
    tmpTblRec['ACNA']=BDT_BALDTL_tbl['ACNA']
    tmpTblRec['BAN']=BDT_BALDTL_tbl['BAN']
    tmpTblRec['EOB_DATE']=BDT_BALDTL_tbl['EOB_DATE']
    tmpTblRec['DINVDATECC']=BDT_BALDTL_tbl['DINVDATECC']
    tmpTblRec['DSTATE']=BDT_BALDTL_tbl['DSTATE']
    
                
    if (process_check_exists("CAIMS_BDT_BALDTL", tmpTblRec, BDT_BALDTL_DEFN_DICT,con,output_log)):   
        process_update_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,output_log)
    else:
        if baldue_rec:
            process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,output_log) 
        else:
            process_insert_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,output_log) 
            writelog("WARNING: The BALDTL record "+str(record_id)+" has no associated BALDUE record. INSERTING ANYWAY.",output_log)
    
    baldtl_rec=True

def process_TYP2715_BALDTL():
    #debug("****** procedure==>  "+whereami()+" ******")
    "271500"
    #Add this paragraph for combined company data.
    global BDT_BALDTL_tbl 
    global baldtl_rec
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
        BDT_BALDTL_tbl['INPUT_RECORDS']+="*"+str(record_id);
        process_update_table("CAIMS_BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT,con,output_log) 
    else:
        writelog("WARNING: The BALDTL record "+str(record_id)+" has no associated BALDUE record. INSERTING ANYWAY.",output_log)
    

    
def initialize_BDT_ADJMTDTL_tbl():            
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_ADJMTDTL_tbl
    global current_abbd_rec_key 
    

    BDT_ADJMTDTL_tbl['ACNA']=current_abbd_rec_key['ACNA']
    BDT_ADJMTDTL_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    BDT_ADJMTDTL_tbl['BAN']=current_abbd_rec_key['BAN']
    
 
    BDT_ADJMTDTL_tbl['ADJMT_SER_NO']=''
    BDT_ADJMTDTL_tbl['PORA']=''
    BDT_ADJMTDTL_tbl['AINV_REF']=''
    BDT_ADJMTDTL_tbl['AINV_DATE']=''
    BDT_ADJMTDTL_tbl['DATERCVCC']=''
    BDT_ADJMTDTL_tbl['PHRASE_CD']=''
    BDT_ADJMTDTL_tbl['AUDITNUM']=''
    BDT_ADJMTDTL_tbl['INTER_INTRA']=''
    BDT_ADJMTDTL_tbl['AMOUNT']=''
    BDT_ADJMTDTL_tbl['APSTATE']=''
    BDT_ADJMTDTL_tbl['APSTLVCC']=''
    BDT_ADJMTDTL_tbl['ADJ_FACTYP']=''
    BDT_ADJMTDTL_tbl['PF_IND']=''
    BDT_ADJMTDTL_tbl['ADJ_FRDTCC']=''
    BDT_ADJMTDTL_tbl['ADJ_THRUDTCC']=''
    BDT_ADJMTDTL_tbl['CKT_ID']=''
    BDT_ADJMTDTL_tbl['ADJ_ACCTYP']=''
    BDT_ADJMTDTL_tbl['BUS_RSDC_IND']=''
    BDT_ADJMTDTL_tbl['PF_BAND1']=''
    BDT_ADJMTDTL_tbl['PF_BAND2']=''
    BDT_ADJMTDTL_tbl['INPUT_RECORDS']=''
        
    
    
    
    
    
 
def initialize_PMNTADJ_tbl():            
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_PMNTADJ_tbl,
    global current_abbd_rec_key
   
    BDT_PMNTADJ_tbl['ACNA']=current_abbd_rec_key['ACNA']
    BDT_PMNTADJ_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    BDT_PMNTADJ_tbl['BAN']=current_abbd_rec_key['BAN']

    BDT_PMNTADJ_tbl['PORA']='P'        
    BDT_PMNTADJ_tbl['AINV_REF']=''
    BDT_PMNTADJ_tbl['AINV_DATE']=''
    BDT_PMNTADJ_tbl['DATERCVCC']=''
    BDT_PMNTADJ_tbl['APSTATE']=''
    BDT_PMNTADJ_tbl['AMOUNT']=''
    BDT_PMNTADJ_tbl['INPUT_RECORDS']=''
     
     
     
def initialize_BALDTL_tbl():
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_BALDTL_tbl,
    global current_abbd_rec_key

    BDT_BALDTL_tbl['ACNA']=current_abbd_rec_key['ACNA']
    BDT_BALDTL_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    BDT_BALDTL_tbl['BAN']=current_abbd_rec_key['BAN']
    
    BDT_BALDTL_tbl['DINV_REF']=''
    BDT_BALDTL_tbl['DINVDATECC']=''
    BDT_BALDTL_tbl['DSTATE']=''
    BDT_BALDTL_tbl['DPREVBAL']=''
    BDT_BALDTL_tbl['DPAYMNT']=''
    BDT_BALDTL_tbl['DADJT']=''
    BDT_BALDTL_tbl['DBAL']=''
    BDT_BALDTL_tbl['LPC_APPLIED']=''
    BDT_BALDTL_tbl['LPC_INV_IR']=''   
    BDT_BALDTL_tbl['LPC_INV_IA']=''
    BDT_BALDTL_tbl['LPC_INV_ND']=''      
    BDT_BALDTL_tbl['INPUT_RECORDS']=''
 

 
def  initialize_BCCBBIL_tbl():
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_BCCBBIL_tbl,
    global current_abbd_rec_key 
    
    BDT_BCCBBIL_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    BDT_BCCBBIL_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    BDT_BCCBBIL_tbl['BAN']=current_abbd_rec_key['BAN']

    BDT_BCCBBIL_tbl['JDATE']=''
    BDT_BCCBBIL_tbl['VERSION_NBR']=''
    BDT_BCCBBIL_tbl['TLF']=''
    BDT_BCCBBIL_tbl['NLATA']=''
    BDT_BCCBBIL_tbl['HOLD_BILL']=''
    BDT_BCCBBIL_tbl['TACCNT']=''
    BDT_BCCBBIL_tbl['TFGRP']=''
    BDT_BCCBBIL_tbl['TACCNT_FGRP']=''
    BDT_BCCBBIL_tbl['MAN_BAN_TYPE']=''
    BDT_BCCBBIL_tbl['UNB_SWA_PROV']=''
    BDT_BCCBBIL_tbl['BILL_DATE']=''
    BDT_BCCBBIL_tbl['EOBDATEA']=''
    BDT_BCCBBIL_tbl['EOBDATECC']=''
    BDT_BCCBBIL_tbl['BILLDATECC']=''
    BDT_BCCBBIL_tbl['CAIMS_REL']=''
    BDT_BCCBBIL_tbl['MPB']=''             
    BDT_BCCBBIL_tbl['BO_CODE']=''
#BALDUE items
    BDT_BCCBBIL_tbl['ASTATE']=''
    BDT_BCCBBIL_tbl['PREVBAL']=''
    BDT_BCCBBIL_tbl['REF_NUM']=''
    BDT_BCCBBIL_tbl['INVDATECC']=''
    BDT_BCCBBIL_tbl['PAYMNT']=''
    BDT_BCCBBIL_tbl['ADJT']=''
    BDT_BCCBBIL_tbl['ADJIR']=''
    BDT_BCCBBIL_tbl['ADJIA']=''
    BDT_BCCBBIL_tbl['ADJUS']=''
    BDT_BCCBBIL_tbl['BAL']=''
    BDT_BCCBBIL_tbl['ADLOC']=''
    BDT_BCCBBIL_tbl['INPUT_RECORDS']=''
 
    
def  initialize_CRNT1_tbl():
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_CRNT1_tbl
    global current_abbd_rec_key 
    
    BDT_CRNT1_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    BDT_CRNT1_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    BDT_CRNT1_tbl['BAN']=current_abbd_rec_key['BAN']

    BDT_CRNT1_tbl['REF_NUM']=''
    BDT_CRNT1_tbl['INVDT1CC']=''
    BDT_CRNT1_tbl['SUBSTATE']=''
    BDT_CRNT1_tbl['MONCHGFROMCC']=''
    BDT_CRNT1_tbl['MONCHGTHRUCC']='' 
    BDT_CRNT1_tbl['DTINVDUECC']='' 
    BDT_CRNT1_tbl['LPC']=''
    BDT_CRNT1_tbl['TOT_MRC']=''
    BDT_CRNT1_tbl['MRCIR']=''
    BDT_CRNT1_tbl['MRCIA']=''
    BDT_CRNT1_tbl['MRCLO']=''
    BDT_CRNT1_tbl['STLVCC']=''
    BDT_CRNT1_tbl['INPUT_RECORDS']=''
    
def  initialize_CRNT2_tbl():
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_CRNT2_tbl, 
    global current_abbd_rec_key 
    
    BDT_CRNT2_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    BDT_CRNT2_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    BDT_CRNT2_tbl['BAN']=current_abbd_rec_key['BAN']

    BDT_CRNT2_tbl['REF_NUM']=''
    BDT_CRNT2_tbl['INVDT2CC']=''
    BDT_CRNT2_tbl['SUBSTATE2']=''
    BDT_CRNT2_tbl['TOT_OCC']=''
    BDT_CRNT2_tbl['OCCIR']=''
    BDT_CRNT2_tbl['OCCIA']=''
    BDT_CRNT2_tbl['OCCUS']=''
    BDT_CRNT2_tbl['TOT_USG']=''
    BDT_CRNT2_tbl['USGIR']=''
    BDT_CRNT2_tbl['USGIA']=''
    BDT_CRNT2_tbl['TOT_TAX']=''
    BDT_CRNT2_tbl['CRNTCHG']=''
    BDT_CRNT2_tbl['TOT_SURCHG']=''
    BDT_CRNT2_tbl['STLVCC2']=''
    BDT_CRNT2_tbl['OCCLO']=''
    BDT_CRNT2_tbl['USGLO']=''
    BDT_CRNT2_tbl['INPUT_RECORDS']=''



def  initialize_SWSPLCHG_tbl():
    #debug("****** procedure==>  "+whereami()+" ******")
#    global BDT_SWSPLCHG_tbl,
    global current_abbd_rec_key 
    
    BDT_SWSPLCHG_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    BDT_SWSPLCHG_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    BDT_SWSPLCHG_tbl['BAN']=current_abbd_rec_key['BAN']

    BDT_SWSPLCHG_tbl['STATE_IND']=''
    BDT_SWSPLCHG_tbl['ST_LVLCC']=''
    BDT_SWSPLCHG_tbl['CHGFROMDTCC']=''
    BDT_SWSPLCHG_tbl['CHGTHRUDTCC']='' 
    BDT_SWSPLCHG_tbl['MAC_ACCTYP']=''
    BDT_SWSPLCHG_tbl['MAC_FACTYP']=''
    BDT_SWSPLCHG_tbl['MACIR']=''
    BDT_SWSPLCHG_tbl['MACIA']='' 
    BDT_SWSPLCHG_tbl['MACIRIA']=''
    BDT_SWSPLCHG_tbl['MACIAIA']=''
    BDT_SWSPLCHG_tbl['MACLOC']=''
    BDT_SWSPLCHG_tbl['MAC_RECTYP']=''
    BDT_SWSPLCHG_tbl['MACND']=''
    BDT_SWSPLCHG_tbl['INPUT_RECORDS']=''
 
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
    
    writelog("\n  Total count: "+str(unkCnt),output_log)    
    writelog("**",output_log)    
    
    writelog("TOTAL ACNA/EOB_DATE/BAN's processed:"+str(BDT_KEY_cnt),output_log)
    writelog(" ",output_log)
    writelog("Total input records read from input file:"+str(idCnt+unkCnt),output_log)
    writelog(" ",output_log)    
 
    
def process_ERROR_END(msg):
    #debug("****** procedure==>  "+whereami()+" ******")
    writelog("ERROR:"+msg,output_log)
    #debug("ERROR:"+msg)
    con.commit()
    con.close()
    process_close_files()
    raise Exception("ERROR:"+msg)

    
def process_close_files():
    #debug("****** procedure==>  "+whereami()+" ******")
    global bdt_input
    global output_log
    
#    if DEBUGISON:
#        DEBUG_LOG.close()
        
    bdt_input.close();
    output_log.close()
   
    
    
def endProg(msg):
    #debug("****** procedure==>  "+whereami()+" ******")
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
#try:
    
#except Exception as e:
#    process_ERROR_END(e.message)
#else:
endProg("-END OF PROGRAM-")
