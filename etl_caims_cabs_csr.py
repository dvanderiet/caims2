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
import inspect
#import collections
import collections
startTM=datetime.datetime.now();
import cx_Oracle
import sys
import ConfigParser
import os.path

settings = ConfigParser.ConfigParser();
settings.read('settings.ini')
settings.sections()

"SET ORACLE CONNECTION"
con=cx_Oracle.connect(settings.get('OracleSettings','OraCAIMSUser'),settings.get('OracleSettings','OraCAIMSPw'),settings.get('OracleSettings','OraCAIMSSvc'))
 
    
"CONSTANTS"
#set to true to get debug statements
debugOn=True
if debugOn:
    csr_debug_log = open(settings.get('CSRSettings','CSR_DEBUG'), "w");  


#SET FILE NAME AND PATH
#--Path comes from settings.ini
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

path=settings.get('CSRSettings','CSR_CABS_inDir') 
  
#fullname="C:\\Users\\dxvand3\\My Documents\\python scripts\\in\\BDTNov2015\\"
#"PBCL.CY.XRU0102O.CABS.G0353V00.txt"
fullname =path+fileNm

if os.path.isfile(fullname):
    print "yes its there"
else:
    raise Exception("ERROR: File not found:"+fullname)
















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
 
 
"GLOBAL VARIABLES"     
record_id=''
badKey=False
current_abbd_rec_key={'ACNA':'x','EOB_DATE':'1','BAN':'x'}
prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}   #EOB_DATE,ACNA,BAN key   
 
record_counts={}
unknown_record_counts={}
CSR_REC='40' 
STAR_LINE='*********************************************************************************************'  
MONTH_DICT={'01':'JAN','02':'FEB','03':'MAR','04':'APR','05':'MAY','06':'JUN','07':'JUL','08':'AUG','09':'SEP','10':'OCT','11':'NOV','12':'DEC',}
#http://www.3480-3590-data-conversion.com/article-signed-fields.html
DIGIT_DICT={'{':'0','A':'1','B':'2','C':'3','D':'4','E':'5','F':'6','G':'7','H':'8','I':'9',\
            '}':'0','J':'1','K':'2','L':'3','M':'4','N':'5','O':'6','P':'7','Q':'8','R':'9'} 
NEGATIVE_NUMS=['}','J','K','L','M','N','O','P','Q','R']

   
"TRANSLATERS"

def debug(msg):
    if debugOn:
        csr_debug_log.write("\n"+str(msg))

def whereami():
    return inspect.stack()[1][3]         
    
def init():
    debug("****** procedure==>  "+whereami()+" ******")
    
    global csr_input 
    global csr_BCCBSPL_log
    global record_id
  
    "OPEN FILES"
    "   CABS INPUT FILE"
    csr_input = open(fullname, "r");
    
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
    log_file=str(settings.get('CSRSettings','CSR_LOG_DIR'))
    if log_file.endswith("/"):
        pass
    else:
        log_file+="/"
        
    log_file = str(settings.get('CSRSettings','CSR_LOG_DIR'))+"CSR_Cycle"+str(cycl_yy)+str(cycl_mmdd)+str(cycl_time)+"_"+startTM.strftime("%Y%m%d_@%H%M") +".txt"
         
    csr_BCCBSPL_log = open(log_file, "w");
    csr_BCCBSPL_log.write("-CSR CAIMS ETL PROCESS-")
    
    if record_id not in settings.get('CSRSettings','CSRHDR'):
        process_ERROR_END("The first record in the input file was not a "+str(settings.get('CSRSettings','CSRHDR')).rstrip(' ')+" record")

    writelog("Process "+sys.argv[0])
    writelog("   started execution at: " + str(startTM))
    writelog(STAR_LINE)
    writelog(" ")
    writelog("Input file: "+str(csr_input))
    writelog("Log file: "+str(csr_BCCBSPL_log))
    
    "Write header record informatio only"
    writelog("Header Record Info:")
    writelog("     Record ID: "+record_id+" YY: "+cycl_yy+", MMDD: "+cycl_mmdd+", TIME: "+str(cycl_time))
    
    count_record(record_id,False)
    del headerLine,cycl_yy,cycl_mmdd,cycl_time
    
def main():
    debug("****** procedure==>  "+whereami()+" ******")
    #CSR_config.initialize_CSR() 
    global record_type
    global line, Level
    global record_counts, unknown_record_counts
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
    global CSR_CFID_tbl, CSR_CFID_DEFN_DICT
    
    
    "text files"
    global csr_input, csr_BCCBSPL_log
    
      
    global CSR_column_names
    global record_id
    global prev_record_id
    global current_abbd_rec_key 
    global prev_abbd_rec_key   
    global goodKey      
    
#DECLARE TABLE ARRAYS AND DICTIONARIES
    CSR_BCCBSPL_tbl=collections.OrderedDict() 
    CSR_BCCBSPL_DEFN_DICT=createTableTypeDict('CAIMS_CSR_BCCBSPL')     
    CSR_BILLREC_tbl=collections.OrderedDict()
    CSR_BILLREC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_BILLREC')
    CSR_ACTLREC_tbl=collections.OrderedDict()
    CSR_ACTLREC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_ACTLREC')
    CSR_CKT_tbl=collections.OrderedDict()
    CSR_CKT_DEFN_DICT=createTableTypeDict('CAIMS_CSR_CKT')    
    CSR_LOC_tbl=collections.OrderedDict()
    CSR_LOC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_LOC')    
    CSR_LFID_tbl=collections.OrderedDict()
    CSR_LFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_LFID') 
    CSR_COSFID_tbl=collections.OrderedDict()
    CSR_COSFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_COSFID') 
    CSR_UFID_tbl=collections.OrderedDict()
    CSR_UFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_UFID') 
    CSR_USOC_tbl=collections.OrderedDict()
    CSR_USOC_DEFN_DICT=createTableTypeDict('CAIMS_CSR_USOC') 
    CSR_CFID_tbl=collections.OrderedDict()
    CSR_CFID_DEFN_DICT=createTableTypeDict('CAIMS_CSR_CFID') 

    "COUNTERS"
    inputLineCnt=0
    CSR_KEY_cnt=0
    status_cnt=0
    "KEY"
    
    prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}
    Level=' '
    "LOOP THROUGH INPUT CABS TEXT FILE"
    for line in csr_input:
      
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
          
        if record_type == CSR_REC:
            "START-MAIN PROCESS LOOP"
            "GET KEY OF NEXT RECORD"
            current_abbd_rec_key=process_getkey()
            if badKey:
                count_record("BAD_ABD_KEY",True)
                writelog("WARNING: BAD INPUT DATA.  ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'])
            else:
                if current_abbd_rec_key != prev_abbd_rec_key:
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
            writelog("ERROR: Not sure what type of record this is:")
            writelog(line)
         
         
        prev_record_id=record_id
        "END-MAIN PROCESS LOOP"    
    #END of For Loop
       
 
###############################################main end#########################################################
 


def log_footer_rec_info(): 
    debug("****** procedure==>  "+whereami()+" ******")
    global record_id
    global Level
    
    BAN_cnt=line[6:12]
    REC_cnt=line[13:22]
    writelog(" ")
    writelog("Footer Record Info:")
    writelog("     Record ID "+record_id+" BAN Count: "+BAN_cnt+" RECORD CNT: "+REC_cnt)
    writelog("The total number of lines counted in input file is : "+ str(inputLineCnt))
    writelog(" ")
    
    count_record(record_id,False)



def process_csr_records():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_BCCBSPL_tbl
    global record_id, Level
    global tstx, verno, tsty, UACT
      
    dtst=line[224:225]  
    unknownRecord=False
    if record_id == '010100':
        process_TYP0101_HEADREC()
    elif record_id == '050500':
        process_ROOTREC_TYP0505()   #INSERT
    elif record_id in ('051000','051100'):
        process_ROOTREC_CHEKFID() #UPDATE
    elif record_id == '100500':
        process_BILLREC_BILLTO()  
    elif record_id == '101000':
        process_ACTLREC_BILLTO() 
    elif record_id == '101500':       #udpate root record
        process_ROOTREC_UPROOT() 
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
        
        
    else:  #UNKNOWN RECORDS
        unknownRecord=True
    
    count_record(record_id,unknownRecord)

        
    
    
def process_getkey():
    debug("****** procedure==>  "+whereami()+" ******")
    global badKey
    
    if line[6:11].rstrip(' ') == '' or line[17:30].rstrip(' ') == '' or line[11:17].rstrip(' ') == '':
        badKey=True
    else:
        badKey=False
        
    return { 'ACNA':line[6:11],'EOB_DATE':line[11:17],'BAN':line[17:30]}
     
    
def reset_record_flags():
    debug("****** procedure==>  "+whereami()+" ******")    
    "These flags are used to check if a record already exists for the current acna/ban/eob_date"
    "The first time a record is processed, if the count == 0, then an insert will be performed,"
    "else an update."
    global csr_tape_val   
    csr_tape_val=''    
    
    global root_rec,baldue_rec,swsplchg_rec,baldtl_rec,pmntadj_rec,adjmtdtl_rec,crnt1_051200_rec,crnt1_055000_rec,crnt2_rec
    global dispdtl_rec
    
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
    debug("****** procedure==>  "+whereami()+" ******")
    global verno, Level
    verno=line[82:84]  
#    CSR_BCCBSPL_tbl['HOLD_BILL']=line[104:105]   
    writelog("** BOS VERSION NUMBER IS "+verno+" ** ")
    writelog("CASE HEADREC HOLD_BILL = "+line[104:105] )
    writelog("**--------------------------**")
    
    
    #Reset Variables at this level?????
    Level=' '    
    
    
    
def process_ROOTREC_TYP0505(): 
    debug("****** procedure==>  "+whereami()+" ******")
    "050500"
    
    global CSR_BCCBSPL_tbl  
    global root_rec
    global record_id
  
    initialize_BCCBSPL_tbl()
    "record_id doesn't need populated"
  
    CSR_BCCBSPL_tbl['TNPA']=line[19:21] 
    CSR_BCCBSPL_tbl['FGRP']=line[25:26]  
    CSR_BCCBSPL_tbl['BILL_DATE']=line[159:167]
    CSR_BCCBSPL_tbl['ICSC_OFC']=line[177:181]
    CSR_BCCBSPL_tbl['NLATA']=line[182:185]
    CSR_BCCBSPL_tbl['BAN_TAR']=line[190:194]
    CSR_BCCBSPL_tbl['TAX_EXMPT']=line[196:205]
    CSR_BCCBSPL_tbl['UNB_TAR_IND']=line[207:208]
    CSR_BCCBSPL_tbl['INPUT_RECORDS']=str(record_id)
    
    if CSR_BCCBSPL_tbl['FGRP'] in ('S','W'):
        CSR_BCCBSPL_tbl['GRPLOCK']='N'
    else:
        CSR_BCCBSPL_tbl['GRPLOCK']='Y'

    "we already know ACNA, EOB_DATE, and BAN are the same"   

    process_insert_table("CAIMS_CSR_BCCBSPL", CSR_BCCBSPL_tbl, CSR_BCCBSPL_DEFN_DICT)
    root_rec=True
         
    "no flag to set - part of root"

def process_ROOTREC_CHEKFID():
    debug("****** procedure==>  "+whereami()+" ******")
    "051000,051100"
    
    global CSR_BCCBSPL_tbl  
    global record_id
    
    if line[189:193] == 'CCNA':
        #do UPCCNA
         #CCNA = EDIT(BILLFID_DATA,'999$'); get first 3 chars
        CSR_BCCBSPL_tbl['CCNA']=line[183:186]
        CSR_BCCBSPL_tbl['INPUT_RECORDS']+="*"+str(record_id)
    elif line[189:192] == 'MCN':
        #do this
        #MCN = EDIT(BILLFID_DATA,'9999999999999999999$');--get first 19 chars
        CSR_BCCBSPL_tbl['MCN']=line[183:202]
        CSR_BCCBSPL_tbl['INPUT_RECORDS']+="*"+str(record_id)
    else:
        #continue on, skip
        pass

    if root_rec == True:
        if CSR_BCCBSPL_tbl['MCN'].rstrip(' ') == '' and CSR_BCCBSPL_tbl['CCNA'].rstrip(' ') == '':
            pass   #values are blank nothing to update
        else:
            process_update_table("CAIMS_CSR_BCCBSPL", CSR_BCCBSPL_tbl, CSR_BCCBSPL_DEFN_DICT)
    else:
        process_ERROR_END("ERROR: No root record for CHEKFID record "+str(record_id)+".  Not updating MCN or CCNA.")
        
 

def process_BILLREC_BILLTO():   
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_BILLREC_tbl,record_id
    "100500"
#    -************************************************************
#    -*   PROCESS DATA FOR THE BILLREC SEGMENT OF DATABASE
#    -*    USE '401005' RECORDS FOR BOS VER 22.
#    -************************************************************

    initialize_BILLREC_tbl()    
    
    CSR_BILLREC_tbl['BILLNAME']=line[65:97]
    CSR_BILLREC_tbl['BADDR1']=line[97:129]
    CSR_BILLREC_tbl['BADDR2']=line[129:161]
    CSR_BILLREC_tbl['BADDR3']=line[161:193]
    CSR_BILLREC_tbl['BADDR4']=line[193:225]
    CSR_BILLREC_tbl['INPUT_RECORDS']=str(record_id)
    
    process_insert_table("CAIMS_CSR_BILLREC", CSR_BILLREC_tbl, CSR_BILLREC_DEFN_DICT)

def process_ACTLREC_BILLTO(): 
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_ACTLREC_tbl, record_id
    "ACTLREC  -  101000"    
    
    initialize_ACTLREC_tbl()
    
    CSR_ACTLREC_tbl['ACTL_NUM']=line[61:65]
    
    CSR_ACTLREC_tbl['ACTL']=line[65:76]
    CSR_ACTLREC_tbl['ACTLADDR1']=line[76:106]
    CSR_ACTLREC_tbl['ACTLADDR2']=line[106:136]
    CSR_ACTLREC_tbl['ACTLADDR3']=line[136:166]
    CSR_ACTLREC_tbl['CUST_NAME']=line[167:197]
    CSR_ACTLREC_tbl['FGRP']=line[217:218]
    CSR_ACTLREC_tbl['INPUT_RECORDS']=str(record_id)
    
    process_insert_table("CAIMS_CSR_ACTLREC", CSR_ACTLREC_tbl, CSR_ACTLREC_DEFN_DICT)
    
def process_ROOTREC_UPROOT():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_BCCBSPL_tbl, record_id
    "ROOTREC  -  101500"    

    if root_rec==True:
        if line[61:62].rstrip('') != '':
            CSR_BCCBSPL_tbl['TAPE']=line[61:62]
            CSR_BCCBSPL_tbl['INPUT_RECORDS']+="*"+str(record_id)
            process_update_table("CAIMS_CSR_BCCBSPL", CSR_BCCBSPL_tbl, CSR_BCCBSPL_DEFN_DICT)
        else:
            writelog("No TAPE value on input data for ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'])
 
    else:
        process_ERROR_END("ERROR: Encountered UPROOT record (record id "+record_id+") but no root record has been created.")



def process_TYP1505_CKT_LOC():
    debug("****** procedure==>  "+whereami()+" ******")    
    global CSR_CKT_tbl,CSR_LOC_tbl
    global Level, record_id, USOCCNT
    "150500"
    
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
            initialize_CKT_tbl()
##FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
#FIXFORM CDINSTCC/8 X3 FID/A4 X1 PSM/16 X-16 CIRCUIT/A53 X67
#FIXFORM X1 CKT_LTP/A4 X1 CKT_MAN_BAN_UNITS/Z7.4 X6
#FIXFORM CDACTCC/8 CACT/A1
            Level = 'C'
            CSR_CKT_tbl['CDINSTCC']=line[61:69]
            CSR_CKT_tbl['FID']=line[72:76]
            CSR_CKT_tbl['PSM']=line[77:93]
            CSR_CKT_tbl['CIRCUIT']=line[77:130]
            CSR_CKT_tbl['CKT_LTP']=line[198:202]
            CSR_CKT_tbl['CKT_MAN_BAN_UNITS']=line[203:210]
            CSR_CKT_tbl['CDACTCC']=line[216:224]
            CSR_CKT_tbl['CACT']=line[224:225]
            CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
                
            process_insert_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)

            
        elif tfid in  ('CKL','CKLT'):
            #build LOCSEG setment
            initialize_LOC_tbl()
            Level = 'L'
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
#FIXFORM LDINSTCC/8 X3 CKLFID/A4 X1 LOC_DATA/A60 X60
#FIXFORM X1 LOC_LTP/A4 X1 LOC_MAN_BAN_UNITS/Z7.4 X6
#FIXFORM LDACTCC/8 LACT/A1
#COMPUTE
#USOCCNT/I2=0;
#below:It would ignore the first two characters and get the 3rd and 4th.
#TLOC = EDIT(LOC_DATA,'$$99$');

            USOCCNT=0
      
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
            
            process_insert_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)            
        else:
            pass #SKIP record
            
       
def process_FIDDRVR():
    debug("****** procedure==>  "+whereami()+" ******")    
    "150600, 151001 records"
#    -************************************************************
#    -* PROCESS '401506' AND '401511' RECORDS. THESE ARE FLOATING FIDS
#    -* ONLY.  LEFT-HANDED FIDS ARE PROCESSED IN THE TYP1505 CASE
#    -************************************************************
    global CSR_CUFID_tbl, Level, record_id, tfid
    
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
        initialize_CUFID_tbl
        
        CSR_CUFID_tbl['CUFID']=line[72:76]
        CSR_CUFID_tbl['CUFID_DATA']=line[77:113]
        CSR_CUFID_tbl['FGRP']=line[198:199]
        CSR_CUFID_tbl['INPUT_RECORDS']=str(record_id)
        
        process_insert_table("CAIMS_CSR_CUFID", CSR_CUFID_tbl, CSR_CUFID_DEFN_DICT)
        
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
               
        UFID=line[72:76]
        if UFID == 'ASG':
            pass #go to top
        else:
            initialize_UFID_tbl()            
            
            CSR_UFID_tbl['UFID']=line[72:76]
            CSR_UFID_tbl['UFID_DATA']=line[77:113]
            CSR_UFID_tbl['FGRP']=line[198:199]
            CSR_UFID_tbl['INPUT_RECORDS']=str(record_id)
            
            process_insert_table("CAIMS_CSR_UFID", CSR_UFID_tbl, CSR_UFID_DEFN_DICT)   
    else:
        pass #go to top

def process_LOCFIDRVR():
    debug("****** procedure==>  "+whereami()+" ******")    
    global CSR_LOC_tbl, tfid, Level, record_id
   

    #initialize_LOC_tbl()    
    #No initialize... These should all be updates.
 
    if tfid == 'LSO ':                      
#         GOTO LOCLSOUP (UPDATE THE LSO FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LSO/7
        CSR_LOC_tbl['LSO']=line[77:84]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'FSO ':  
        
#         GOTO LOCFSOUP (UPDATE THE FSO FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 FSO/7
        CSR_LOC_tbl['FSO']=line[77:84]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'NCI ': 
#         GOTO LOCNCIUP (UPDATE THE NCI FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 NCI/13
        CSR_LOC_tbl['NCI']=line[77:90]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'NC  ': 
#         GOTO LOCLNCUP (UPDATE THE LNCODE FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LNCODE/4
        CSR_LOC_tbl['LNCODE']=line[77:81]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'ICO ': 
#        GOTO LOCICOUP (UPDATE THE ICO FIELD IN THE LOC SEGMENT )
#        LOC = LOC;
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 ICO/4
        CSR_LOC_tbl['ICO']=line[77:81]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'SGN ': 
#         GOTO LOCSGNUP (UPDATE THE SGN FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SGN/3
        CSR_LOC_tbl['SGN']=line[77:80]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'TAR ': 
#         GOTO LOCTARUP  (UPDATE THE LTAR FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LTAR/4
        CSR_LOC_tbl['LTAR']=line[77:81]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'RTAR': 
#         GOTO LOCRTARUP (UPDATE THE LRTAR FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LRTAR/4
        CSR_LOC_tbl['LRTAR']=line[77:81] 
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'DES ': 
#         GOTO LOCLDESUP (UPDATE THE LDES  FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LDES/36
        CSR_LOC_tbl['LDES']=line[77:113] 
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'HSO ': 
#         GOTO LOCHSOUP  (UPDATE THE HSO FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 HSO/7
        CSR_LOC_tbl['HSO']=line[77:84]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'CFA ': 
#         GOTO LOCCFAUP  (UPDATE THE CFA FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 CFA/40
        CSR_LOC_tbl['CFA']=line[77:117]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'XR  ': 
#         GOTO LOCXRUP   (UPDATE THE XR  FIELD IN THE LOC SEGMENT )
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 XR/4
        CSR_LOC_tbl['XR']=line[77:81]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    elif tfid == 'SN  ': 
#         GOTO LOCSNUP (UPDATE THE SN  FIELD IN THE LOC SEGMENT ) 
#         LOC = LOC;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SN/30
        CSR_LOC_tbl['SN']=line[77:107]
        CSR_LOC_tbl['INPUT_RECORDS']=str(record_id)
        process_update_table("CAIMS_CSR_LOC", CSR_LOC_tbl, CSR_LOC_DEFN_DICT)
    else:  
        process_LOCFID()               
 

def process_LOCFID():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_LFID_tbl, record_id
#     -* THIS CASE READS THE  FLOATED FID AND LOADS THE CORRESPONDING
#     -* FIELD IN THE LFIDSEG SEGMENT OF THE DATABASE.
#     -************************************************************ 
#     FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X42
#     FIXFORM LOCFID/A4 X1 LF_DATA/A36 X85
#     FIXFORM FGRP/1 X26  
    
    initialize_LFID_tbl()
    
    CSR_LFID_tbl['LOCFID']=line[72:76]
    CSR_LFID_tbl['LOCFID_DATA']=line[77:113]
    CSR_LFID_tbl['FGRP']=line[198:199]
    CSR_LFID_tbl['INPUT_RECORDS']=str(record_id)
    process_insert_table("CAIMS_CSR_LFID", CSR_LFID_tbl, CSR_LFID_DEFN_DICT)
 
def process_COSFID():   
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_COSFID_tbl
    global tfid, Level,record_id
    " "
#-************************************************************
#-* PROCESS '401511' RECORDS FROM CASE FIDDRVR WHEN Level EQUALS 'CO'.
#-* THIS CASE READS THE FLOATED FID AND LOADS THE COSFID & COSFID_DATA
#-* FIELD IN COSFID   SEGMENT OF THE DATABASE.
#-************************************************************
    initialize_COSFID_tbl()    
            #GOTO COSFID sEGMENT
#                FIXFORM X-225 X5 X6 X13 X42
#                FIXFORM COSFID/A4 X1 COSFID_DATA/36 X85
#                FIXFORM X1 X26
    CSR_COSFID_tbl['COSFID']=line[72:76]
    CSR_COSFID_tbl['COSFID_DATA']=line[77:113]
    CSR_COSFID_tbl['INPUT_RECORDS']=str(record_id)
    process_insert_table("CAIMS_CSR_COSFID", CSR_COSFID_tbl, CSR_COSFID_DEFN_DICT)

def process_CKTFIDRVR():
    debug("****** procedure==>  "+whereami()+" ******")  
    global CSR_CKT_tbl, Level, tfid, record_id,CSR_CFID_tbl
    
    if tfid == 'NC  ': 
#         GOTO CKTNCUP (LOADS THE NCODE FIELD IN THE CKTSEG OF THE DATABASE)
          #CIRCUIT = CIRCUIT;?????
         #FIXFORM X-225 X5 X6 X13 X42 X4 X1 NCODE/4
         #MATCH CIRCUIT? update ncode
         CSR_CKT_tbl['NCODE']=line[77:81]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'PIU ': 
#        GOTO CKTPIUUP (LOADS THE PIU FIELD IN THE CKTSEG OF THE DATABASE)
#        CIRCUIT = CIRCUIT;
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 PIU/3
         #MATCH CIRCUIT? update piu
         CSR_CKT_tbl['PIU']=line[77:80]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'ASG ': 
#         GOTO CKTASGUP (LOADS THE ASG FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 ASG/6
          #MATCH CIRCUIT? update asg
         CSR_CKT_tbl['ASG']=line[77:83]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'LCC ': 
#         GOTO CKTLCCUP (LOADS THE LCC FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LCC/3
#         #MATCH CIRCUIT? update lcc
         CSR_CKT_tbl['LCC']=line[77:80]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'BAND': 
#         GOTO CKTBNDUP (LOADS THE BAND FIELD IN THECKTSEG OF THE DATABASE)
#        CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 BAND/1
         #MATCH CIRCUIT? update band
         CSR_CKT_tbl['BAND']=line[77:78]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'TAR ': 
#        GOTO CKTTARUP (LOADS THE TAR FIELD IN THE CKTSEG OF THE DATABASE)
#        CIRCUIT = CIRCUIT;
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 TAR/4
         #MATCH CIRCUIT? update TAR
         CSR_CKT_tbl['TAR']=line[77:81]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'RTAR': 
#         GOTO CKTRTARUP (CRTAR  FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 RTAR/4
         #MATCH CIRCUIT? update RTAR
         CSR_CKT_tbl['RTAR']=line[77:81]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'LLN ': 
#         GOTO CKTLLNUP (LOADS THE LLN FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 LLN/12
         #MATCH CIRCUIT? update LLN
         CSR_CKT_tbl['LLN']=line[77:89]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'TLI ': 
#         GOTO CKTTLIUP (LOADS THE TLI FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 TLI/12
         #MATCH CIRCUIT? update tli
         CSR_CKT_tbl['TLI']=line[77:89]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'PICX': 
#         GOTO CKTPICXUP (LOADS THE PICX FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 PICX/3
         #MATCH CIRCUIT? update picx
         CSR_CKT_tbl['PICX']=line[77:80]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'HML ': 
#         GOTO CKTHMLUP (LOADS THE HML FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 HML/40
          #MATCH CIRCUIT? update hml
         CSR_CKT_tbl['HML']=line[77:117]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'HTG ': 
#         GOTO CKTHTGUP (LOADS THE HTG FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 HTG/40
          #MATCH CIRCUIT? update htg
         CSR_CKT_tbl['HTG']=line[77:117]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'TN  ': 
#         GOTO CKTTNUP (LOADS THE TN FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 TN/12
          #MATCH CIRCUIT? update tn
         CSR_CKT_tbl['TN']=line[77:89]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'TER ': 
#         GOTO CKTTERUP (LOADS THE TER FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 TER/4
          #MATCH CIRCUIT? update ter
         CSR_CKT_tbl['TER']=line[77:81]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'STN ': 
#         GOTO CKTSTNUP (LOADS THE STN FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 STN/24
          #MATCH CIRCUIT? update stn
         CSR_CKT_tbl['STN']=line[77:91]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'SFN ': 
#         GOTO CKTSFNUP (LOADS THE SFN FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SFN/4
          #MATCH CIRCUIT? update sfn
         CSR_CKT_tbl['SFN']=line[77:81]  
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'SFG ': 
#         GOTO CKTSFGUP (LOADS THE SFG FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#         FIXFORM X-225 X5 X6 X13 X42 X4 X1 SFG/6
          #MATCH CIRCUIT? update sfg
         CSR_CKT_tbl['SFG']=line[77:83] 
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'CKR ': 
#         GOTO CKTCKRUP (LOADS THE CKR FIELD IN THE CKTSEG OF THE DATABASE)
#         CIRCUIT = CIRCUIT;
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 CKR/36
         #MATCH CIRCUIT? update CKR
         CSR_CKT_tbl['CKR']=line[77:113]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'GSZ ': 
#         GOTO CKTGSZUP logic below
#        -************************************************************
#        -* PROCESS '401506' RECORDS FROM CASE CKTFID WHEN CFID CONTAINS 'GSZ'
#        -* THIS CASE READS THE FLOATED FID AND LOADS THE GSZ FIELD IN THE
#        -* CKTSEG OF THE DATABASE
#        -************************************************************
#        CIRCUIT = CIRCUIT;
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 GSZ/3
        #7+5+6+13+42+4+1  =78 
         CSR_CKT_tbl['GSZ']=line[77:80]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'NHN ': 
#         GOTO CKTNHNUP logic below:
#        -************************************************************
#        -* PROCESS '401506' RECORDS FROM CASE CKTFID WHEN CFID CONTAINS 'NHN'
#        -* THIS CASE READS THE FLOATED FID AND LOADS THE NHN FIELD IN THE
#        -* CKTSEG OF THE DATABASE
#        -************************************************************
#        COMPUTE
#        CIRCUIT = CIRCUIT;
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 NHN/12
         CSR_CKT_tbl['NHN']=line[77:89]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'PTN ': 
#         GOTO CKTPTNUP  logic below
#        -************************************************************
#        -* PROCESS '401506' RECORDS FROM CASE CKTFID WHEN CFID CONTAINS 'PTN'
#        -* THIS CASE READS THE FLOATED FID AND LOADS THE PTN FIELD IN THE
#        -* CKTSEG OF THE DATABASE
#        -************************************************************
#        COMPUTE
#        CIRCUIT = CIRCUIT;
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 PTN/12
         CSR_CKT_tbl['PTN']=line[77:89] 
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
    elif tfid == 'SBN ': 
#         GOTO CKTSBNUP  logic below 
#        -************************************************************
#        -* PROCESS '401506' RECORDS FROM CASE CKTFID WHEN CFID CONTAINS 'SBN'
#        -* THIS CASE READS THE FLOATED FID AND LOADS THE SBN FIELD IN THE
#        -* CKTSEG OF THE DATABASE
#        -************************************************************
#        COMPUTE
#        CIRCUIT = CIRCUIT;
#        FIXFORM X-225 X5 X6 X13 X42 X4 X1 SBN/40
         CSR_CKT_tbl['SBN']=line[77:117]
         CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
         process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
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
            initialize_CSR_CFID_tbl()
            CSR_CFID_tbl['CFID']=line[72:76]
            CSR_CFID_tbl['FID_DATA']=line[77:113]
            CSR_CFID_tbl['INPUT_RECORDS']=str(record_id)
            process_update_table("CAIMS_CSR_CFID", CSR_CFID_tbl, CSR_CFID_DEFN_DICT)
#                     INSERT data here                 
 

def process_USOCDRVR_36():
    debug("****** procedure==>  "+whereami()+" ******")  
    "151000"
    global Level, record_id

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
    debug("****** procedure==>  "+whereami()+" ******")  
#    UPDATES THE CKTUSOC SEGMENT
    global CSR_CKTUSOC_tbl,Level, record_id
    global CUACT
    
    
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
        initialize_CSR_CKTUSOC_tbl()
        Level = 'CU'
    #    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
    #    FIXFORM CUDINSTCC/8 CUSOCQTY/Z5 CUSOC/5 X137
    #    FIXFORM CUDACTCC/8 CUACT/1
        
        CSR_CKTUSOC_tbl['CUDINSTCC']=line[61:69]
        CSR_CKTUSOC_tbl['CUSOCQTY']=line[69:74]
        CSR_CKTUSOC_tbl['CUSOC']=line[74:79]
        CSR_CKTUSOC_tbl['CUDACTCC']=line[216:224]
        CUACT=line[224:225]
        CSR_CKTUSOC_tbl['CUACT']=CUACT
        CSR_CKTUSOC_tbl['INPUT_RECORDS']=str(record_id) 
        
        if CUACT == 'D':
            pass #goto top
        else:
            process_insert_table("CAIMS_CSR_CKTUSOC", CSR_CKTUSOC_tbl, CSR_CKTUSOC_DEFN_DICT)          
    
def process_COSUSOC_36():    
    debug("****** procedure==>  "+whereami()+" ******")  
    global CSR_CKT_tbl, Level, record_id
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
        Level = 'CO';
#HOW TO PROCESS CIRCIUT?        CIRCUIT = CIRCUIT;
#        FIXFORM X-225 X5 X6 X13 X31
#        FIXFORM COSDINSTCC/8 COSUSOCQTY/Z6 COSRES1/2 COS_USOC/5 X121
#        FIXFORM COSRES2/6 X7 COSDACTCC/8 COSACT/1
#       SEGMENT=CKTSEG  
        
        CSR_CKT_tbl['COSDINSTCC']=line[61:69]
        CSR_CKT_tbl['COSUSOCQTY']=line[69:75]
        CSR_CKT_tbl['COSRES1']=line[75:77]
        CSR_CKT_tbl['COS_USOC']=line[77:82]
        CSR_CKT_tbl['COSRES2']=line[203:209]
        CSR_CKT_tbl['COSDACTCC']=line[216:224]
        CSR_CKT_tbl['COSACT']=line[224:225]
        CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
        
        if CSR_CKT_tbl['COSACT'] == 'D':
            pass #GOTO TOP
        else:
             process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
#            MATCH CIRCUIT
#              ON MATCH UPDATE COSDINSTCC COSUSOCQTY COSRES1
#              ON MATCH UPDATE COS_USOC COSRES2 COSDACTCC COSACT
# 
def process_LOCUSOC_36():     
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_USOC_tbl,Level,record_id, USOCCNT, UACT
    
    global UCODE,USOCCNT, Level, record_id
 #USOC
    initialize_CSR_USOC_tbl()
    USOCCNT = USOCCNT + 1;
    USOC_CNT = USOCCNT;
    UCODE = '  ';
    Level = 'LU';
 
#    FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
#    FIXFORM UDINSTCC/8 QUSOC/Z6 USOCRES1/2 USOC/5 X121
#    FIXFORM USOCRES2/6 X7 UDACTCC/8 UACT/A1

    CSR_USOC_tbl['UDINSTCC']=line[61:69]
    CSR_USOC_tbl['QUSOC']=line[69:75]
    CSR_USOC_tbl['USOCRES1']=line[75:76]
    CSR_USOC_tbl['USOC']=line[77:82]
    CSR_USOC_tbl['USOC_CNT']=USOC_CNT
    CSR_USOC_tbl['USOCRES2']=line[203:209]
    
    CSR_USOC_tbl['UDACTCC']=line[216:224]?
    
    UACT=line[224:225]
    CSR_USOC_tbl['UACT']=UACT
    CSR_USOC_tbl['INPUT_RECORDS']=str(record_id) 
               
    if UACT == 'D':
        pass #GOTO TOP
    else:       
        process_insert_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT)
            
def process_USOCDRVR_TAX():
    debug("****** procedure==>  "+whereami()+" ******")
    "151600"
    global x_tbl
    global CSR_USOC_tbl,Level,record_id, CSR_CKTUSOC_tbl, CSR_CKT_tbl
    global USOC_CNT, UCODE, CUACT, CUSOCQTY, UACT
    
#        FIXFORM X-225 X5 X6 X13 X31 X11 TUSOC/5 X121 X9
#        FIXFORM X3 X2 X4 X1 X8
    TUSOC=line[72:77]
    if Level in ('L','LU'):
#        GOTO LOCUSOC_TAX; logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES THE 1516 LOCATION Level USOCS FROM CASE CKTUSOC
#        -*   IF Level EQUALS 'L' OR 'LU'
#        -************************************************************
#SEGMENT=USOCSEG        
#        USOC_CNT = USOC_CNT;
#        QUSOC = QUSOC;
        UCODE = '  ';
        Level = 'LU';
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM USOC/5 X121 USO_FEDTAX/1 USO_STTAX/1 USO_CITYTAX/1
#        FIXFORM USO_CNTYTAX/1 USO_STSLSTAX/1 USO_LSLSTAX/1 USO_SURTAX/1
#        FIXFORM USO_FRANTAX/1 USO_OTHERTAX/1 X3 X2 X4 X1 X8
         
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
        CSR_USOC_tbl['INPUT_RECORDS']=str(record_id)
        
        if UACT == 'D':
            pass #GOTO TOP;
        else:
            process_update_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT)
#            update record

   
    elif TUSOC.rstrip(' ') in ('UDP','U3P','U6P'):
#        GOTO CKTUSOC_TAX  logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES CIRCUIT Level USOC RECORDS 401516. AND
#        -*  UPDATES THE CKTUSOC SEGMENT. SET Level TO 'CU'.
#        -*  CHECK TO SEE IF THIS IS A CIRCUIT Level RECORD OR LOCATION Level
#        -************************************************************
        Level = 'CU';
        CUACT = CUACT;
        CUSOCQTY = CUSOCQTY;
#        SEGMENT=CKTUSOC
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM CUSOC/5 X121 CU_FEDTAX/1 CU_STTAX/1 CU_CITYTAX/1
#        FIXFORM CU_CNTYTAX/1 CU_STSLSTAX/1 CU_LSLSTAX/1 CU_SURTAX/1
#        FIXFORM CU_FRANTAX/1 CU_OTHERTAX/1 X3 X2 X4 X1 X8

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
        CSR_CKTUSOC_tbl['INPUT_RECORDS']=str(record_id)
        
        if CUACT == 'D':
            pass #GOTO TOP;
        else:
            process_update_table("CAIMS_CSR_CKTUSOC", CSR_CKTUSOC_tbl, CSR_CKTUSOC_DEFN_DICT)
#            update record
    
    else:
#        GOTO COSUSOC_TAX   logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES CIRCUIT Level USOC RECORDS 401516. AND
#        -*  UPDATES THE CKTUSOC SEGMENT. SET Level TO 'CO'.
#        -*  CHECK TO SEE IF THIS IS A CIRCUIT Level RECORD OR LOCATION Level
#        -************************************************************
#SEGMENT=CKTSEG
        Level = 'CO';
        CIRCUIT = CIRCUIT;
        COSACT = COSACT;
#        FIXFORM X-225 X5 X6 X13 X31 X11
#        FIXFORM COS_USOC/5 X121 COS_FEDTAX/1 COS_STTAX/1 COS_CITYTAX/1
#        FIXFORM COS_CNTYTAX/1 COS_STSLSTAX/1 COS_LSLSTAX/1 COS_SURTAX/1
#        FIXFORM COS_FRANTAX/1 COS_OTHERTAX/1 X3 X2 X4 X1 X8
        
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
        CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)
        
        if COSACT == 'D':
            pass #GOTO TOP
        else:
             process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)        
#            update record


def process_USOCDRVR_PLN():    
    debug("****** procedure==>  "+whereami()+" ******")
    "151700"
    global x_tbl
    global current_abbd_rec_key   
    global CSR_USOC_tbl, Level, record_id, CSR_CKTUSOC_tbl, CSR_CKT_tbl
#    FIXFORM X-225 X5 X6 X13 X31 X11 TUSOC/5 X26 X5 X5 X8 X8 X3
#    FIXFORM X1 X5 X2 X1 X5 X26 X8 X1 X44    
    TUSOC=line[72:77]
    
    if Level in ('L','LU'):
#        GOTO LOCUSOC_PLN;  logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES THE 1517 LOCATION Level USOCS FROM CASE CKTUSOC
#        -*   IF Level EQUALS 'L' OR 'LU'
#        -************************************************************
#  SEGMENT=USOCSEG
    
        USOC_CNT = USOC_CNT;
        QUSOC = QUSOC;
        UCODE = '  ';
        Level = 'LU';
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM USOC/5 PLAN_ID/26 RQRD_PL_QTY/5 VAR_PL_QTY/5
#        FIXFORM TRM_STRT_DAT/8 TRM_END_DAT/8 LNGTH_OF_TRM/3 PL_TYP_INDR/1
#        FIXFORM DSCNT_PCTGE/Z5.2 SYS_CPCTY/2 ADD_DIS_PL_IND/1
#        FIXFORM SPL_OFR_PCTGE/Z5.2 SPL_OFR_IDENT/26 S_OFR_END_DAT/8
#        FIXFORM PL_TAR_TYP_INDR/1 X44
        CSR_USOC_tbl['USOC']=line[72:77]
        CSR_USOC_tbl['PLAN_ID']=line[77:103]
        CSR_USOC_tbl['RQRD_PL_QTY']=line[103:108]
        CSR_USOC_tbl['VAR_PL_QTY']=line[108:113]
        CSR_USOC_tbl['TRM_STRT_DAT']=line[113:121]
        CSR_USOC_tbl['TRM_END_DAT']=line[121:129]
        CSR_USOC_tbl['LNGTH_OF_TRM']=line[129:132]
        CSR_USOC_tbl['PL_TYP_INDR']=line[132:133]
        CSR_USOC_tbl['DSCNT_PCTGE']=line[133:138]
        CSR_USOC_tbl['SYS_CPCTY']=line[138:140]
        CSR_USOC_tbl['ADD_DIS_PL_IND']=line[140:141]
        CSR_USOC_tbl['SPL_OFR_PCTGE']=line[141:146]
        CSR_USOC_tbl['SPL_OFR_IDENT']=line[146:172]
        CSR_USOC_tbl['S_OFR_END_DAT']=line[172:180]
        CSR_USOC_tbl['PL_TAR_TYP_INDR']=line[180:181]
        CSR_USOC_tbl['INPUT_RECORDS']=str(record_id)
        
        if UACT == 'D':
            pass #GOTO TOP;
        else:
            process_update_table("CAIMS_CSR_USOC", CSR_USOC_tbl, CSR_USOC_DEFN_DICT)
#            update record

    elif TUSOC.rstrip(' ') in ('UDP', 'U3P','U6P'):
#        GOTO CKTUSOC_PLN  logic below
#        -************************************************************
#        -*  THIS CASE PROCESSES CIRCUIT Level USOC RECORDS 401517. AND
#        -*  UPDATES THE CKTUSOC SEGMENT. SET Level TO 'CU'.
#        -*  CHECK TO SEE IF THIS IS A CIRCUIT Level RECORD OR LOCATION Level
#        -************************************************************
#SEGMENT=CKTUSOC
        Level = 'CU';
        CUACT = CUACT;
        CUSOCQTY = CUSOCQTY;
#        FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31 X11
#        FIXFORM CUSOC/5 CU_PLAN_ID/26 CU_RQRD_PL_QTY/5 CU_VAR_PL_QTY/5
#        FIXFORM CU_TRM_STRT_DAT/8 CU_TRM_END_DAT/8 CU_LNGTH_OF_TRM/3
#        FIXFORM CU_PL_TYP_INDR/1 CU_DSCNT_PCTGE/Z5.2 CU_SYS_CPCTY/2
#        FIXFORM CU_ADD_DIS_PL_IND/1 CU_SPL_OFR_PCTGE/Z5.2 CU_SPL_OFR_IDENT/26
#        FIXFORM CU_S_OFR_END_DAT/8 CU_PL_TAR_TYP_INDR/1 X44

        CSR_CKTUSOC_tbl['CUSOC']=line[72:77]
        CSR_CKTUSOC_tbl['CU_PLAN_ID']=line[77:103]
        CSR_CKTUSOC_tbl['CU_RQRD_PL_QTY']=line[103:108]
        CSR_CKTUSOC_tbl['CU_VAR_PL_QTY']=line[108:113]
        CSR_CKTUSOC_tbl['CU_TRM_STRT_DAT']=line[113:121]
        CSR_CKTUSOC_tbl['CU_TRM_END_DAT']=line[121:129]
        CSR_CKTUSOC_tbl['CU_LNGTH_OF_TRM']=line[129:132]
        CSR_CKTUSOC_tbl['CU_PL_TYP_INDR']=line[132:133]
        CSR_CKTUSOC_tbl['CU_DSCNT_PCTGE']=line[133:138]
        CSR_CKTUSOC_tbl['CU_SYS_CPCTY']=line[138:140]
        CSR_CKTUSOC_tbl['CU_ADD_DIS_PL_IND']=line[140:141]
        CSR_CKTUSOC_tbl['CU_SPL_OFR_PCTGE']=line[141:146]
        CSR_CKTUSOC_tbl['CU_SPL_OFR_IDENT']=line[146:172]
        CSR_CKTUSOC_tbl['CU_S_OFR_END_DAT']=line[172:180]
        CSR_CKTUSOC_tbl['CU_PL_TAR_TYP_INDR']=line[180:181]
        CSR_CKTUSOC_tbl['INPUT_RECORDS']=str(record_id)
        
        if CUACT == 'D':
            pass  #GOTO TOP
        else:
            process_update_table("CAIMS_CSR_CKTUSOC", CSR_CKTUSOC_tbl, CSR_CKTUSOC_DEFN_DICT)
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
        Level = 'CO';
        CIRCUIT = CIRCUIT;
        COSACT = COSACT;
#        FIXFORM X-225 X5 X6 X13 X31 X11
#        FIXFORM COS_USOC/5 CO_PLAN_ID/26 CO_RQRD_PL_QTY/5 CO_VAR_PL_QTY/5
#        FIXFORM CO_TRM_STRT_DAT/8 CO_TRM_END_DAT/8 CO_LNGTH_OF_TRM/3
#        FIXFORM CO_PL_TYP_INDR/1 CO_DSCNT_PCTGE/Z5.2 CO_SYS_CPCTY/2
#        FIXFORM CO_ADD_DIS_PL_IND/1 CO_SPL_OFR_PCTGE/Z5.2 CO_SPL_OFR_IDENT/26
#        FIXFORM CO_S_OFR_END_DAT/8 CO_PL_TAR_TYP_INDR/1 X44
        
        CSR_CKT_tbl['COS_USOC']=line[72:77]
        CSR_CKT_tbl['CO_PLAN_ID']=line[77:103]
        CSR_CKT_tbl['CO_RQRD_PL_QTY']=line[103:108]
        CSR_CKT_tbl['CO_VAR_PL_QTY']=line[108:113]
        CSR_CKT_tbl['CO_TRM_STRT_DAT']=line[113:121]
        CSR_CKT_tbl['CO_TRM_END_DAT']=line[121:129]
        CSR_CKT_tbl['CO_LNGTH_OF_TRM']=line[129:132]
        CSR_CKT_tbl['CO_PL_TYP_INDR']=line[132:133]
        CSR_CKT_tbl['CO_DSCNT_PCTGE']=line[133:138]
        CSR_CKT_tbl['CO_SYS_CPCTY']=line[138:140]
        CSR_CKT_tbl['CO_ADD_DIS_PL_IND']=line[140:141]
        CSR_CKT_tbl['CO_SPL_OFR_PCTGE']=line[141:146]
        CSR_CKT_tbl['CO_SPL_OFR_IDENT']=line[146:172]
        CSR_CKT_tbl['CO_S_OFR_END_DAT']=line[172:180]
        CSR_CKT_tbl['CO_PL_TAR_TYP_INDR']=line[180:181]
        CSR_CKT_tbl['INPUT_RECORDS']=str(record_id)    
            
            
            
        if COSACT == 'D':
            pass  #GOTO TOP;
        else:
            process_update_table("CAIMS_CSR_CKT", CSR_CKT_tbl, CSR_CKT_DEFN_DICT)
#            update data
 

#      
##
##INITIALIZATION PARAGRAPHS
##INITIALIZATION PARAGRAPHS

## 
def initialize_CKT_tbl():            
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_CKT_tbl, current_abbd_rec_key     
    
    CSR_CKT_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_CKT_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_CKT_tbl['BAN']=current_abbd_rec_key['BAN']
    
    
    CSR_CKT_tbl['CDINSTCC']=''
    CSR_CKT_tbl['FID']=''
    CSR_CKT_tbl['PSM']=''
    CSR_CKT_tbl['CIRCUIT']=''
    CSR_CKT_tbl['CKT_LTP']=''
    CSR_CKT_tbl['CKT_MAN_BAN_UNITS']=''
    CSR_CKT_tbl['CDACTCC']=''
    CSR_CKT_tbl['CACT']=''
    CSR_CKT_tbl['NCODE']=''
    CSR_CKT_tbl['PIU']=''
    CSR_CKT_tbl['ASG']=''
    CSR_CKT_tbl['LCC']=''
    CSR_CKT_tbl['BAND']=''
    CSR_CKT_tbl['TAR']=''
    CSR_CKT_tbl['RTAR']=''
    CSR_CKT_tbl['LLN']=''
    CSR_CKT_tbl['TLI']=''
    CSR_CKT_tbl['PICX']=''
    CSR_CKT_tbl['HML']=''
    CSR_CKT_tbl['HTG']=''
    CSR_CKT_tbl['TN']=''
    CSR_CKT_tbl['TER']=''
    CSR_CKT_tbl['STN']=''
    CSR_CKT_tbl['SFN']=''    
    CSR_CKT_tbl['SFG']='' 
    CSR_CKT_tbl['CKR']=''
    CSR_CKT_tbl['GSZ']=''
    CSR_CKT_tbl['NHN']='' 
    CSR_CKT_tbl['PTN']=''
    CSR_CKT_tbl['SBN']=''
    CSR_CKT_tbl['INPUT_RECORDS']=''
    
    
def initialize_LFID_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_LFID_tbl
    global current_abbd_rec_key 

    CSR_LFID_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_LFID_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_LFID_tbl['BAN']=current_abbd_rec_key['BAN']
    
    CSR_LFID_tbl['LOCFID']=''
    CSR_LFID_tbl['LOCFID_DATA']=''
    CSR_LFID_tbl['FGRP']='' 
    CSR_LFID_tbl['INPUT_RECORDS']=''               
 
def initialize_LOC_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_LOC_tbl
    global current_abbd_rec_key     
    
    CSR_LOC_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_LOC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_LOC_tbl['BAN']=current_abbd_rec_key['BAN']

 
    CSR_LOC_tbl['LDINSTCC']=''
    CSR_LOC_tbl['CKLFID']=''
    CSR_LOC_tbl['LOC_DATA']=''
    CSR_LOC_tbl['TLOC']=''
    CSR_LOC_tbl['LOC']=''
    CSR_LOC_tbl['LOC_LTP']=''
    CSR_LOC_tbl['LOC_MAN_BAN_UNITS']=''
    CSR_LOC_tbl['LDACTCC']=''
    CSR_LOC_tbl['LACT']=''
#    CSR_LOC_tbl['USOCCNT']=''
    CSR_LOC_tbl['LSO']=''
    CSR_LOC_tbl['FSO']=''
    CSR_LOC_tbl['NCI']=''
    CSR_LOC_tbl['LNCODE']=''
    CSR_LOC_tbl['ICO']=''
    CSR_LOC_tbl['SGN']=''
    CSR_LOC_tbl['LTAR']=''
    CSR_LOC_tbl['LRTAR']=''
    CSR_LOC_tbl['LDES']=''
    CSR_LOC_tbl['HSO']=''
    CSR_LOC_tbl['CFA']=''
    CSR_LOC_tbl['SN']=''
    CSR_LOC_tbl['INPUT_RECORDS']=''
       
    
def  initialize_BCCBSPL_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_BCCBSPL_tbl
    global current_abbd_rec_key 
    
    CSR_BCCBSPL_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_BCCBSPL_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_BCCBSPL_tbl['BAN']=current_abbd_rec_key['BAN']

    
    CSR_BCCBSPL_tbl['VERSION_NBR']=''
    CSR_BCCBSPL_tbl['HOLD_BILL']=''
    CSR_BCCBSPL_tbl['FINAL_IND']=''
    CSR_BCCBSPL_tbl['MKT_IND']=''
    CSR_BCCBSPL_tbl['FRMT_IND']=''
    CSR_BCCBSPL_tbl['MAN_BAN_TYPE']='' 
 
    CSR_BCCBSPL_tbl['TNPA']='' 
    CSR_BCCBSPL_tbl['FGRP']='' 
    CSR_BCCBSPL_tbl['BILL_DATE']='' 
    CSR_BCCBSPL_tbl['ICSC_OFC']='' 
    CSR_BCCBSPL_tbl['NLATA']='' 
    CSR_BCCBSPL_tbl['BAN_TAR']='' 
    CSR_BCCBSPL_tbl['TAX_EXMPT']='' 
    CSR_BCCBSPL_tbl['UNB_TAR_IND']='' 
    CSR_BCCBSPL_tbl['GRPLOCK']=''
    CSR_BCCBSPL_tbl['CCNA']=''
    CSR_BCCBSPL_tbl['MCN']=''
    CSR_BCCBSPL_tbl['TAPE']=''
 
 
def initialize_BILLREC_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_BILLREC_tbl
    global current_abbd_rec_key     
    
    CSR_BILLREC_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_BILLREC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_BILLREC_tbl['BAN']=current_abbd_rec_key['BAN']    
    
    
    CSR_BILLREC_tbl['BILLNAME']=''
    CSR_BILLREC_tbl['BADDR1']=''
    CSR_BILLREC_tbl['BADDR2']=''
    CSR_BILLREC_tbl['BADDR3']=''
    CSR_BILLREC_tbl['BADDR4']=''
    CSR_BILLREC_tbl['INPUT_RECORDS']=''
    
    
    
def initialize_ACTLREC_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_ACTLREC_tbl
    global current_abbd_rec_key    

    CSR_ACTLREC_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_ACTLREC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_ACTLREC_tbl['BAN']=current_abbd_rec_key['BAN']

    CSR_ACTLREC_tbl['ACTL_NUM']=''
    CSR_ACTLREC_tbl['ACTL']=''
    CSR_ACTLREC_tbl['ACTLADDR1']=''
    CSR_ACTLREC_tbl['ACTLADDR2']=''
    CSR_ACTLREC_tbl['ACTLADDR3']=''
    CSR_ACTLREC_tbl['CUST_NAME']=''
    CSR_ACTLREC_tbl['FGRP']=''
    CSR_ACTLREC_tbl['INPUT_RECORDS']=''
 
def initialize_CUFID_tbl():  
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_CUFID_tbl
    global current_abbd_rec_key  

    CSR_CUFID_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_CUFID_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_CUFID_tbl['BAN']=current_abbd_rec_key['BAN']
    
    CSR_CUFID_tbl['CUFID']=''
    CSR_CUFID_tbl['CUFID_DATA']=''
    CSR_CUFID_tbl['FGRP']=''
    CSR_CUFID_tbl['INPUT_RECORDS']=''
 
def initialize_COSFID_tbl():  
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_COSFID_tbl
    global current_abbd_rec_key    
    
    CSR_COSFID_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_COSFID_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_COSFID_tbl['BAN']=current_abbd_rec_key['BAN']    
    
    
    CSR_COSFID_tbl['COSFID']=''
    CSR_COSFID_tbl['COSFID_DATA']=''
    CSR_COSFID_tbl['INPUT_RECORDS']=''
 
def initialize_UFID_tbl():        
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_UFID_tbl
    global current_abbd_rec_key  

    CSR_UFID_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_UFID_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_UFID_tbl['BAN']=current_abbd_rec_key['BAN']    
             
    CSR_UFID_tbl['UFID']=''
    CSR_UFID_tbl['UFID_DATA']=''
    CSR_UFID_tbl['FGRP']=''
    CSR_UFID_tbl['INPUT_RECORDS']=''
            
         
def initialize_CSR_USOC_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_USOC_tbl
    global current_abbd_rec_key  

    CSR_USOC_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_USOC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_USOC_tbl['BAN']=current_abbd_rec_key['BAN'] 

    CSR_USOC_tbl['UDINSTCC']=''
    CSR_USOC_tbl['QUSOC']=''
    CSR_USOC_tbl['USOCRES1']=''
    CSR_USOC_tbl['USOC']=''
    CSR_USOC_tbl['USOC_CNT']=''
    CSR_USOC_tbl['USOCRES2']=''
    CSR_USOC_tbl['UDACTCC']=''
    CSR_USOC_tbl['UACT']=''
    
    CSR_USOC_tbl['USO_FEDTAX']=''
    CSR_USOC_tbl['USO_STTAX']=''
    CSR_USOC_tbl['USO_CITYTAX']=''
    CSR_USOC_tbl['USO_CNTYTAX']=''
    CSR_USOC_tbl['USO_STSLSTAX']=''
    CSR_USOC_tbl['USO_LSLSTAX']=''
    CSR_USOC_tbl['USO_SURTAX']=''
    CSR_USOC_tbl['USO_FRANTAX']=''
    CSR_USOC_tbl['USO_OTHERTAX']=''
 
    
    CSR_USOC_tbl['PLAN_ID']=''
    CSR_USOC_tbl['RQRD_PL_QTY']=''
    CSR_USOC_tbl['VAR_PL_QTY']=''
    CSR_USOC_tbl['TRM_STRT_DAT']=''
    CSR_USOC_tbl['TRM_END_DAT']=''
    CSR_USOC_tbl['LNGTH_OF_TRM']=''
    CSR_USOC_tbl['PL_TYP_INDR']=''
    CSR_USOC_tbl['DSCNT_PCTGE']=''
    CSR_USOC_tbl['SYS_CPCTY']=''
    CSR_USOC_tbl['ADD_DIS_PL_IND']=''
    CSR_USOC_tbl['SPL_OFR_PCTGE']=''
    CSR_USOC_tbl['SPL_OFR_IDENT']=''
    CSR_USOC_tbl['S_OFR_END_DAT']=''
    CSR_USOC_tbl['PL_TAR_TYP_INDR']=''      
    CSR_USOC_tbl['INPUT_RECORDS']=''
    

 
    
    
    
    
    
    
        

def initialize_CSR_CKTUSOC_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_CKTUSOC_tbl
    global current_abbd_rec_key  

    CSR_CKTUSOC_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_CKTUSOC_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_CKTUSOC_tbl['BAN']=current_abbd_rec_key['BAN'] 

    CSR_CKTUSOC_tbl['CUDINSTCC']=''
    CSR_CKTUSOC_tbl['CUSOCQTY']==''
    CSR_CKTUSOC_tbl['CUSOC']==''
    CSR_CKTUSOC_tbl['CUDACTCC']==''
    CSR_CKTUSOC_tbl['CUACT']==''
    CSR_CKTUSOC_tbl['INPUT_RECORDS']=''

def initialize_CSR_CFID_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_CFID_tbl
    global current_abbd_rec_key 
 
    CSR_CFID_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_CFID_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_CFID_tbl['BAN']=current_abbd_rec_key['BAN'] 
    
    CSR_CFID_tbl['CFID']=line[72:76]
    CSR_CFID_tbl['FID_DATA']=line[77:113]
    CSR_CFID_tbl['INPUT_RECORDS']=str(record_id)


    
####END INITIALIZE PROCEDURES
    
    
    
    
    
def format_date(datestring):
    debug("****** procedure==>  "+whereami()+" ******")
    dtSize=len(datestring)
    
    if dtSize ==5:
        #jdate conversion
        return "TO_DATE('"+datestring+"','YY-DDD')"
    elif dtSize==6:
        return "TO_DATE('"+datestring[4:6]+"-"+MONTH_DICT[datestring[2:4]]+"-"+"20"+datestring[:2]+"','DD-MON-YY')"  
    elif dtSize==8:
        return "TO_DATE('"+datestring[:4]+"-"+datestring[4:6]+"-"+"20"+datestring[6:2]+"','YYYY-MM-DD')"     
 
 
    
def process_insert_table(tbl_name, tbl_rec,tbl_dic):
    debug("****** procedure==>  "+whereami()+" ******")
    
    firstPart="INSERT INTO "+tbl_name+" (" #add columns
    secondPart=" VALUES ("  #add values
   
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
                    secondPart+=str(convertnumber(value))+","                    
            else:
                print "ERROR:" +tbl_dic[key]
        except KeyError as e:
            writelog("WARNING: Column "+key+" not found in the "+tbl_name+" table.  Skipping.")
            writelog("KeyError:"+e.message)
 
    firstPart=firstPart.rstrip(',')+")"     
    secondPart=secondPart.rstrip(',') +")"      
     
    insCurs=con.cursor()
    insSQL=firstPart+secondPart

    try:
        insCurs.execute(insSQL) 
        con.commit()
        writelog("SUCCESSFUL INSERT INTO "+tbl_name+".")
    except cx_Oracle.DatabaseError , e:
        if ("%s" % e.message).startswith('ORA-00001:'):
            writelog("****** DUPLICATE INSERT INTO "+str(tbl_name)+"*****************")
            writelog("Insert SQL: "+str(insSQL))
        else:
            writelog("ERROR:"+str(e.message))
            writelog("SQL causing problem:"+insSQL)
    finally:
        insCurs.close()
   
   
        
def process_update_table(tbl_name, tbl_rec,tbl_dic):
    debug("****** procedure==>  "+whereami()+" ******")

    "ASSUMPTIONS!!!"
    "THis code assumes that if a value is null that it does not update the ORacle table."
    "Need to make a code change if that rule is not true"
    
    updateSQL="UPDATE "+tbl_name+" SET " #add columns
    whereClause="WHERE "

    for key, value in tbl_rec.items() :
      
        nulVal=False
        if str(value).rstrip(' ') == '':
            nulVal=True
               
        try:
            if key == 'ACNA':
                whereClause+="ACNA='"+str(value).rstrip(' ')+"' AND "
            elif key == 'EOB_DATE':
                whereClause+="EOB_DATE="+format_date(value)+" AND "
            elif key == 'BAN':
                whereClause+="BAN='"+str(value).rstrip(' ')+"' AND "
            elif tbl_dic[key] == 'STRING':
                if str(key) == 'INPUT_RECORDS':
                    updateSQL+="INPUT_RECORDS = INPUT_RECORDS||'*"+str(value).rstrip(' ')+"',"
                elif not nulVal:
                    updateSQL+=str(key)+"='"+str(value).rstrip(' ')+"',"
            elif tbl_dic[key] =='DATETIME':
#                print "date key and value:"+str(key)+" "+str(value)
                if not nulVal:
                    updateSQL+=str(key)+"="+format_date(value)+","                
            elif tbl_dic[key] =='NUMBER':
                if not nulVal:                   
                    updateSQL+=str(key)+"="+str(convertnumber(value))+","        
            else:
                print "ERROR:" +tbl_dic[key]
        except KeyError as e:
            writelog("WARNING: Column "+key+" not found in the "+tbl_name+" table.  Skipping.")
            writelog("KeyError:"+e.message)
                
    whereClause=whereClause.rstrip(' ').rstrip('AND')
    updateSQL=updateSQL.rstrip(',')
        
    updCurs=con.cursor()
    updateSQL+=" "+whereClause
 
    try:
        updCurs.execute(updateSQL) 
#        print "Number of rows updated: " + updCurs.count??????
        con.commit()
        writelog("SUCCESSFUL UPDATE TO  "+tbl_name+".")
        writelog("SUCCESSFUL INSERT INTO "+tbl_name+".")
        
    except cx_Oracle.DatabaseError, e:
        if ("%s" % e.message).startswith('ORA-:'):
            writelog("ERROR:"+str(exc.message))
            writelog("UPDATE SQL Causing problem:"+updateSQL)
    finally:
        updCurs.close()
            


def convertnumber(num) :
    debug("****** procedure==>  "+whereami()+" ******")
    global record_id
    "this procedure assumes 2 decimal places"
#   0000022194F
#   0000000000{
#   00000000000
#    writelog("number in :"+str(num))
    newNum= str(num).lstrip('0')
    
    if newNum == '{' or newNum == '' or newNum.rstrip(' ') == '':
        return "0"
    elif newNum.isdigit():
#        writelog("number out: "+newNum[:len(str(newNum))-2]+"."+newNum[len(str(newNum))-2:len(str(newNum))])
        return newNum[:len(str(newNum))-2]+"."+newNum[len(str(newNum))-2:len(str(newNum))]
#        eg 98765
#        return 987.65
    elif len(str(newNum)) == 1:
        hundredthsPlaceSymbol=DIGIT_DICT[str(newNum)]
        if hundredthsPlaceSymbol in NEGATIVE_NUMS:
            return "-0.0"+hundredthsPlaceSymbol
        else:
            return "0.0"+hundredthsPlaceSymbol
        
    elif str(newNum[:len(newNum)-1]).isdigit():
        leftPart=str(newNum)[:len(str(newNum))-2]+"."
        right2=newNum[len(str(newNum))-2:len(str(newNum))]

        tensPlace=right2[:1]
        hundredthsPlaceSymbol=right2[1:2] 
        #convert last place non numeric digit to a number
        hundredthsPlace= DIGIT_DICT[hundredthsPlaceSymbol]
#        writelog("number out: "+leftPart+tensPlace+hundredthsPlace)
        if hundredthsPlaceSymbol in NEGATIVE_NUMS:
            return "-"+leftPart+tensPlace+hundredthsPlace
        else:
            return leftPart+tensPlace+hundredthsPlace
        #everything numeric except last digit
    else:
        process_ERROR_END("ERROR: Cant Convert Number: "+str(num) +"   from line:"+str(line))



def getTableColumns(tablenm):
    debug("****** procedure==>  "+whereami()+" ******")
             
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
    debug("****** procedure==>  "+whereami()+" ******")
               
    colTypDict=collections.OrderedDict()
        
    myArray=[]
    myArray=getTableColumns(tablenm)
     
    for y in myArray:
        colTypDict[y[0]]=str(y[1]).replace("<type 'cx_Oracle.",'').replace("'>",'') 

    return colTypDict  
         

    
"########################################################################################################################"
"####################################TRANSLATION MODULES#################################################################"

def translate_TACCNT_FGRP(taccnt,tfgrp):
    debug("****** procedure==>  "+whereami()+" ******")    
    
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
    debug("****** procedure==>  "+whereami()+" ******")
    global record_counts
    global unknown_record_counts
    global CSR_KEY_cnt
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
    
    writelog("TOTAL ACNA/EOB_DATE/BAN's processed:"+str(CSR_KEY_cnt))
    writelog(" ")
    writelog("Total input records read from input file:"+str(idCnt+unkCnt))
    writelog(" ")    
 
    
def writelog(msg):
    debug("****** procedure==>  "+whereami()+" ******")
    global csr_BCCBSPL_log
    
    csr_BCCBSPL_log.write("\n"+msg)


def process_ERROR_END(msg):
    writelog("ERROR:"+msg)
    debug("ERROR:"+msg)
    process_close_files()
    raise Exception("ERROR:"+msg)
    
    
def process_close_files():
    debug("****** procedure==>  "+whereami()+" ******")
    global csr_input
    global csr_BCCBSPL_log
    
    if debugOn:
        csr_debug_log.close()
        
    csr_input.close();
    csr_BCCBSPL_log.close()
   
    
    
def endProg(msg):
    debug("****** procedure==>  "+whereami()+" ******")
 
 
    con.commit()
    con.close()
    
    process_write_program_stats()
     
    endTM=datetime.datetime.now()
    print "\nTotal run time:  %s" % (endTM-startTM)
    writelog("\nTotal run time:  %s" % (endTM-startTM))
     

    writelog("\n"+msg)
     
    process_close_files()



"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
"BEGIN Program logic"
"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


print "Starting Program"
init()
main()
endProg("-END OF PROGRAM-")