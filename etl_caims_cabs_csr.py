# -*- coding: utf-8 -*-
"""
Spyder Editor    

This is a temporary script file.
"""
#!/usr/bin/python
#CSR python program
import datetime
import inspect
#import collections
import collections
startTM=datetime.datetime.now();
import cx_Oracle
import sys
import ConfigParser

settings = ConfigParser.ConfigParser();
settings.read('settings.ini')
settings.sections()

"SET ORACLE CONNECTION"
con=cx_Oracle.connect(settings.get('OracleSettings','OraCAIMSUser'),settings.get('OracleSettings','OraCAIMSPw'),settings.get('OracleSettings','OraCAIMSSvc'))
 
    
"CONSTANTS"
#set to true to get debug statements
debugOn=False
if debugOn:
    csr_debug_log = open(settings.get('CSRSettings','CSR_DEBUG'), "w");  


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
    csr_input = open(settings.get('CSRSettings','CSR_CABS_infile'), "r");
    
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
    global line
    global record_counts, unknown_record_counts
    "Counters"

 
    
    global inputLineCnt, CSR_KEY_cnt

    "TABLE Dictionaries - for each segment"
    global CSR_BCCBSPL_tbl,  CSR_BCCBSPL_DEFN_DICT
    global CSR_BILLREC_tbl, CSR_BILLREC_DEFN_DICT
    global CSR_ACTLREC_tbl, CSR_ACTLREC_DEFN_DICT
    global CSR_CKTSEG_tbl, CSR_CKTSEG_DEFN_DICT
    global CSR_LOCSEG_tbl, CSR_LOCSEG_DEFN_DICT
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
    
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
    CSR_BCCBSPL_DEFN_DICT=createTableTypeDict('CSR_BCCBSPL')     
    CSR_BILLREC_tbl=collections.OrderedDict()
    CSR_BILLREC_DEFN_DICT=createTableTypeDict('CSR_BILLREC')
    CSR_ACTLREC_tbl=collections.OrderedDict()
    CSR_ACTLREC_DEFN_DICT=createTableTypeDict('CSR_ACTLREC')
    CSR_CKTSEG_tbl=collections.OrderedDict()
    CSR_CKTSEG_DEFN_DICT=createTableTypeDict('CSR_CKTSEG')    
    CSR_LOCSEG_tbl=collections.OrderedDict()
    CSR_LOCSEG_DEFN_DICT=createTableTypeDict('CSR_LOCSEG')    
 
 
 

 

    "COUNTERS"
    inputLineCnt=0
    CSR_KEY_cnt=0
    status_cnt=0
    "KEY"
    
    prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}
    
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
    global record_id
    global tstx, verno, tsty
#   record_id=line[225:231] 
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
    elif record_id == '150500' and dtst !='D':     
        process_TYP1505_CKT_LOC()      #insert root record


        
    else:  #UNKNOWN RECORDS
        unknownRecord=True
    
    count_record(record_id,unknownRecord)

        
    
    
def process_getkey():
    global badKey
    debug("****** procedure==>  "+whereami()+" ******")
    
    if line[6:11].rstrip(' ') == '' or line[17:30].rstrip(' ') == '' or line[11:17].rstrip(' ') == '':
        badKey=True
    else:
        badKey=False
        
    return { 'ACNA':line[6:11],'EOB_DATE':line[11:17],'BAN':line[17:30]}
     
    
def reset_record_flags():
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
    global verno
    verno=line[82:84]  
#    CSR_BCCBSPL_tbl['HOLD_BILL']=line[104:105]   
    writelog("** BOS VERSION NUMBER IS "+verno+" ** ")
    writelog("CASE HEADREC HOLD_BILL = "+line[104:105] )
    writelog("**--------------------------**")

def process_ROOTREC_TYP0505(): 
    "050500"
    debug("****** procedure==>  "+whereami()+" ******")
    
    global CSR_BCCBSPL_tbl  
    global root_rec
  
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

    process_insert_table("CSR_BCCBSPL", CSR_BCCBSPL_tbl, CSR_BCCBSPL_DEFN_DICT)
    root_rec=True
         
    "no flag to set - part of root"

def process_ROOTREC_CHEKFID():
    debug("****** procedure==>  "+whereami()+" ******")
    "051000,051100"
    
    global CSR_BCCBSPL_tbl  
    
    initialize_BCCBSPL_tbl()
  
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
            process_update_table("CSR_BCCBSPL", CSR_BCCBSPL_tbl, CSR_BCCBSPL_DEFN_DICT)
    else:
        process_ERROR_END("ERROR: No root record for CHEKFID record "+str(record_id)+".  Not updating MCN or CCNA.")
        
 

def process_BILLREC_BILLTO():   
    global CSR_BILLREC_tbl
    "100500"

    initialize_BILLREC_tbl()    
    
    CSR_BILLREC_tbl['BILLNAME']=line[65:97]
    CSR_BILLREC_tbl['BADDR1']=line[97:129]
    CSR_BILLREC_tbl['BADDR2']=line[129:161]
    CSR_BILLREC_tbl['BADDR3']=line[161:193]
    CSR_BILLREC_tbl['BADDR4']=line[193:225]
    CSR_BILLREC_tbl['INPUT_RECORDS']=str(record_id)
    
    process_insert_table("CSR_BILLREC", CSR_BILLREC_tbl, CSR_BILLREC_DEFN_DICT)

def process_ACTLREC_BILLTO(): 
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_ACTLREC_tbl
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
    
    process_insert_table("CSR_ACTLREC", CSR_ACTLREC_tbl, CSR_ACTLREC_DEFN_DICT)
    
def process_ROOTREC_UPROOT():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_BCCBSPL_tbl
    "ROOTREC  -  101500"    

    initialize_BCCBSPL_tbl()
    
    if root_rec==True:
        CSR_BCCBSPL_tbl['TAPE']=line[61:65]
        CSR_BCCBSPL_tbl['INPUT_RECORDS']+="*"+str(record_id)
        process_update_table("CSR_BCCBSPL", CSR_BCCBSPL_tbl, CSR_BCCBSPL_DEFN_DICT)
 
    else:
        process_ERROR_END("ERROR: Encountered UPROOT record (record id "+record_id+") but no root record has been created.")



def process_TYP1505_CKT_LOC():    
    global CSR_CKTSEG_tbl
    global CSR_LOCSEG_tbl
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
        if tfid in ('CLS','CLF','CLT','CLM'):  #val=1
        
            initialize_CSR_CKTSEG_tbl()
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
#FIXFORM CDINSTCC/8 X3 FID/A4 X1 PSM/16 X-16 CIRCUIT/A53 X67
#FIXFORM X1 CKT_LTP/A4 X1 CKT_MAN_BAN_UNITS/Z7.4 X6
#FIXFORM CDACTCC/8 CACT/A1
            CSR_CKTSEG_tbl['CDINSTCC']=line[61:69]
            CSR_CKTSEG_tbl['FID']=line[72:76]
            CSR_CKTSEG_tbl['PSM']=line[77:93]
            CSR_CKTSEG_tbl['CIRCUIT']=line[77:130]
            CSR_CKTSEG_tbl['CKT_LTP']=line[198:202]
            CSR_CKTSEG_tbl['CKT_MAN_BAN_UNITS']=line[203:210]
            CSR_CKTSEG_tbl['CDACTCC']=line[216:224]
            CSR_CKTSEG_tbl['CACT']=line[224:225]
            CSR_CKTSEG_tbl['INPUT_RECORDS']=str(record_id)
                
            process_insert_table("CSR_CKTSEG", CSR_CKTSEG_tbl, CSR_CKTSEG_DEFN_DICT)
            
        elif tfid in ('CKL','CKLT'):
            #LOCSEG stuff
            initialize_CSR_LOCSEG_tbl() 
#FIXFORM X-225 ACNA/A5 EOB_DATE/A6 BAN/A13 X31
#FIXFORM LDINSTCC/8 X3 CKLFID/A4 X1 LOC_DATA/A60 X60
#FIXFORM X1 LOC_LTP/A4 X1 LOC_MAN_BAN_UNITS/Z7.4 X6
#FIXFORM LDACTCC/8 LACT/A1
#COMPUTE
#USOCCNT/I2=0;
#TLOC = EDIT(LOC_DATA,'$$99$');
#LOC = IF TLOC EQ '1-' THEN '1  ' ELSE
#      IF TLOC EQ '2-' THEN '2  ' ELSE
#      IF TLOC EQ '3-' THEN '3  ' ELSE
#      IF TLOC EQ '4-' THEN '4  ' ELSE
#      IF TLOC EQ '5-' THEN '5  ' ELSE
#      IF TLOC EQ '6-' THEN '6  ' ELSE
#      IF TLOC EQ '7-' THEN '7  ' ELSE
#      IF TLOC EQ '8-' THEN '8  ' ELSE
#      IF TLOC EQ '9-' THEN '9  ' ELSE TLOC;
            CSR_LOCSEG_tbl['LDINSTCC']=line[61:69]
            CSR_LOCSEG_tbl['CKLFID']=line[72:76]
            CSR_LOCSEG_tbl['LOC_DATA']=line[77:137]
            CSR_LOCSEG_tbl['LOC_LTP']=line[198:202]
            CSR_LOCSEG_tbl['LOC_MAN_BAN_UNITS']=line[203:210]
            CSR_LOCSEG_tbl['LDACTCC']=line[216:224]
            CSR_LOCSEG_tbl['LACT']=line[224:225]
            CSR_LOCSEG_tbl['TLOC']=line[79:81]
            if CSR_LOCSEG_tbl['TLOC'] == '1-':
                CSR_LOCSEG_tbl['LOC'] ='1'
            elif CSR_LOCSEG_tbl['TLOC'] == '2-':
                CSR_LOCSEG_tbl['LOC'] ='2'
            elif CSR_LOCSEG_tbl['TLOC'] == '3-':
                CSR_LOCSEG_tbl['LOC'] ='3'
            elif CSR_LOCSEG_tbl['TLOC'] == '4-':
                CSR_LOCSEG_tbl['LOC'] ='4'
            elif CSR_LOCSEG_tbl['TLOC'] == '5-':
                CSR_LOCSEG_tbl['LOC'] ='5'
            elif CSR_LOCSEG_tbl['TLOC'] == '6-':
                CSR_LOCSEG_tbl['LOC'] ='6'
            elif CSR_LOCSEG_tbl['TLOC'] == '7-':
                CSR_LOCSEG_tbl['LOC'] ='7'
            elif CSR_LOCSEG_tbl['TLOC'] == '8-':
                CSR_LOCSEG_tbl['LOC'] ='8'
            elif CSR_LOCSEG_tbl['TLOC'] == '9-':
                CSR_LOCSEG_tbl['LOC'] ='9'
            else:
                CSR_LOCSEG_tbl['LOC'] =CSR_LOCSEG_tbl['TLOC'] 
                
            CSR_LOCSEG_tbl['INPUT_RECORDS']=str(record_id)
            
            process_insert_table("CSR_LOCSEG", CSR_LOCSEG_tbl, CSR_LOCSEG_DEFN_DICT)
      
#
#INITIALIZATION PARAGRAPHS
#INITIALIZATION PARAGRAPHS
#INITIALIZATION PARAGRAPHS
#INITIALIZATION PARAGRAPHS
#INITIALIZATION PARAGRAPHS
#INITIALIZATION PARAGRAPHS
#  
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
    CSR_ACTLREC_tbl['ACTL=line']=''
    CSR_ACTLREC_tbl['ACTLADDR1']=''
    CSR_ACTLREC_tbl['ACTLADDR2']=''
    CSR_ACTLREC_tbl['ACTLADDR3']=''
    CSR_ACTLREC_tbl['CUST_NAME']=''
    CSR_ACTLREC_tbl['FGRP']=''
    CSR_ACTLREC_tbl['INPUT_RECORDS']=''


def initialize_CSR_CKTSEG_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_CKTSEG_tbl
    global current_abbd_rec_key 
    
    CSR_CKTSEG_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_CKTSEG_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_CKTSEG_tbl['BAN']=current_abbd_rec_key['BAN']   
    
    CSR_CKTSEG_tbl['CDINSTCC']=''
    CSR_CKTSEG_tbl['FID']=''
    CSR_CKTSEG_tbl['PSM']=''
    CSR_CKTSEG_tbl['CIRCUIT']=''
    CSR_CKTSEG_tbl['CKT_LTP']=''
    CSR_CKTSEG_tbl['CKT_MAN_BAN_UNITS']=''
    CSR_CKTSEG_tbl['CDACTCC']=''
    CSR_CKTSEG_tbl['CACT']=''
    CSR_CKTSEG_tbl['INPUT_RECORDS']=''
 
    
def initialize_CSR_LOCSEG_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global CSR_LOCSEG_tbl
    global current_abbd_rec_key    
    
    CSR_LOCSEG_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    CSR_LOCSEG_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    CSR_LOCSEG_tbl['BAN']=current_abbd_rec_key['BAN']
        
    CSR_LOCSEG_tbl['LDINSTCC']=''
    CSR_LOCSEG_tbl['CKLFID']=''
    CSR_LOCSEG_tbl['LOC_DATA']=''
    CSR_LOCSEG_tbl['LOC_LTP']=''
    CSR_LOCSEG_tbl['LOC_MAN_BAN_UNITS']=''
    CSR_LOCSEG_tbl['LDACTCC']=''
    CSR_LOCSEG_tbl['LACT']=''
    CSR_LOCSEG_tbl['TLOC']=''
    CSR_LOCSEG_tbl['LOC']=''
    CSR_LOCSEG_tbl['TLOC']=''
    CSR_LOCSEG_tbl['INPUT_RECORDS']=''
    
 


    
def format_date(datestring):
    debug("****** procedure==>  "+whereami()+" ******")
    dtSize=len(datestring)
    
    if dtSize ==5:
        #jdate conversion
        return "TO_DATE('"+datestring+"','YY-DDD')"
    elif dtSize==6:
        return "TO_DATE('"+datestring[5:7]+"-"+MONTH_DICT[datestring[2:4]]+"-"+"20"+datestring[:2]+"','DD-MON-YY')"  
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
            writelog("****** DUPLICATE INSERT INTO"+str(tbl_name)+"*****************")
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