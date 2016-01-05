# -*- coding: utf-8 -*-
"""
Spyder Editor 

This is a temporary script file
"""
#!/usr/bin/python
#BDT python program
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
    bdt_debug_log = open(settings.get('BDTSettings','BDT_DEBUG'), "w");  


root_rec=False
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
BILL_REC='10' 
STAR_LINE='*********************************************************************************************'  
MONTH_DICT={'01':'JAN','02':'FEB','03':'MAR','04':'APR','05':'MAY','06':'JUN','07':'JUL','08':'AUG','09':'SEP','10':'OCT','11':'NOV','12':'DEC',}
#http://www.3480-3590-data-conversion.com/article-signed-fields.html
DIGIT_DICT={'{':'0','A':'1','B':'2','C':'3','D':'4','E':'5','F':'6','G':'7','H':'8','I':'9',\
            '}':'0','J':'1','K':'2','L':'3','M':'4','N':'5','O':'6','P':'7','Q':'8','R':'9'} 
NEGATIVE_NUMS=['}','J','K','L','M','N','O','P','Q','R']

   
"TRANSLATERS"

def debug(msg):
    if debugOn:
        bdt_debug_log.write("\n"+str(msg))

def whereami():
    return inspect.stack()[1][3]         
    
def init():
    debug("****** procedure==>  "+whereami()+" ******")
    
    global bdt_input 
    global bdt_BCCBBIL_log
    global record_id
  
    "OPEN FILES"
    "   CABS INPUT FILE"
    bdt_input = open(settings.get('BDTSettings','BDT_CABS_infile'), "r");
    
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
    log_file=str(settings.get('BDTSettings','BDT_LOG_DIR'))
    if log_file.endswith("/"):
        pass
    else:
        log_file+="/"
        
    log_file = str(settings.get('BDTSettings','BDT_LOG_DIR'))+"BDT_Cycle"+str(cycl_yy)+str(cycl_mmdd)+str(cycl_time)+"_"+startTM.strftime("%Y%m%d_@%H%M") +".txt"
         
    bdt_BCCBBIL_log = open(log_file, "w");
    bdt_BCCBBIL_log.write("-BDT CAIMS ETL PROCESS-")
    
    if record_id not in settings.get('BDTSettings','BDTHDR'):
        process_ERROR_END("The first record in the input file was not a "+settings.get('BDTSettings','BDTHDR').rstrip(' ')+" record.")

    writelog("Process "+sys.argv[0])
    writelog("   started execution at: " + str(startTM))
    writelog(STAR_LINE)
    writelog(" ")
    writelog("Input file: "+str(bdt_input))
    writelog("Log file: "+str(bdt_BCCBBIL_log))
    
    "Write header record informatio only"
    writelog("Header Record Info:")
    writelog("     Record ID: "+record_id+" YY: "+cycl_yy+", MMDD: "+cycl_mmdd+", TIME: "+str(cycl_time))
    
    count_record(record_id,False)
    del headerLine,cycl_yy,cycl_mmdd,cycl_time
    
def main():
    debug("****** procedure==>  "+whereami()+" ******")
    #BDT_config.initialize_BDT() 
    global record_type
    global line
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
    global bdt_input, bdt_BCCBBIL_log
    
      
    global BDT_column_names
 
    
    global firstBillingRec
    firstBillingRec=True
    
    global record_id
    global prev_record_id
    global current_abbd_rec_key 
    global prev_abbd_rec_key   
    global goodKey      
    
#DECLARE TABLE ARRAYS AND DICTIONARIES
    BDT_BCCBBIL_tbl=collections.OrderedDict() 
    BDT_BCCBBIL_DEFN_DICT=createTableTypeDict('BDT_BCCBBIL')
    BDT_BALDTL_tbl=collections.OrderedDict()
    BDT_BALDTL_DEFN_DICT=createTableTypeDict('BDT_BALDTL')
    BDT_CRNT1_tbl=collections.OrderedDict()
    BDT_CRNT1_DEFN_DICT=createTableTypeDict('BDT_CRNT1')  
    BDT_CRNT2_tbl=collections.OrderedDict()
    BDT_CRNT2_DEFN_DICT=createTableTypeDict('BDT_CRNT2')
    BDT_SWSPLCHG_tbl=collections.OrderedDict()
    BDT_SWSPLCHG_DEFN_DICT=createTableTypeDict('BDT_SWSPLCHG')       
    BDT_PMNTADJ_tbl=collections.OrderedDict()
    BDT_PMNTADJ_DEFN_DICT=createTableTypeDict('BDT_PMNTADJ')        
    BDT_ADJMTDTL_tbl=collections.OrderedDict()
    BDT_ADJMTDTL_DEFN_DICT=createTableTypeDict('BDT_ADJMTDTL')         
         

    "COUNTERS"
    inputLineCnt=0
    BDT_KEY_cnt=0
    status_cnt=0
    "KEY"
    
    prev_abbd_rec_key={'ACNA':'XXX','EOB_DATE':'990101','BAN':'000'}
    
    "LOOP THROUGH INPUT CABS TEXT FILE"
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
                writelog("WARNING: BAD INPUT DATA.  ACNA="+current_abbd_rec_key['ACNA']+", BAN="+current_abbd_rec_key['BAN']+", EOB_DATE="+current_abbd_rec_key['EOB_DATE'])
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
            writelog("ERROR: Not sure what type of record this is:")
            writelog(line)
         
         
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
#    writelog("Log file: "+str(bdt_BCCBBIL_log))
#    writelog("Header Record Info:")
#    writelog("     Record ID: "+record_id+" YY: "+system_yy+", MMDD: "+system_mmdd+", TIME: "+str(system_time))
#    
#    
#    count_record(record_id,False)
 



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



def process_bill_records():
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_BCCBBIL_tbl
    global record_id
    global firstBillingRec
    global tstx, verno, tsty
    
    tstx=line[77:79]
    verno=line[82:84]
    tstz=line[161:163]
    
    if firstBillingRec:
        "FIRST RECORD ONLY"
        #write BOS version number to log
        writelog("**--------------------------**")
        writelog("** BOS VERSION NUMBER IS "+verno+" ** ")
        writelog("**--------------------------**")
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
        
    elif record_id in ('051400','051500','051600','055200','055300','055400'):  
        process_TYP0514_SWSPLCHG()
        
    elif record_id == '150500':
        process_TYP1505_PMNTADJ()
    
    elif record_id == '200500':
        process_TYP2005_ADJMTDTL()
    elif record_id == '200501':
        process_TYP20051_ADJMTDTL()
        
    elif record_id =='250500':
        process_TYP2505_BALDTL()
        
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
   
    
def process_TYP0101_ROOT():
    debug("****** procedure==>  "+whereami()+" ******")
    global firstBillingRec
    global verno
    global BDT_BCCBBIL_tbl,  BDT_BCCBBIL_DEFN_DICT  
    global currentRecordHasPrevBillMatch
    global root_rec
   
    
    
    
    initialize_BCCBBIL_tbl()
    "record_id doesn't need populated"
    
    "we already know ACNA, EOB_DATE, and BAN are the same"   
    BDT_BCCBBIL_tbl['JDATE']=line[71:76]
    BDT_BCCBBIL_tbl['BANLOCK']='N'    #defaulted db value
    BDT_BCCBBIL_tbl['VERSION_NBR']=line[82:84]

    BDT_BCCBBIL_tbl['TLF']=line[97:98]
    BDT_BCCBBIL_tbl['NLATA']=line[98:101]
    BDT_BCCBBIL_tbl['HOLD_BILL']=line[105:106]
    BDT_BCCBBIL_tbl['TACCNT']=line[107:108]
    BDT_BCCBBIL_tbl['TFGRP']=line[108:109]   
    BDT_BCCBBIL_tbl['TACCNT_FGRP']=translate_TACCNT_FGRP(BDT_BCCBBIL_tbl['TACCNT'],BDT_BCCBBIL_tbl['TFGRP'])  
    
    BDT_BCCBBIL_tbl['BILL_DATE']=BDT_BCCBBIL_tbl['JDATE']
    BDT_BCCBBIL_tbl['EOBDATEA']=BDT_BCCBBIL_tbl['EOB_DATE']
    BDT_BCCBBIL_tbl['EOBDATECC']=BDT_BCCBBIL_tbl['EOBDATEA']
    BDT_BCCBBIL_tbl['BILLDATECC']=BDT_BCCBBIL_tbl['BILL_DATE']
    BDT_BCCBBIL_tbl['CAIMS_REL']='B'
    BDT_BCCBBIL_tbl['MPB']=line[224:225]
    BDT_BCCBBIL_tbl['INPUT_RECORDS']=str(record_id)
 
    process_insert_table("BDT_BCCBBIL", BDT_BCCBBIL_tbl,BDT_BCCBBIL_DEFN_DICT)
    
    root_rec=True
 
    
def process_TYP0505_BO_CODE(): 
    debug("****** procedure==>  "+whereami()+" ******")
    "BO_CODE"
    
    global BDT_BCCBBIL_tbl  
    global root_rec
  
    initialize_BCCBBIL_tbl()
    "record_id doesn't need populated"
    
    "we already know ACNA, EOB_DATE, and BAN are the same"   

#    get bo_code
    BDT_BCCBBIL_tbl['BO_CODE']=line[211:215].rstrip(' ').lstrip(' ')
    BDT_BCCBBIL_tbl['INPUT_RECORDS']=str(record_id)
    
#    process_update_bccbbil() 
    if root_rec:
        process_update_table("BDT_BCCBBIL", BDT_BCCBBIL_tbl, BDT_BCCBBIL_DEFN_DICT)
    else:
#        process_insert_table("BDT_BCCBBIL", BDT_BCCBBIL_tbl, BDT_BCCBBIL_DEFN_DICT)
         process_ERROR_END("Trying to update BO_CODE but there is no root record. record_id: "+str(record_id))
         
    "no flag to set - part of root"

def process_TYP0510_BALDUE():
    debug("****** procedure==>  "+whereami()+" ******")
    "BALDUE"   
    global BDT_BCCBBIL_tbl 
    global baldue_rec
    global root_rec
 
    initialize_BCCBBIL_tbl()

    #CURR_INVOICE
    BDT_BCCBBIL_tbl['REF_NUM']=line[79:89]   
    
    
    #populate astate
    if line[37:39] == '  ':
        BDT_BCCBBIL_tbl['ASTATE']='XX'
    else:
        BDT_BCCBBIL_tbl['ASTATE']=line[37:39] 
    
    BDT_BCCBBIL_tbl['INVDATECC']=line[89:94]    
    BDT_BCCBBIL_tbl['PREVBAL']=line[97:108]
    BDT_BCCBBIL_tbl['PAYMNT']=line[108:119]
    BDT_BCCBBIL_tbl['ADJT']=line[119:130]
    BDT_BCCBBIL_tbl['ADJIR']=line[130:141]
    BDT_BCCBBIL_tbl['ADJIA']=line[141:152]
    BDT_BCCBBIL_tbl['ADJUS']=line[152:163]
    BDT_BCCBBIL_tbl['BAL']=line[163:174]
    BDT_BCCBBIL_tbl['ADLOC']=line[178:185]  
    BDT_BCCBBIL_tbl['INPUT_RECORDS']=str(record_id)
   
#    process_update_bccbbil() 
     
    if root_rec:
        process_update_table("BDT_BCCBBIL", BDT_BCCBBIL_tbl, BDT_BCCBBIL_DEFN_DICT)
    else:
        process_ERROR_END("BALDUE record needs a root record. Record id:"+str(record_id))  
        
    baldue_rec=True
 

def process_TYP0512_CRNT1():    
    debug("****** procedure==>  "+whereami()+"(record id:"+str(record_id)+") ******")
    global BDT_CRNT1_tbl 
    global BDT_CRNT1_DEFN_DICT 
    global crnt1_051200_rec,crnt1_055000_rec
    
    initialize_CRNT1_tbl()
    
    if not root_rec or not baldue_rec or not baldtl_rec:
        writelog("ERROR???: Writing CRNT1 record but missing parent records.")
 
   
#    BDT_CRNT1_tbl
#    BUILD pieces here
    BDT_CRNT1_tbl['REF_NUM']=line[61:71]
    BDT_CRNT1_tbl['INVDT1CC']=line[71:76]
    BDT_CRNT1_tbl['SUBSTATE']=line[77:79]
    BDT_CRNT1_tbl['MONCHGFROMCC']=line[79:87] 
    BDT_CRNT1_tbl['MONCHGTHRUCC']=line[87:95]  
    BDT_CRNT1_tbl['DTINVDUECC']=line[95:103] 
    BDT_CRNT1_tbl['LPC']=line[103:114] 
    BDT_CRNT1_tbl['TOT_MRC']=line[136:147] 
    BDT_CRNT1_tbl['MRCIR']=line[147:158]
    BDT_CRNT1_tbl['MRCIA']=line[158:169]
    
    if record_id == '051200':
        BDT_CRNT1_tbl['MRCLO']=line[180:191]
        BDT_CRNT1_tbl['STLVCC']=line[222:226] 
        crnt1_051200_rec+=1
    elif record_id == '055000':
        BDT_CRNT1_tbl['MRCLO']=line[169:180]
        BDT_CRNT1_tbl['STLVCC']=line[217:221 ]         
        crnt1_055000_rec+=1
    else:
        process_ERROR_END("ERROR: Expected record_id 051200 or 055000 but recieved a record_id of "+str(record_id))

    
    BDT_CRNT1_tbl['INPUT_RECORDS']=str(record_id)

    process_insert_table("BDT_CRNT1", BDT_CRNT1_tbl, BDT_CRNT1_DEFN_DICT)
               
 
    
def process_TYP0513_CRNT2():    
    debug("****** procedure==>  "+whereami()+"(record id:"+str(record_id)+") ******")
    global BDT_CRNT2_tbl 
    global BDT_CRNT2_DEFN_DICT 
    global crnt2_rec 
    
    initialize_CRNT2_tbl()
    
    if not root_rec or not baldue_rec:
        writelog("ERROR???: Writing crnt2 record but missing parent root record and baldue record.")
 
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
    BDT_CRNT2_tbl['TOT_OCC']=line[79:90]
    BDT_CRNT2_tbl['OCCIR']=line[90:101]
    BDT_CRNT2_tbl['OCCIA']=line[101:112]
    BDT_CRNT2_tbl['OCCUS']=line[112:123]
    BDT_CRNT2_tbl['TOT_USG']=line[123:134]
    BDT_CRNT2_tbl['USGIR']=line[134:145]
    BDT_CRNT2_tbl['USGIA']=line[145:156]
    BDT_CRNT2_tbl['TOT_TAX']=line[156:167]
    BDT_CRNT2_tbl['CRNTCHG']=line[167:178]
    BDT_CRNT2_tbl['INPUT_RECORDS']=str(record_id)
    if record_id == '051300':
        BDT_CRNT2_tbl['TOT_SURCHG']=line[190:201]
        BDT_CRNT2_tbl['STLVCC2']=line[206:210]
        "DONT UPDATE OR INSERT A RECORD, the 051300 should always be followed by a 051301 record"
        " to populate OCCLO and USGLO records, so return will go back to read another record"
        " and OCCLO and USGLO will be populated by the TYP05131 module"
    if record_id == '055100':
        BDT_CRNT2_tbl['OCCLO']=line[178:189]
        BDT_CRNT2_tbl['STLVCC2']=line[205:209]
        BDT_CRNT2_tbl['USGLO']=line[209:220]
        process_insert_table("BDT_CRNT2", BDT_CRNT2_tbl, BDT_CRNT2_DEFN_DICT)
    
        crnt2_rec=True    
   
def process_TYP05131_CRNT2():    
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_CRNT2_tbl 
    global BDT_CRNT2_DEFN_DICT 
    global crnt2_rec 
    
    "Dont initialize.  Initialization was done in TYPE0513 para"
#    initialize_CRNT2_tbl()

#   FIXFORM ON BCTFBDTI X105 OCCLO/Z11.2 USGLO/Z11.2

#    BDT_CRNT2_tbl
#    BUILD pieces here
   
    BDT_CRNT2_tbl['OCCLO']=line[105:116]
    BDT_CRNT2_tbl['USGLO']=line[116:127]
    BDT_CRNT2_tbl['INPUT_RECORDS']+="*"+str(record_id)
    
    if prev_record_id == '051300':
        process_insert_table("BDT_CRNT2", BDT_CRNT2_tbl, BDT_CRNT2_DEFN_DICT)
    else:
        process_ERROR_END("ERROR: Previous record should have been a 051300.")
               
    crnt2_rec=True       
    
def process_TYP0514_SWSPLCHG():  
    debug("****** procedure==>  "+whereami()+"(record id:"+str(record_id)+") ******")
    global BDT_SWSPLCHG_tbl       
    global BDT_SWSPLCHG_DEFN_DICT 
    global swsplchg_rec 
    
    initialize_SWSPLCHG_tbl()
    
    if not root_rec or not baldue_rec:
        writelog("ERROR???: Writing crnt2 record but missing parent records.")

    BDT_SWSPLCHG_tbl['STATE_IND']=line[76:78]
    BDT_SWSPLCHG_tbl['ST_LVLCC']=line[78:82]
    BDT_SWSPLCHG_tbl['CHGFROMDTCC']=line[86:94]
    BDT_SWSPLCHG_tbl['CHGTHRUDTCC']=line[94:102] 
    BDT_SWSPLCHG_tbl['MAC_ACCTYP']=line[102:103]
    BDT_SWSPLCHG_tbl['MAC_FACTYP']=line[103:104]
    BDT_SWSPLCHG_tbl['MACIR']=line[104:115]
    BDT_SWSPLCHG_tbl['MACIA']=line[115:126] 
    if record_id in ('051400','051600','055200'):
        BDT_SWSPLCHG_tbl['MACND']=0
        BDT_SWSPLCHG_tbl['MACIRIA']=line[126:137]
        BDT_SWSPLCHG_tbl['MACIAIA']=line[137:148]
        BDT_SWSPLCHG_tbl['MACLOC']=line[148:159]
        
    if record_id in ('051500','055300'):
        BDT_SWSPLCHG_tbl['MACND']=line[126:137]
        BDT_SWSPLCHG_tbl['MACIRIA']=line[137:148]
        BDT_SWSPLCHG_tbl['MACIAIA']=line[148:159]
        BDT_SWSPLCHG_tbl['MACLOC']=line[159:170]
    
    if record_id in ('051400','055200'):
        BDT_SWSPLCHG_tbl['MAC_RECTYP']=14
    elif record_id in ('051600','055400'):
        BDT_SWSPLCHG_tbl['MAC_RECTYP']=16
    elif record_id in ('051500','055300'):
        BDT_SWSPLCHG_tbl['MAC_RECTYP']=15
 

    BDT_SWSPLCHG_tbl['INPUT_RECORDS']=str(record_id)
 
    process_insert_table("BDT_SWSPLCHG", BDT_SWSPLCHG_tbl, BDT_SWSPLCHG_DEFN_DICT)
    
    swsplchg_rec=True

def process_TYP1505_PMNTADJ():
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_PMNTADJ_tbl
    global pmntadj_rec
    global BDT_PMNTADJ_DEFN_DICT

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
    BDT_PMNTADJ_tbl['AMOUNT']=line[149:160]
    BDT_PMNTADJ_tbl['INPUT_RECORDS']=str(record_id)
    

    process_insert_table("BDT_PMNTADJ", BDT_PMNTADJ_tbl, BDT_PMNTADJ_DEFN_DICT) 

    pmntadj_rec=True

def process_TYP2005_ADJMTDTL():                    
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_ADJMTDTL_tbl
    global adjmtdtl_rec
 
    
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
    debug("****** procedure==>  "+whereami()+" ******") 
    global BDT_ADJMTDTL_tbl
    global adjmtdtl_rec
    global BDT_ADJMTDTL_DEFN_DICT
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
    
    if prev_record_id == '200500':
        process_insert_table("BDT_ADJMTDTL", BDT_ADJMTDTL_tbl, BDT_ADJMTDTL_DEFN_DICT)
    else:
        process_ERROR_END("ERROR: previous record should have been a 200500.")
               
    adjmtdtl_rec=True   
    
def process_TYP2505_BALDTL():                
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_BALDTL_tbl 
    global baldtl_rec
     
    initialize_BALDTL_tbl()    
    
     #DTL_INVOICE-concatenation of DINV_REF and DINV_DATE
  
    
    BDT_BALDTL_tbl['DINV_REF']=line[61:71]
    BDT_BALDTL_tbl['DINVDATECC']=line[71:76]    
    BDT_BALDTL_tbl['DSTATE']=line[76:78] 
    BDT_BALDTL_tbl['DPREVBAL']=line[116:127]
    BDT_BALDTL_tbl['DPAYMNT']=line[127:138]
    BDT_BALDTL_tbl['DADJT']=line[138:149]
    BDT_BALDTL_tbl['DBAL']=line[149:160]
    BDT_BALDTL_tbl['LPC_APPLIED']=line[164:175]
    BDT_BALDTL_tbl['INPUT_RECORDS']=str(record_id)
    
    if baldue_rec:
        process_insert_table("BDT_BALDTL", BDT_BALDTL_tbl, BDT_BALDTL_DEFN_DICT) 
    else:
        writelog("WARNING: The BALDTL record "+str(record_id)+" has no associated BALDUE record.")
    
    baldtl_rec=True



     
def initialize_BDT_ADJMTDTL_tbl():            
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_ADJMTDTL_tbl
    

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
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_PMNTADJ_tbl
   
    BDT_PMNTADJ_tbl['ACNA']=current_abbd_rec_key['ACNA']
    BDT_PMNTADJ_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    BDT_PMNTADJ_tbl['BAN']=current_abbd_rec_key['BAN']

    if record_id == '150500':
        BDT_PMNTADJ_tbl['PORA']='P'        
    BDT_PMNTADJ_tbl['AINV_REF']=line[61:71]
    BDT_PMNTADJ_tbl['AINV_DATE']=line[71:76]
    BDT_PMNTADJ_tbl['DATERCVCC']=line[76:84]
    BDT_PMNTADJ_tbl['APSTATE']=line[132:133]
    BDT_PMNTADJ_tbl['AMOUNT']=line[149:160]
    BDT_PMNTADJ_tbl['INPUT_RECORDS']=str(record_id)
     
     
     
def initialize_BALDTL_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_BALDTL_tbl

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
    BDT_BALDTL_tbl['INPUT_RECORDS']=''



def  initialize_BCCBBIL_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_BCCBBIL_tbl
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
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_CRNT1_tbl
    global current_abbd_rec_key 
    
    BDT_CRNT1_tbl['ACNA']=current_abbd_rec_key['ACNA']    
    BDT_CRNT1_tbl['EOB_DATE']=current_abbd_rec_key['EOB_DATE']
    BDT_CRNT1_tbl['BAN']=current_abbd_rec_key['BAN']

    BDT_CRNT1_tbl['REF_NUM']=line[61:71]
    BDT_CRNT1_tbl['INVDT1CC']=line[71:76]
    BDT_CRNT1_tbl['SUBSTATE']=line[78:77]
    BDT_CRNT1_tbl['MONCHGFROMCC']=line[79:87] 
    BDT_CRNT1_tbl['MONCHGTHRUCC']=line[87:95]  
    BDT_CRNT1_tbl['DTINVDUECC']=line[95:103] 
    BDT_CRNT1_tbl['LPC']=line[103:114] 
    BDT_CRNT1_tbl['TOT_MRC']=line[136:147] 
    BDT_CRNT1_tbl['MRCIR']=line[147:158]
    BDT_CRNT1_tbl['MRCIA']=line[158:169]
    BDT_CRNT1_tbl['MRCLO']=line[180:191]
    BDT_CRNT1_tbl['STLVCC']=line[222:226] 
    BDT_CRNT1_tbl['INPUT_RECORDS']=''
    
def  initialize_CRNT2_tbl():
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_CRNT2_tbl
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
    debug("****** procedure==>  "+whereami()+" ******")
    global BDT_SWSPLCHG_tbl
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
    debug("****** procedure==>  "+whereami()+" ******")
    global bdt_BCCBBIL_log
    
    bdt_BCCBBIL_log.write("\n"+msg)


def process_ERROR_END(msg):
    writelog("ERROR:"+msg)
    debug("ERROR:"+msg)
    process_close_files()
    raise "ERROR:"+msg
    
    
def process_close_files():
    debug("****** procedure==>  "+whereami()+" ******")
    global bdt_input
    global bdt_BCCBBIL_log
    
    if debugOn:
        bdt_debug_log.close()
        
    bdt_input.close();
    bdt_BCCBBIL_log.close()
   
    
    
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