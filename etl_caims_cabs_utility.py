# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
 :  

"""

#import inspect
#import collections
import cx_Oracle
import re
#import sys
#import ConfigParser
#import platform
#import os

##Global variables
global  con
global record_counts,unknown_record_counts
global meta_load_dtm
global meta_upd_dtm
global running_user 
global eob_date
global file_hdr_id   


#record_counts={}
#unknown_record_counts={} 
##http://www.3480-3590-data-conversion.com/article-signed-fields.html
MONTH_DICT={'01':'JAN','02':'FEB','03':'MAR','04':'APR','05':'MAY','06':'JUN','07':'JUL','08':'AUG','09':'SEP','10':'OCT','11':'NOV','12':'DEC',} 
DIGIT_DICT={'0':'0','1':'1','2':'2','3':'3','4':'4','5':'5','6':'6','7':'7','8':'8','9':'9',\
            '{':'0','A':'1','B':'2','C':'3','D':'4','E':'5','F':'6','G':'7','H':'8','I':'9',\
            '}':'0','J':'1','K':'2','L':'3','M':'4','N':'5','O':'6','P':'7','Q':'8','R':'9'}

NEGATIVE_NUMS=['}','J','K','L','M','N','O','P','Q','R']
 
#    
#def whereami():
#    # returns the name of the current executing procedure, etc...
#   #e.g. procedure==>  format_date
#    return inspect.stack()[1][3]         
    
#returns true if any characters in the acna string contain bad non-blank whitespace characters
#or other bad character.
def invalid_acna_chars(strg, search=re.compile(r'[^A-Z0-9- ]').search):
    return bool(search(strg))

def format_date(datestring):
#    return 19000101 for null /zero values
    dtSize=len(datestring)
    
    if datestring.strip('0').strip(' ') == '' :
        return "TO_DATE('1900-01-01','YYYY-MM-DD')"  
    elif dtSize ==5:
        #jdate conversion
        return "TO_DATE('"+datestring+"','YY-DDD')"
    elif dtSize==6:
        return "TO_DATE('"+datestring[4:6]+"-"+MONTH_DICT[datestring[2:4]]+"-"+"20"+datestring[:2]+"','DD-MON-YY')"  
    elif dtSize==8:
        return "TO_DATE('"+datestring[:4]+"-"+datestring[4:6]+"-"+datestring[6:8]+"','YYYY-MM-DD')"     


def process_check_exists(tbl_name, tbl_rec,tbl_dic,con,schema,output_log):
#(process_check_exists("CAIMS_BDT_BALDTL", tmpTblRec, BDT_BALDTL_DEFN_DICT,con,output_log)):
#return true or false
    sqlQuery="SELECT ID FROM "+schema+"."+tbl_name+" WHERE "
    
    for key, value in tbl_rec.items() :
      
        nulVal=False
        if str(value).rstrip(' ') == '':
            nulVal=True
               
        try:
            if key == 'ACNA':
                sqlQuery+="ACNA='"+str(value).rstrip(' ')+"' AND "
            elif key == 'EOB_DATE':
                sqlQuery+="EOB_DATE="+format_date(value)+" AND "
            elif key == 'BAN':
                sqlQuery+="BAN='"+str(value).rstrip(' ')+"' AND "
            #####STRINGS##############
            elif tbl_dic[key] in ('c|STRING','x|STRING'):
                if not nulVal:
                    sqlQuery+=str(key)+"='"+str(value).rstrip(' ')+"' AND "
            #####DATETIME#############
            elif tbl_dic[key] in ('c|DATETIME','x|DATETIME'):
#                print "date key and value:"+str(key)+" "+str(value)
                if not nulVal:
                    sqlQuery+=str(key)+"="+format_date(value)+" AND "    
            #####NUMBERS#############               
            elif tbl_dic[key] in ('c|NUMBER','x|NUMBER','c|INTEGER','x|INTEGER'):
                if not nulVal:                   
                    sqlQuery+=str(key)+"="+str(value)+" AND "       
            else:
#                process_ERROR_END("ERROR: process_update_table could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.")  
                raise Exception("ERROR: process_check_exists could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.") 
        except KeyError as e:
            writelog("WARNING: Column "+key+" not found in the "+tbl_name+" table.  Skipping.",output_log)
            writelog("KeyError:"+e.message,output_log)
                
    sqlQuery=sqlQuery.rstrip(' ').rstrip('AND')
     
        
    chkCurs=con.cursor()
 
    val=-1
    result=-1
    try:
        
        chkCurs.execute(sqlQuery)
        for id in chkCurs:
            result=id
        con.commit()
#        print "Number of rows updated: " + updCurs.count??????
        con.commit()
        
        writelog("SUCCESSFUL RECORD CHECK TO  "+tbl_name+". result:"+str(result),output_log)
        
    except cx_Oracle.DatabaseError, exc:
        if ("%s" % exc.message).startswith('ORA-:'):
            writelog("ERROR:"+str(exc.message),output_log)
            writelog("UPDATE SQL Causing problem:"+updateSQL,output_log)
    finally:
        chkCurs.close()

    if result > 0:
        val=int(result[0])
        return val
    else:
        return 0
   
def set_meta_vars(load_dt,upd_dt,user, partition_date,file_hdr):
    global meta_load_dtm
    global meta_upd_dtm
    global running_user 
    global eob_date
    global file_hdr_id   
     
    meta_load_dtm=load_dt
    meta_upd_dtm=upd_dt
    running_user=user
    eob_date=format_date(partition_date)
    file_hdr_id=file_hdr


    
def process_insert_table(tbl_name, tbl_rec,tbl_dic,con,schema,seq_name,output_log):
    global meta_load_dtm
    global meta_upd_dtm
    global running_user 
    global eob_date
    global file_hdr_id   
 
    
#    firstPart="INSERT INTO "+schema+"."+tbl_name+" (ID," #add columns
#    secondPart=" VALUES ("+str(seq_name)+".nextVal,"  #add values
    firstPart="INSERT INTO "+schema+"."+tbl_name+" (ID,FILE_HDR_ID,META_LOAD_DTTM,LOAD_ACTIVITY_CUID,META_UPD_DTTM,LAST_ACTIVITY_CUID,"  
    secondPart=" VALUES ("+str(seq_name)+".nextVal,"+file_hdr_id.rstrip('')+","+meta_load_dtm+",'"+running_user+"',"+meta_upd_dtm+",'"+running_user+"',"    
   
    for key, value in tbl_rec.items() :
        nulVal=False
        if str(value).rstrip(' ') == '':
            nulVal=True
#             "EMPTY VALUE"
    
        try:
            if key =='ID':
                pass
            elif tbl_dic[key] in ('c|STRING', 'x|STRING'):
                firstPart+=key+","
                if nulVal:
                    if tbl_dic[key] == 'x|STRING':
                        secondPart+="'nl',"
                    else:
                        secondPart+="NULL,"
                else:
#                    secondPart+="'"+str(value).rstrip(' ')+"',"
#                    new code from Jason
                     secondPart+="'"+str(value).rstrip(' ').replace("'","''")+"',"
            elif tbl_dic[key] in ('c|DATETIME', 'x|DATETIME'):
                firstPart+=key+","
                if nulVal:
                    if tbl_dic[key] == 'x|DATETIME':  #use 19000101 as null value if index key
                        secondPart+=format_date('19000101')+","
                    else:
                        secondPart+="NULL,"
                else:                    
                    secondPart+=format_date(value)+","
            elif tbl_dic[key] in ('c|NUMBER','x|NUMBER','c|INTEGER','x|INTEGER'):
                firstPart+=key+","
                "RULE: if null/blank number, then populate with 0."
                if nulVal:
                    secondPart+="0,"
                else:                    
                    secondPart+=str(value)+","                    
            else:
#                process_ERROR_END("ERROR: "+whereami()+"procedure could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.") 
                raise Exception("ERROR: Could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.")
        except KeyError as e:
            writelog("WARNING: Column "+key+" not found in the "+tbl_name+" table.  Skipping.",output_log)
            writelog("KeyError:"+e.message,output_log)

    #if EOB_DATE not part of insert we need to add it.
    #EOB_DATE is how we're partioning all Oracle tables. 
#METADATA CHANGES-START
    if firstPart.find(",EOB_DATE,") >0:
        firstPart=firstPart.rstrip(',')+")"     
        secondPart=secondPart.rstrip(',') +")"          
    else:
#        firstPart+=key+","
        firstPart+="EOB_DATE)"
        secondPart+=eob_date+")"
#METADATA CHANGES        
     
    insCurs=con.cursor()
    insSQL=firstPart+secondPart  + " returning ID into :id "
    
#    returning Id
#>               into :id"
    idVal=insCurs.var(cx_Oracle.NUMBER)
        
    try:
        insCurs.execute(insSQL,id=idVal) 
        con.commit()
        writelog("SUCCESSFUL INSERT INTO "+tbl_name+".",output_log)
#        return idVal.getvalue()
        insCurs.close()
        return int(idVal.getvalue())
    except cx_Oracle.DatabaseError , e:
        if ("%s" % e.message).startswith('ORA-00001:'):
            writelog("****** DUPLICATE INSERT INTO "+str(tbl_name)+"*****************",output_log)
#            writelog("ACNA:"+tbl_rec['ACNA']+" BAN:"+tbl_rec['BAN']+" EOB_DATE:"+tbl_rec['EOB_DATE'],output_log)
            writelog(str(tbl_rec),output_log)
            writelog("Insert SQL: "+str(insSQL),output_log)
            writelog("***************************************************************",output_log)
        else:
            writelog("ERROR:"+str(e.message),output_log)
            writelog("SQL causing problem:"+insSQL,output_log)
        insCurs.close()
        return -1
 
        
        
   
def process_update_table(tbl_name, tbl_rec,tbl_dic,con,schema,output_log,**optional_where_condition):
    
    "ASSUMPTIONS!!!"
    "THis code assumes that if a value is null that it does not update the ORacle table."
    "Need to make a code change if that rule is not true"
    
    "This routine is intended to update a single row.  If more than record is updated the process errors"
    "Optional"     
    
    whereClause="WHERE "
    
    updateSQL="UPDATE "+schema+"."+tbl_name+" SET " #add columns
    

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
            #####STRINGS##############
            elif tbl_dic[key] == 'c|STRING':
                if str(key) == 'INPUT_RECORDS':
                    updateSQL+="INPUT_RECORDS = INPUT_RECORDS||',"+str(value).rstrip(' ')+"',"
                elif not nulVal:
                    updateSQL+=str(key)+"='"+str(value).rstrip(' ')+"',"
            elif tbl_dic[key] == 'x|STRING':
                if nulVal:
                    #problem ix value should not be null
#                    process_ERROR_END("ERROR: "+key+" is a unique index STRING/VARCHAR value but was passed as null to the "+whereami()+ " procedure for an update to "+tbl_name)
                    writelog("WARNING: "+key+" is a unique index STRING/VARCHAR value but was passed as null when updating "+tbl_name,output_log)
#                    raise Exception("ERROR: "+key+" is a unique index STRING/VARCHAR value but was passed as null when updating "+tbl_name)
                else:
                   whereClause+=str(key)+"='"+str(value).rstrip(' ')+"' AND "  
                   
            #####DATETIME#############
            elif tbl_dic[key] =='c|DATETIME':
#                print "date key and value:"+str(key)+" "+str(value)
                if not nulVal:
                    updateSQL+=str(key)+"="+format_date(value)+"," 
            elif tbl_dic[key] =='x|DATETIME':
                if nulVal:
                    #problem ix value should not be null
#                    process_ERROR_END("ERROR: "+key+" is a unique index DATETIME value but was passed as null to the "+whereami()+ " procedure for an update to "+tbl_name)
#                    raise Exception("ERROR: "+key+" is a unique index DATETIME value but was passed as null when updating "+tbl_name) 
                    writelog("WARNING: "+key+" is a unique index STRING/VARCHAR value but was passed as null when updating "+tbl_name,output_log)
                else:
                    whereClause+=str(key)+"="+format_date(value)+" AND "  
                   
            #####NUMBERS#############               
            elif tbl_dic[key] in ('c|NUMBER','c|INTEGER'):
                if not nulVal:  
                    updateSQL+=str(key)+"="+str(value)+","        
            elif tbl_dic[key] in ('x|NUMBER','x|INTEGER'):        
                if nulVal:
                    #problem ix value should not be null
#                    process_ERROR_END("ERROR: "+key+" is a unique index NUMBER value but was passed as null to the "+whereami()+ " procedure for an update to "+tbl_name)
#                     raise Exception("ERROR: "+key+" is a unique index NUMBER value but was passed as null when updating "+tbl_name)
                     writelog("WARNING: "+key+" is a unique index STRING/VARCHAR value but was passed as null when updating "+tbl_name,output_log)
                else:
                   whereClause+=str(key)+"="+str(value)+" AND "  
                    
            else:
#                process_ERROR_END("ERROR: process_update_table could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.")  
                raise Exception("ERROR: process_update_table could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.") 
        except KeyError as e:
            writelog("WARNING: Column "+key+" not found in the "+tbl_name+" table.  Skipping.",output_log)
            writelog("KeyError:"+e.message,output_log)
        
    if ('addlwhere' in optional_where_condition):
#        print 'optional parameter found, it is ', optional_where_condition['addlwhere']
        whereClause+=" "+optional_where_condition['addlwhere']+" "
            
    whereClause=whereClause.rstrip(' ').rstrip('AND')
    updateSQL=updateSQL.rstrip(',')        
    updCurs=con.cursor()
    updateSQL+=" "+whereClause
    
    #Not all KEY values need to be part of the where clause, check to see that the where clause has at least one condition.
    if len(whereClause) <=6:
        raise Exception("ERROR: the where clause for process_update_table has no conditions. SQL:"+updateSQL)
        
    try:
        updCurs.execute(updateSQL) 
        if (updCurs.rowcount) == 1:
            writelog("SUCCESSFUL UPDATE TO  "+tbl_name+". "+str(updCurs.rowcount) +" row udpated.",output_log)
        elif (updCurs.rowcount) == 0:
            writelog("ERROR:  No records updated for "+tbl_name,output_log)
            writelog("Problem SQL:"+updateSQL,output_log)
        elif (updCurs.rowcount) > 1:
            writelog("ERROR: "+str(updCurs.rowcount)+" records updated for "+tbl_name+".  process_update_table intended to update a single record.",output_log)
            writelog("Problem SQL:"+updateSQL,output_log)
        con.commit()
    except cx_Oracle.DatabaseError, exc:
        if ("%s" % exc.message).startswith('ORA-:'):
            writelog("****** ORACLE ERROR "+str(tbl_name)+"**************************",output_log)
            writelog("ERROR:"+str(exc.message),output_log)
            writelog("Problem SQL:"+updateSQL,output_log)
            writelog("***************************************************************",output_log)
    finally:
        updCurs.close()
                 


def convertnumber(number, decimalPlaces) :

    num=number.rstrip(' ').lstrip(' ')
    if num == '':
        return 0
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
        raise Exception("ERROR: Cant Convert Number: "+str(num))




def getTableColumns(tablenm,con,schema,output_log):
      
         
    myCurs=con.cursor()
    myTbl=tablenm
    mySQL="select * FROM "+schema+".%s WHERE ROWNUM=1" % (myTbl)
    tmpArray=[]
    try:    
        myCurs.execute(mySQL)  
        for x in myCurs.description:
            tmpArray.append(x)
        con.commit()
    except cx_Oracle.DatabaseError, exc:
        if ("%s" % exc.message).startswith('ORA-:'):
            writelog("ERROR:"+str(exc.message),output_log)
            writelog("Problem SQL:"+mySQL,output_log)
    finally:
        myCurs.close()
        
    return tmpArray  
 
def getUniqueKeyColumns(tablenm,con,output_log):
             
    keyCurs=con.cursor()
    myTbl=tablenm
    
    mySQL="SELECT column_name FROM USER_IND_COLUMNS WHERE INDEX_NAME IN (SELECT INDEX_NAME FROM USER_INDEXES WHERE TABLE_NAME ='"+myTbl+"' AND UNIQUENESS='UNIQUE')" 
    colArray=[]
    try:    
        keyCurs.execute(mySQL)  
        for x in keyCurs:
            colArray.append(x)
        con.commit()
    except cx_Oracle.DatabaseError, exc:
        if ("%s" % exc.message).rstrip(' ')== 'ORA-00942':
            raise Exception("ERROR:Table does not exist: "+tablenm+" table.") 
        elif ("%s" % exc.message).startswith('ORA-:'):
            writelog("ERROR:"+str(exc.message),output_log)
            writelog("SQL causing problem:"+mySQL,output_log)
    finally:
        keyCurs.close()
        
    return colArray  
    
def setDictFields (tablenm, defn_dict):
   #This Procedure sets the field names in the table dictionaries 
    #used for updating and inserting into oracle tables./
           
#    colTypDict=collections.OrderedDict()
    colDict=dict()
        
     
    for key,value in defn_dict.items() :
        colDict[key]=''
    
    return colDict 

    
def createTableTypeDict(tablenm, con,schema,output_log):
    #This Procedure retrieves table columsn and unique key columns
    #-It iterates through and marks the column as either and x (index)
    #-or a c (column).  This is then used in the update routine.  The
    #Update table can then use the unique index fields in the where
    #clause
           
#    colTypDict=collections.OrderedDict()
    colTypDict=dict()
        
    colArray=[]
    colArray=getTableColumns(tablenm,con,schema,output_log)
    indexArray=[]
    indexArray=getUniqueKeyColumns(tablenm,con,output_log)
     
    ixKeyFound=False
    for tblCol in colArray:
        for keyCol in indexArray:
            if tblCol[0] == keyCol[0]:
                #Index column found  "x" = unique index column
                ixKeyFound=True
                colTypDict[tblCol[0]]=str("x|")+str(tblCol[1]).replace("<type 'cx_Oracle.",'').replace("'>",'') 
                break
                
        if not ixKeyFound:
            colTypDict[tblCol[0]]=str("c|")+str(tblCol[1]).replace("<type 'cx_Oracle.",'').replace("'>",'') 
        else:
            ixKeyFound = False 
        
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
            return '$'
    else:
        return taccnt.rstrip(' ')
 
    

"####################################TRANSLATION MODULES END ############################################################"    
"########################################################################################################################" 
 
 
    
def writelog(msg,output_log):
    output_log.write("\n"+msg)



   
