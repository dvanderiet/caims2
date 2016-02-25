# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
 :  

"""

import datetime
import inspect
#import collections
startTM=datetime.datetime.now();
import cx_Oracle
#import sys
#import ConfigParser
#import platform
#import os

##Global variables
global  con
global record_counts,unknown_record_counts
 
#record_counts={}
#unknown_record_counts={} 
 
MONTH_DICT={'01':'JAN','02':'FEB','03':'MAR','04':'APR','05':'MAY','06':'JUN','07':'JUL','08':'AUG','09':'SEP','10':'OCT','11':'NOV','12':'DEC',} 
DIGIT_DICT={'{':'0','A':'1','B':'2','C':'3','D':'4','E':'5','F':'6','G':'7','H':'8','I':'9',\
            '}':'0','J':'1','K':'2','L':'3','M':'4','N':'5','O':'6','P':'7','Q':'8','R':'9'} 
NEGATIVE_NUMS=['}','J','K','L','M','N','O','P','Q','R']
 
    
def whereami():
    # returns the name of the current executing procedure, etc...
   #e.g. procedure==>  format_date
    return inspect.stack()[1][3]         
    

def format_date(datestring):
    
    dtSize=len(datestring)
    
    if dtSize ==5:
        #jdate conversion
        return "TO_DATE('"+datestring+"','YY-DDD')"
    elif dtSize==6:
        return "TO_DATE('"+datestring[4:6]+"-"+MONTH_DICT[datestring[2:4]]+"-"+"20"+datestring[:2]+"','DD-MON-YY')"  
    elif dtSize==8:
        return "TO_DATE('"+datestring[:4]+"-"+datestring[4:6]+"-"+"20"+datestring[6:2]+"','YYYY-MM-DD')"     


def process_check_exists(tbl_name, tbl_rec,tbl_dic,con,output_log):
#(process_check_exists("CAIMS_BDT_BALDTL", tmpTblRec, BDT_BALDTL_DEFN_DICT,con,output_log)):
#return true or false
    sqlQuery="SELECT ACNA FROM "+tbl_name+" WHERE "
    
    
    
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
            elif tbl_dic[key] == ('c|NUMBER','x|NUMBER'):
                if not nulVal:                   
                    sqlQuery+=str(key)+"="+str(convertnumber(value))+"' AND "       
            else:
#                process_ERROR_END("ERROR: process_update_table could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.")  
                raise Exception("ERROR: process_check_exists could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.") 
        except KeyError as e:
            writelog("WARNING: Column "+key+" not found in the "+tbl_name+" table.  Skipping.",output_log)
            writelog("KeyError:"+e.message,output_log)
                
    sqlQuery=sqlQuery.rstrip(' ').rstrip('AND')
     
        
    chkCurs=con.cursor()
 
    result=''
    try:
        chkCurs.execute(sqlQuery)
        for x in chkCurs:
            result=x
        con.commit()
#        print "Number of rows updated: " + updCurs.count??????
        con.commit()
        writelog("SUCCESSFUL RECORD CHECK TO  "+tbl_name+".",output_log)
        
    except cx_Oracle.DatabaseError, exc:
        if ("%s" % exc.message).startswith('ORA-:'):
            writelog("ERROR:"+str(exc.message),output_log)
            writelog("UPDATE SQL Causing problem:"+updateSQL,output_log)
    finally:
        chkCurs.close()

    if len(result) > 0:
        return True


    
def process_insert_table(tbl_name, tbl_rec,tbl_dic,con,output_log):
    
    firstPart="INSERT INTO "+tbl_name+" (" #add columns
    secondPart=" VALUES ("  #add values
   
    for key, value in tbl_rec.items() :
        nulVal=False
        if str(value).rstrip(' ') == '':
            nulVal=True
#             "EMPTY VALUE"
    
        try:
            if tbl_dic[key] in ('c|STRING', 'x|STRING'):
                firstPart+=key+","
                if nulVal:
                    secondPart+="NULL,"
                else:
                    secondPart+="'"+str(value).rstrip(' ')+"',"
            elif tbl_dic[key] in ('c|DATETIME', 'x|DATETIME'):
                firstPart+=key+","
                if nulVal:
                    secondPart+="NULL,"
                else:                    
                    secondPart+=format_date(value)+","
            elif tbl_dic[key] in ('c|NUMBER','x|NUMBER'):
                firstPart+=key+","
                "RULE: if null/blank number, then populate with 0."
                if nulVal:
                    secondPart+="0,"
                else:                    
                    secondPart+=str(convertnumber(value))+","                    
            else:
#                process_ERROR_END("ERROR: "+whereami()+"procedure could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.") 
                raise Exception("ERROR: "+whereami()+"procedure could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.")
        except KeyError as e:
            writelog("WARNING: Column "+key+" not found in the "+tbl_name+" table.  Skipping.",output_log)
            writelog("KeyError:"+e.message,output_log)
 
    firstPart=firstPart.rstrip(',')+")"     
    secondPart=secondPart.rstrip(',') +")"      
     
    insCurs=con.cursor()
    insSQL=firstPart+secondPart

    try:
        insCurs.execute(insSQL) 
        con.commit()
        writelog("SUCCESSFUL INSERT INTO "+tbl_name+".",output_log)
    except cx_Oracle.DatabaseError , e:
        if ("%s" % e.message).startswith('ORA-00001:'):
            writelog("****** DUPLICATE INSERT INTO "+str(tbl_name)+"*****************",output_log)
            writelog("Insert SQL: "+str(insSQL),output_log)
        else:
            writelog("ERROR:"+whereami()+":" +str(e.message),output_log)
            writelog("SQL causing problem:"+insSQL,output_log)
    finally:
        insCurs.close()
   
def process_update_table(tbl_name, tbl_rec,tbl_dic,con,output_log):
    
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
            #####STRINGS##############
            elif tbl_dic[key] == 'c|STRING':
                if str(key) == 'INPUT_RECORDS':
                    updateSQL+="INPUT_RECORDS = INPUT_RECORDS||'*"+str(value).rstrip(' ')+"',"
                elif not nulVal:
                    updateSQL+=str(key)+"='"+str(value).rstrip(' ')+"',"
            elif tbl_dic[key] == 'x|STRING':
                if nulVal:
                    #problem ix value should not be null
#                    process_ERROR_END("ERROR: "+key+" is a unique index STRING/VARCHAR value but was passed as null to the "+whereami()+ " procedure for an update to "+tbl_name)
                    raise Exception("ERROR: "+key+" is a unique index STRING/VARCHAR value but was passed as null to the "+whereami()+ " procedure for an update to "+tbl_name)
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
                    raise Exception("ERROR: "+key+" is a unique index DATETIME value but was passed as null to the "+whereami()+ " procedure for an update to "+tbl_name) 
                else:
                    whereClause+=str(key)+"="+format_date(value)+" AND "  
                   
            #####NUMBERS#############               
            elif tbl_dic[key] =='c|NUMBER':
                if not nulVal:  
                    updateSQL+=str(key)+"="+str(convertnumber(value))+","        
            elif tbl_dic[key] =='x|NUMBER':        
                if nulVal:
                    #problem ix value should not be null
#                    process_ERROR_END("ERROR: "+key+" is a unique index NUMBER value but was passed as null to the "+whereami()+ " procedure for an update to "+tbl_name)
                     raise Exception("ERROR: "+key+" is a unique index NUMBER value but was passed as null to the "+whereami()+ " procedure for an update to "+tbl_name)
                else:
                   whereClause+=str(key)+"='"+str(convertnumber(value))+"' AND "  
                    
            else:
#                process_ERROR_END("ERROR: process_update_table could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.")  
                raise Exception("ERROR: process_update_table could not determine data type for " +tbl_dic[key] + " in the "+tbl_name+" table.") 
        except KeyError as e:
            writelog("WARNING: Column "+key+" not found in the "+tbl_name+" table.  Skipping.",output_log)
            writelog("KeyError:"+e.message,output_log)
                
    whereClause=whereClause.rstrip(' ').rstrip('AND')
    updateSQL=updateSQL.rstrip(',')
        
    updCurs=con.cursor()
    updateSQL+=" "+whereClause
 
    try:
        updCurs.execute(updateSQL) 
#        print "Number of rows updated: " + updCurs.count??????
        con.commit()
        writelog("SUCCESSFUL UPDATE TO  "+tbl_name+".",output_log)
        writelog("SUCCESSFUL INSERT INTO "+tbl_name+".",output_log)
        
    except cx_Oracle.DatabaseError, exc:
        if ("%s" % exc.message).startswith('ORA-:'):
            writelog("ERROR:"+str(exc.message),output_log)
            writelog("UPDATE SQL Causing problem:"+updateSQL,output_log)
    finally:
        updCurs.close()
                 


def convertnumber(num) :

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
        if newNum in NEGATIVE_NUMS:
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
#        process_ERROR_END("ERROR: Cant Convert Number: "+str(num) +"   from line:"+str(line))
        raise Exception("ERROR: Cant Convert Number: "+str(num))



def getTableColumns(tablenm,con,output_log):
      
         
    myCurs=con.cursor()
    myTbl=tablenm
    mySQL="select * FROM %s WHERE ROWNUM=1" % (myTbl)
    tmpArray=[]
    try:    
        myCurs.execute(mySQL)  
        for x in myCurs.description:
            tmpArray.append(x)
        con.commit()
    except cx_Oracle.DatabaseError, exc:
        if ("%s" % exc.message).startswith('ORA-:'):
            writelog("ERROR:"+str(exc.message),output_log)
            writelog("SQL causing problem:"+mySQL,output_log)
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
        if ("%s" % exc.message).startswith('ORA-:'):
            writelog("ERROR:"+str(exc.message),output_log)
            writelog("SQL causing problem:"+mySQL,output_log)
    finally:
        keyCurs.close()
        
    return colArray  
 
def createTableTypeDict(tablenm, con,output_log):
    #This Procedure retrieves table columsn and unique key columns
    #-It iterates through and marks the column as either and x (index)
    #-or a c (column).  This is then used in the update routine.  The
    #Update table can then use the unique index fields in the where
    #clause
           
#    colTypDict=collections.OrderedDict()
    colTypDict=dict()
        
    colArray=[]
    colArray=getTableColumns(tablenm,con,output_log)
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
            return taccnt.rstrip(' ')
    else:
        return tfgrp.rstrip(' ')
 
    

"####################################TRANSLATION MODULES END ############################################################"    
"########################################################################################################################" 
 
 
    
def writelog(msg,output_log):
    output_log.write("\n"+msg)



   