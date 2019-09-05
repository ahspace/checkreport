"""
For creating the monthly channel subscrier full export
It will read grtool configuration, logging configuration
"""
# -*- coding: utf-8 -*-
__author__ = "NFPV2962"
__version__ = "0.2"

import sys
import os
import logging
import logging_ini
import csv
import time
import json
from Oracle import *
import argparse
import datetime
import traceback
import shutil
import hashlib
from utils import Encryption
from collections import OrderedDict
import locale
import codecs
import traceback
#####logging Section#############
log = logging.getLogger("compliance Export")
lag_value = ""
lag_check_time = ""
reconcileData = {}

#####Argument parsing############
def parseArg():
    """
    Read command line argument runtype
    Throws error if the choice is not correct
    """
    try:
        parser = argparse.ArgumentParser(
            description='This is the complaince export utility')
        choices = ['delta', 'deltamonth', 'full', 'regen','fullactive']
        parser.add_argument('--runtype', action='store', dest='runtype',
                            choices=choices, required=True, help='select the right runtype')
        parser.add_argument('--strtdt', default=-1, action='store', dest='strtdt', help='last tun date in DD/MM/YYYY')
        args = parser.parse_args()
        global runtype
        global strtdt
        runtype = args.runtype
        strtdt = args.strtdt
        strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
        log.info("1;EME;RUNNING;000;Complaince.py;;;;;STARTING " + os.path.basename(__file__) +
                 " in " + runtype + " mode")
        readConfig()
        end_time = time.strftime("%d/%m/%Y %H:%M:%S")
        elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
        log.info("1;EME;SUCCESS;200;;;;;"+"ExecTime "+str(elapsed_time)+"(s); "+ os.path.basename(__file__) +
                 " in " + runtype + " mode")

    except Exception as e:
        log.exception("1;EME;FAILURE;700;STARTUP ERROR " + str(e), exc_info=False)
        sys.exit(0)


#####Config Section##############
def readConfig():
    """
    Reads the json configuration file for the various db and app parameters
    """
    try:
        log.debug("1;EME;RUNNING;000;Complaince.py;Setting Configuration")
        #with open('config.json') as config_file:
        config = json.load(open('config.json'),object_pairs_hook=OrderedDict)
        log.debug("1;EME;SUCCESS;200;Setting Configuration")
        os.environ['NLS_LANG']='FRENCH_FRANCE.UTF8'
        locale.setlocale(locale.LC_ALL, 'fr_FR')
        connectDB(config)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;CONFIG ERROR " + str(e), exc_info=False)
        sys.exit(0)


#####Database Section#############
def connectDB(config):
    """
    Create an oracle instance of the Oracle class
    It is used for executing the sql queries
    """
    try:
        log.debug("1;EME;RUNNING;000;Complaince.py;Setting Database")
        config['genparams']['DBPASSWORD']=Encryption().decrypt(config['genparams']['DBPASSWORD'])
        global oracle
        log.debug("1;EME;RUNNING;000;Complaince.py;Connecting Database")
        oracle = Oracle()
        log.debug("1;EME;RUNNING;000;Compliance.py;Oracle Instance initiated")
        oracle.connect(config['genparams']['DBUSERNAME'], config['genparams']['DBPASSWORD'],
                       config['genparams']['DBHOSTNAME'], config['genparams']['DBPORT'], config['genparams']['DBSID'])
        log.debug("1;EME;SUCCESS;200;Setting Database")
        checkPrerequisites(config)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;DATABASE ERROR " + str(e), exc_info=False)
        sys.exit(0)
    finally:
        oracle.disconnect()


#####Common Functions############
# def check_dbref_temp_tables(data):
#     """
#     Checks the DBREF tables for data refresh
#     """
#     log.info("1;EME;RUNNING;000;Complaince.py;;;StardDate="+TS_STARTD+";EndDate="+TS_CURD+";Checking DBRef tables")
#     is_refreshed = 'KO'
#     retry_nbr = 0
#     strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
#     try:
#         #check the SKIP_FRESHNESS_MODE parameter and skip the execution if 'Y'
#         if (data['genparams']['SKIP_FRESHNESS_MODE'] == 'Y'):
#             log.info("1;EME;RUNNING;000;Compliance.py;;;;;SKIP_FRESHNESS_MODE:" +
#                      data['genparams']['SKIP_FRESHNESS_MODE'])
#             log.info("1;EME;SUCCESS;200;;;;;;Checking DBRef tables")
#             runextraction(data)
#         else:
#             for table in data['dbreftablesparams']:
#                 log.info("1;EME;RUNNING;000;Compliance.py;;;;;Checking if the data is refreshed in " + table['TABLE'])
#                 for retry_nbr in range(1, int(table['NBRETRY']) + 1):
#                     query_chk_dbref_temp_table = open(
#                     'sqls/CHECK_DBREF_TEMP_TABLE.sql').read()
#                     param = {'TABLENAME': table['TABLE']}
#                     result = oracle.execute(query_chk_dbref_temp_table, param)
#                     is_refreshed = result.fetchone()[0]
#                     log.info("1;EME;RUNNING;000;Compliance.py;Table:" + table['TABLE'] +
#                      " is_refreshed:" + is_refreshed)
#                     if is_refreshed == 'OK':
#                         break
#                     elif retry_nbr == int(table['NBRETRY']):
#                         raise Exception("Number of retries exhaused")
#                     else:
#                         log.info("1;EME;RUNNING;000;Compliance.py;;;;;Going to Retry after " + table['TEMPO'])
#                         log.info("1;EME;RUNNING;000;Compliance.py;;;;;try:" + str(retry_nbr))
#                         time.sleep(int(table['TEMPO']))
#             if is_refreshed == 'OK':  # change here for debug
#                 end_time = time.strftime("%d/%m/%Y %H:%M:%S")
#                 elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
#                 log.info("1;EME;SUCCESS;200;;;StardDate="+TS_STARTD+";EndDate="+TS_CURD+";ExecTime "+str(elapsed_time)+"(s); Checking Dbref tables")
#                 runextraction(data)
#     except Exception as e:
#         log.exception("1;EME;FAILURE;700;RETRY ERROR " + str(e), exc_info=False)
#         sys.exit(0)
 
def checkPrerequisites(data):
    """
    Checks the prerequisite for the extraction
    Checks for any dbref sync lag and if found it retries after the specified time
    as per the configuration file
    """
    global runparams
    global TS_CURD
    global TS_STARTD
    global TS_STARTD_YYYYMMDD
    global lag_value
    global lag_check_time
    runparams = data['genparams'][runtype + 'parameters']
    TS_CURD = datetime.datetime.today().strftime('%d/%m/%Y') if runparams['TS_CURD'] == "" else runparams['TS_CURD']
    TS_PREVD = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    if (runtype == 'fullactive' or runtype == 'delta'):
      if (strtdt != -1):
         TS_STARTD = strtdt
      else:
         TS_STARTD = (datetime.datetime.strptime(TS_CURD,'%d/%m/%Y') - datetime.timedelta(days=1)).strftime('%d/%m/%Y')
    else:
      if (strtdt != -1):
         TS_STARTD = strtdt
      else:
         TS_STARTD = runparams['TS_STARTD']

    '''
    if (runtype.upper() == 'REGEN'):
        TS_STARTD_YYYYMMDD = runparams['TS_STARTD'][6:] + \
        runparams['TS_STARTD'][3:5] + runparams['TS_STARTD'][:2]
    else:
        TS_STARTD_YYYYMMDD = TS_PREVD
    '''
    TS_STARTD_YYYYMMDD = TS_STARTD[6:]+TS_STARTD[3:5] + TS_STARTD[:2]
    try:
        lag = 'DELAY'
        if (data['genparams']['DB_TYPE_TANGO'] == 'Y'):
            log.info("1;EME;RUNNING;000;Compliance.py;DB_TYPE_TANGO:" +
                     data['genparams']['DB_TYPE_TANGO'])
            log.info("1;EME;RUNNING;000;Compliance.py;Skipping checking prerequisite")
#            check_dbref_temp_tables(data)
            runextraction(data)
        else:
            retry_nbr = 0
            query_lag = open('sqls/CHECK_DBREF_APPLY_LAG.sql').read()
            for retry_nbr in range(1, int(data['genparams']['NBRETRY']) + 2):
                result = oracle.execute(query_lag, {})
                lag_tupple = result.fetchone()
                lag = lag_tupple[0]
                lag_value = lag_tupple[1]
                strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
                lag_check_time = strt_time
                log.info("1;EME;RUNNING;000;Compliance.py;;;StardDate="+TS_STARTD+";EndDate="+TS_CURD+";Running:CHECK_DBREF_APPLY_LAG.sql for checking DB LAG")
                log.info("1;EME;RUNNING;000;Compliance.py;;;;;Try: " + str(retry_nbr))
                log.info("1;EME;RUNNING;000;Compliance.py;;;;;DBRef lag: " + lag)
                #if lag == 'DELAY':  #done for debug purpose
                if lag == 'OK':
                    end_time = time.strftime("%d/%m/%Y %H:%M:%S")
                    elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
                    log.info("1;EME;SUCCESS;200;;;StardDate="+TS_STARTD+";EndDate="+TS_CURD+";ExecTime "+str(elapsed_time)+"(s); checking prerequisites")
                    break
                else:
                    log.info("1;EME;RUNNING;000;Compliance.py;Going to retry after " +
                             data['genparams']['TEMPO'] + " secs")
                    time.sleep(int(data['genparams']['TEMPO']))
            if(retry_nbr == int(data['genparams']['NBRETRY']) + 1):
                raise Exception("Number of retries exhaused")
        #if lag == 'DELAY':   #done for debugging
        if lag == 'OK':   #done for debugging
        #     check_dbref_temp_tables(data)
              runextraction(data)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;RETRY ERROR " + str(e), exc_info=False)


def runextraction(data):
    """
    Main extraction logic to create the various EME exports
    Also creates EME KPI report
    """
    log.info("1;EME;RUNNING;000;Complaince.py;;;;StardDate="+TS_STARTD+";EndDate="+TS_CURD+";Running Extraction")
    EME_DIR = data['genparams']['EMEBASEPATH']+runparams['OUT_DIR'] + data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD
    params = {'INSTITUTE': data['genparams']['INSTITUTE'], 'TYPOFFILE': runtype.upper(
    ), 'TS_STARTD': TS_STARTD, 'TS_CURD': TS_CURD, 'MSISDN': runparams['MSISDN']}
    begin_time = time.strftime("%d/%m/%Y %H:%M:%S")
    log.info("1;EME;RUNNING;000;Compliance.py;PARAMETER:" + str(params))
    try:
        for index, record in enumerate(data['sqlparams']):
            index = str(index+1)
            if runtype in ['full', 'deltamonth'] and 'in_customer_extension_hist.txt' in record['outputfile']:
                origsqlfile = record['sqlfile']
                record['sqlfile'] = record['sqlfilefull']
            realparams = record['bindvars']
            if not os.path.exists(EME_DIR):
                os.makedirs(EME_DIR)
            with codecs.open((EME_DIR + '/' + record['outputfile']), 'w+', 'utf-8') as outputfile:
                with open('sqls/' + record['sqlfile']) as sql_file:
                    log.info(index+";EME;RUNNING;000;Compliance.py;"+EME_DIR+";"+record['outputfile']+";StardDate="+TS_STARTD+";EndDate="+TS_CURD+";Running:"+record['sqlfile'])
                    strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
                    sql = sql_file.read()
                    outputset = oracle.execute(sql, params, realparams)
                    for row in outputset:
                        outputfile.write(''.join(str(s) for s in row) + '\n')
                    end_time = time.strftime("%d/%m/%Y %H:%M:%S")
                    elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
                    log.info(index+";EME;SUCCESS;200;"+EME_DIR+";"+record['outputfile']+";StartDate="+TS_STARTD+";ENDDate="+TS_CURD+";ExecTime "+str(elapsed_time)+"(s);")
            if runtype in ['full', 'deltamonth'] and 'in_customer_extension_hist.txt' in record['outputfile']:
                record['sqlfile'] = origsqlfile 
        end_time = time.strftime("%d/%m/%Y %H:%M:%S")
        elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(begin_time,"%d/%m/%Y %H:%M:%S"))
        log.info("1;EME;SUCCESS;200;;;StardDate="+TS_STARTD+";EndDate="+TS_CURD+";ExecTime "+str(elapsed_time)+"(s); Running extraction")
        #runkpi(data, ';')
        compressreports(data)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;EXTRACTION ERROR" + str(e), exc_info=False)
        sys.exit(0)

def compressreports(data):
    """
    It compresses the folder with .zip extension
    """
    ZIP_DIR=data['genparams']['EMEBASEPATH']+runparams['OUT_DIR']
    ZIP_FILENAME = data['genparams']['INSTITUTE'] + "_" + TS_STARTD_YYYYMMDD
    log.info("1;EME;RUNNING;000;Complaince.py;"+ZIP_DIR+";"+ZIP_FILENAME+".zip;;;Running compression")
    strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
    try:
        shutil.make_archive(ZIP_DIR + ZIP_FILENAME,'zip', ZIP_DIR+ZIP_FILENAME)
        end_time = time.strftime("%d/%m/%Y %H:%M:%S")
        elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
        log.info("1;EME;SUCCESS;200;"+ZIP_DIR+";"+ZIP_FILENAME+".zip;;;ExecTime "+str(elapsed_time)+"(s); Running compression")
        md5checksum(data)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;ZIP ERROR" + str(e), exc_info=False)
        sys.exit(0)

# def compresskpi(data):
#     """
#     It compresses the folder with .zip extension
#     """
#     ZIP_DIR=data['genparams']['KPIBASEPATH']+runparams['OUT_DIR']
#     ZIP_FILENAME = data['genparams']['INSTITUTE'] + "_" + TS_STARTD_YYYYMMDD
#     log.info("1;EME;RUNNING;000;Complaince.py;"+ZIP_DIR+";"+ZIP_FILENAME+".zip;;;Running compression")
#     strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
#     try:
#         shutil.make_archive(ZIP_DIR + ZIP_FILENAME,'zip', ZIP_DIR+ZIP_FILENAME)
#         end_time = time.strftime("%d/%m/%Y %H:%M:%S")
#         elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
#         shutil.rmtree(ZIP_DIR+ZIP_FILENAME)
#         log.info("1;EME;SUCCESS;200;"+ZIP_DIR+";"+ZIP_FILENAME+".zip;;;ExecTime "+str(elapsed_time)+"(s); Running compression")
#     except Exception as e:
#         log.exception("1;EME;FAILURE;700;ZIP ERROR" + str(e), exc_info=False)
#         sys.exit(0)



def md5checksum(data):
    """
    MD5 hashing is done to avoid any tampereing in the network
    """
    MD5_DIR=data['genparams']['EMEBASEPATH']+runparams['OUT_DIR']
    MD5_FILENAME=data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD + '.zip.md5'
    log.info("1;EME;RUNNING;000;Complaince.py;"+MD5_DIR+";"+MD5_FILENAME+";;;Running md5checksum")
    strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
    try:
        with open((MD5_DIR+MD5_FILENAME), 'w+') as md5_file:
            filehash = hashlib.md5()
            filehash.update(open(MD5_DIR + data['genparams']
                                 ['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD + '.zip', 'rb').read())
            md5_file.write(filehash.hexdigest())
        end_time = time.strftime("%d/%m/%Y %H:%M:%S")
        elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))        
        log.info("1;EME;SUCCESS;200;"+MD5_DIR+";"+MD5_FILENAME+";;;ExecTime "+str(elapsed_time)+"(s); Running md5checksum")
        cleanup(data)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;MD5 ERROR" + str(e), exc_info=False)
        sys.exit(0)

def cptopp(data):
    """
    Copy EME Exports zip, KPI, MD5 to PP server 
    """
    try:
        log.info("1;EME;RUNNING;000;Complaince.py;;;;;Copying files to PP")
        strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
        ZIP_DIR=data['genparams']['EMEBASEPATH']+runparams['OUT_DIR']
        ZIP_FILENAME = data['genparams']['INSTITUTE'] + "_" + TS_STARTD_YYYYMMDD
        MD5_DIR=data['genparams']['EMEBASEPATH']+runparams['OUT_DIR']
        MD5_FILENAME=data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD + '.zip.md5'
        KPI_DIR = data['genparams']['KPIBASEPATH']+runparams['KPI_DIR'] + data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD
        KPI_FILENAME = data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD + '_Control.csv'
        PP_EME_DIR = data['genparams']['PP_EMEBASEPATH']+runparams['OUT_DIR']
        PP_KPI_DIR = data['genparams']['PP_KPIBASEPATH']+runparams['KPI_DIR']
        if not os.path.exists(PP_EME_DIR):
            os.makedirs(PP_EME_DIR)
        if not os.path.exists(PP_KPI_DIR):
            os.makedirs(PP_KPI_DIR)
        shutil.copy(ZIP_DIR+ZIP_FILENAME+'.zip',PP_EME_DIR+ZIP_FILENAME)
        shutil.copy(MD5_DIR+MD5_FILENAME,PP_EME_DIR+MD5_FILENAME)
        shutil.copy(KPI_DIR+KPI_FILENAME,PP_KPI_DIR+KPI_FILENAME)     
        end_time = time.strftime("%d/%m/%Y %H:%M:%S")
        elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
        log.info("1;EME;SUCCESS;200;;;;;ExecTime "+str(elapsed_time)+"(s); Copying files to PP")
    except Exception as e:
        log.exception("1;EME;FAILURE;700;COPY ERROR" + str(e), exc_info=False)
        sys.exit(0)

def cleanup(data):
    """
    Removes the uncompressed folder and updates the json for the last run date
    """
    try:
        log.info("1;EME;RUNNING;000;Complaince.py;;;;;running cleanup")
        log.info("1;EME;RUNNING;000;Compliance.py;;;;;deleting directory "+runparams['OUT_DIR'] + data['genparams']
                      ['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD)
        shutil.rmtree(data['genparams']['EMEBASEPATH']+runparams['OUT_DIR'] + data['genparams']
                      ['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD)
        log.info("1;EME;RUNNING;000;Compliance.py;;;;;updating configuration TS_STARTD with "+TS_CURD)
        with open('config.json', 'w') as f:
            #if runtype in ['delta', 'deltamonth']:
            data['genparams'][runtype + 'parameters']['TS_STARTD'] = TS_CURD
            f.write(json.dumps(data, indent=4))
        log.info("1;EME;SUCCESS;200;;;;;;running cleanup")
        if data['genparams']['DOUBLE_RUN_PRO_PP']=='Y':
            cptopp(data)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;CLEANUP ERROR" + str(e), exc_info=False)
        sys.exit(0)

#def reconciliation(data):
#    """
#    It is responsible to create control files
#    """
#    global reconcileData
#    i = 0
#    KPI_DIR = data['genparams']['KPIBASEPATH']+data['genparams'][runtype + 'parameters']['KPI_DIR'] + data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD
#    params = {'TYPOFFILE': runtype.upper(), 'TS_STARTD': TS_STARTD, 'TS_CURD': TS_CURD, 'MSISDN': runparams['MSISDN'], 'TIMESTP' : ""}
#    try:
#        log.info("1;EME;RUNNING;200;Complaince.py;;;;;runing reconciliation")
#        for key in reconcileData:
#            controlFileName = data['genparams']['INSTITUTE']+"_"+TS_STARTD_YYYYMMDD+"_"+key.replace(".txt",".csv")
#            with codecs.open(KPI_DIR+'/'+controlFileName, 'w+','utf-8') as f:
#                for reconcile_param in data['reconcilesql_params']:
#                    if reconcile_param['identifier'] == key:
#                        i = i + 1
#                        with open('sqls/reconciliation/' + reconcile_param['sqlfile']) as reconcile_sql:
#                            log.info(str(i)+";EME;RUNNING;000;Compliance.py;"+KPI_DIR+";"+controlFileName+";StardDate="+TS_STARTD+";EndDate="+TS_CURD)
#                            strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
#                            sql = reconcile_sql.read()
#                            params['TIMESTP'] = reconcile_param['executiontime']
#                            outputset = oracle.execute(sql, params, reconcile_param['bindvars'])
#                            f.write('Date de Génération du rapport;'+reconcile_param['executiontime'] +'\n')
#                            f.write('données du'+ TS_STARTD +'\n')
#                            f.write('Pays;'+data['genparams']['AFFILIATENAME'] +'\n')
#                            f.write('\n')
#                            f.write('start date:;'+TS_STARTD +'\n')
#                            f.write('end date:;'+TS_CURD +'\n')
#                            f.write('extraction mode:;'+runtype.upper() +'\n')
#                            f.write('file name:;'+key +'\n')
#                            listColnames = [list(i[0]) for i in outputset.description]
#                            temp = ""
#                            for colName in listColnames:
#                                finalColName = ''.join(colName)
#                                temp = temp + finalColName + ';'
#                            f.write(temp[:-1] + '\n')
#                            rowList = [list(row) for row in outputset]
#                            for row in rowList:
#                                temp = ""
#                                for col in row:
#                                    temp = temp + str(col) + ';'
#                                f.write(temp[:-1] + '\n')
#                            end_time = time.strftime("%d/%m/%Y %H:%M:%S")
#                            elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
#                        log.info(str(i)+";EME;SUCCESS;200;"+KPI_DIR+';'+controlFileName+";StartDate="+TS_STARTD+";ENDDate="+TS_CURD+";ExecTime "+str(elapsed_time)+"(s);")
#        compresskpi(data)
#    except Exception as e:
#        log.exception("1;EME;FAILURE;700;RECONCILIATION ERROR " + str(e), exc_info=False)
#        sys.exit(0)

def getlag():
         log.info("1;EME;RUNNING;000;Compliance.py;Getting lag info between tango and dbref DB")
         begin_time = time.strftime("%d/%m/%Y %H:%M:%S")
         query_dbref_checklag = open('sqls/CHECK_DBREF_APPLY_LAG.sql').read()
         result = oracle.execute(query_dbref_checklag, {})
         checklagtime = result.fetchone()[1]
         end_time = time.strftime("%d/%m/%Y %H:%M:%S")
         elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(begin_time,"%d/%m/%Y %H:%M:%S"))
         #log.info("1;EME;RUNNING;000;Compliance.py;Tango account count:"+tango_in_customer_count+";ExecTime "+str(elapsed_time)+"(s);")
         return str(checklagtime)


# def runkpi(data, sep):
#     """
#     Creates the EME KPI report for the various stats in the extraction
#     """
#     overallstatus = "TRUE"
#     KPI_DIR = data['genparams']['KPIBASEPATH']+data['genparams'][runtype + 'parameters']['KPI_DIR'] + data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD
#     KPI_FILENAME = data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD + '_Control.csv'
#     EME_DIR = data['genparams']['EMEBASEPATH']+runparams['OUT_DIR'] + data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD
#     log.info("1;EME;RUNNING;000;Compliance.py;"+KPI_DIR+";"+KPI_FILENAME+";;;Running EME KPI")
#     strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
#     global reconcileData
#     try:
#         countallTango(data)
#         if not os.path.exists(KPI_DIR):
#             os.makedirs(KPI_DIR)
#         with codecs.open((KPI_DIR + '/' + KPI_FILENAME), 'w+', 'utf-8') as kpi_file:
#             kpi_file.write("Date de Génération du rapport" + sep +
#                            datetime.datetime.today().strftime('%d-%m-%Y %H:%M:%S') + '\n')
#             kpi_file.write("données du :" + sep + runparams['TS_STARTD'].replace('/','-') + '\n')
#             kpi_file.write(
#                 "Pays" + sep + data['genparams']['AFFILIATENAME'] + '\n')
#             kpi_file.write('\n')
#             kpi_file.write("Contrôle" + sep + "Date" + sep + "Valeur" + '\n')
#             kpi_file.write("Synchronization DBREF" + sep + lag_check_time + sep + lag_value + '\n')
#             kpi_file.write('\n')
#             kpi_file.write("Extraction"+sep+"Date"+sep+"Taille fichier"+sep+"Tango content"+sep+"Nombre d'enregistrements (Fichier)"+sep+"Nombre distincts (Fichier)"+sep+"Montant cumulé des Transactions (Tango)"+sep+"Montant cumulé des Transactions (Fichier)"+sep+"Result OK/KO"+sep+"Error message"+'\n')
#             for record in data['sqlparams']:
#                 kpi_file.write(record['outputfile'] + sep )
#                 kpi_file.write(time.strftime('%d-%m-%Y %H:%M:%S', time.gmtime(os.path.getmtime(EME_DIR +'/'+ record['outputfile']))) + sep)
#                 kpi_file.write(str(os.path.getsize(EME_DIR +'/'+ record['outputfile'])) + sep )
#                 kpi_file.write(str(record['tangocount'])+ sep )
#                 fileallcount = countall(data, record['outputfile'])
#                 filetransactionsum = 0
#                 kpi_file.write(fileallcount + sep )
#                 if (len(record['rowparam']) > 0):
#                    kpi_file.write(str(countdistinct(data, record['outputfile'], record['delimiter'], record['rowparam'])) + sep )
#                 else:
#                     kpi_file.write(sep)
#                 if (record['outputfile'] == 'in_transaction.txt'):
#                    kpi_file.write(str(record['tangosum']) + sep );
#                    filetransactionsum = sumtransaction(data, record['outputfile'],record['rowparam'])
#                    kpi_file.write(filetransactionsum + sep )
#                 else:
#                    kpi_file.write(sep)
#                    kpi_file.write(sep)
#                 tangodifffilecount = record['tangocount'] - int(fileallcount)
#                 """Gathering data for reconciliation"""
#                 if(record['outputfile'] in ('in_transaction.txt', 'in_account.txt', 'in_customer.txt')):
#                     if(tangodifffilecount != 0):
#                         reconcileData[record['outputfile']] = tangodifffilecount
#
#                 if (record['outputfile'] != 'in_transaction.txt'):
#                    if (tangodifffilecount == 0):
#                       kpi_file.write("OK" + sep )
#                       kpi_file.write(sep)
#                    else:
#                       kpi_file.write("KO" + sep )
#                       kpi_file.write('Fichier '+'"'+record['outputfile']+'"'+' : '+ str(tangodifffilecount) +' lignes manquantes' + sep )
#                       overallstatus = "FALSE"
#                 else:
#                    tangodifffilesum = record['tangosum'] - int(filetransactionsum)
#                    if (tangodifffilesum == 0):
#                       kpi_file.write("OK" + sep )
#                       kpi_file.write(sep)
#                    else:
#                       kpi_file.write("KO" + sep )
#                       kpi_file.write('Fichier '+'"'+record['outputfile']+'"'+' : '+ str(tangodifffilecount) +' ligne manquante, cumuls des montants différents' + sep )
#                       overallstatus = "FALSE"
#                 kpi_file.write('\n')
#
#         compressreports(data)
#         with open((KPI_DIR + '/' + KPI_FILENAME), 'a') as kpi_file:
#             #Writing last 3 lines of file
#             zipfilefullpath=data['genparams']['EMEBASEPATH']+runparams['OUT_DIR']+'/'+data['genparams']['INSTITUTE']+"_"+TS_STARTD_YYYYMMDD+'.zip'
#             kpi_file.write(data['genparams']['INSTITUTE'] + "_" + TS_STARTD_YYYYMMDD + ".zip" + sep +
#                                 time.strftime('%d/%m/%Y %H:%M:%S',time.gmtime(os.path.getmtime(zipfilefullpath))).replace('/','-') + sep +
#                                 str(os.path.getsize(zipfilefullpath)) + '\n')
#             md5filefullpath=data['genparams']['EMEBASEPATH']+runparams['OUT_DIR']+'/'+data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD + '.zip.md5'
#             kpi_file.write(data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD + '.zip.md5' + sep +
#                                 time.strftime('%d/%m/%Y %H:%M:%S',time.gmtime(os.path.getmtime(md5filefullpath))).replace('/','-') + sep +
#                                 str(os.path.getsize(md5filefullpath)) + '\n')
#             kpi_file.write('\n')
#             if(overallstatus == "TRUE"):
#                 overallfilestatus = "OK"
#             else:
#                 overallfilestatus = "KO"
#             kpi_file.write("Contrôle global" + sep + time.strftime("%d-%m-%Y %H:%M:%S") + sep + overallfilestatus +'\n')
#
#         end_time = time.strftime("%d/%m/%Y %H:%M:%S")
#         elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
#         log.info("1;EME;SUCCESS;200;"+KPI_DIR+";"+KPI_FILENAME+";;;ExecTime "+str(elapsed_time)+"(s);")
#         cleanup(data)
#         #reconciliation(data)
#         compresskpi(data)
#     except Exception as e:
#         log.exception("1;EME;FAILURE;700;KPI ERROR" + str(e), exc_info=True)
#         sys.exit(0)


# def countall(data, filename):
#     with open(data['genparams']['EMEBASEPATH']+runparams['OUT_DIR'] + data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD + '/' + filename) as f:
#         return str(sum(1 for _ in f))
#
#
# def countdistinct(data, filename, delimiter, rowparam):
#     with open(data['genparams']['EMEBASEPATH']+runparams['OUT_DIR'] + data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD + '/' + filename) as f:
#         distinct = 0
#         lastrowval = ''
#         rowparam = [int(i) for i in rowparam]
#         for index, line in enumerate(f):
#             if filename in ['in_transaction.txt','in_transaction_extension_hist.txt']:
#                 currrowval = line[57:79]
#             else:
#                 currrowval = line.split(delimiter)[rowparam[0]][rowparam[1]:rowparam[1] + rowparam[2]+1]
#
#             if lastrowval.strip() != currrowval.strip():
#                 distinct += 1
#                 lastrowval = currrowval
#         return str(distinct)
#
#
# def sumtransaction(data, filename, rowparam):
#     with open(data['genparams']['EMEBASEPATH']+runparams['OUT_DIR'] + data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD + '/' + filename) as f:
#         sum = 0
#         lasttrx = ''
#         rowparam = [int(i) for i in rowparam]
#         for line in f:
#           currtrx = line[57:79]
#           if lasttrx != currtrx:
#             sum += int(line[95:108])
#         return str(sum)
#
# def countallTango(data):
#     try:
#          params = {'TYPOFFILE': runtype.upper(), 'TS_STARTD': TS_STARTD, 'TS_CURD': TS_CURD, 'MSISDN': runparams['MSISDN']}
#          log.info("1;EME;RUNNING;000;Complaince.py;;;;StardDate="+TS_STARTD+";EndDate="+TS_CURD+";Running Tango Queries for Reconciliation")
#          log.info("1;EME;RUNNING;000;Compliance.py;Fetching tango account count;PARAMETER:" + str(params))
#          begin_time_acc = time.strftime("%d/%m/%Y %H:%M:%S")
#          query_tango_in_account_count = open('sqls/TANGO_IN_ACCOUNT-0.0.1.sql').read()
#          result = oracle.execute(query_tango_in_account_count, params)
#          tango_in_account_count = result.fetchone()[0]
#          end_time = time.strftime("%d/%m/%Y %H:%M:%S")
#          elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(begin_time_acc,"%d/%m/%Y %H:%M:%S"))
#          log.info("1;EME;RUNNING;000;Compliance.py;Tango account count:"+str(tango_in_account_count)+";ExecTime "+str(elapsed_time)+"(s);")
#
#          params = {'TYPOFFILE': runtype.upper(),'TS_STARTD': TS_STARTD, 'TS_CURD': TS_CURD, 'MSISDN': runparams['MSISDN']}
#          log.info("1;EME;RUNNING;000;Compliance.py;Fetching tango customer count;PARAMETER:" + str(params))
#          begin_time_cus = time.strftime("%d/%m/%Y %H:%M:%S")
#          query_tango_in_customer_count = open('sqls/TANGO_IN_CUSTOMER-0.0.1.sql').read()
#          result = oracle.execute(query_tango_in_customer_count, params)
#          tango_in_customer_count = result.fetchone()[0]
#          end_time = time.strftime("%d/%m/%Y %H:%M:%S")
#          elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(begin_time_cus,"%d/%m/%Y %H:%M:%S"))
#          log.info("1;EME;RUNNING;000;Compliance.py;Tango customer count:"+str(tango_in_customer_count)+";ExecTime "+str(elapsed_time)+"(s);")
#
#
#          params = {}
#          log.info("1;EME;RUNNING;000;Compliance.py;Fetching tango country count;PARAMETER:" + str(params))
#          begin_time = time.strftime("%d/%m/%Y %H:%M:%S")
#          query_tango_in_country_count = open('sqls/TANGO_IN_COUNTRY-0.0.1.sql').read()
#          result = oracle.execute(query_tango_in_country_count, params)
#          tango_in_country_count = result.fetchone()[0]
#          end_time = time.strftime("%d/%m/%Y %H:%M:%S")
#          elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(begin_time,"%d/%m/%Y %H:%M:%S"))
#          log.info("1;EME;RUNNING;000;Compliance.py;Tango country count:"+str(tango_in_country_count)+";ExecTime "+str(elapsed_time)+"(s);")
#
#
#          params = {'TS_CURD': TS_CURD, 'MSISDN': runparams['MSISDN']}
#          log.info("1;EME;RUNNING;000;Compliance.py;Fetching tango transaction sum;PARAMETER:" + str(params))
#          begin_time = time.strftime("%d/%m/%Y %H:%M:%S")
#          query_tango_in_transaction_sum = open('sqls/TANGO_IN_TRANSACTION_SUM-0.0.1.sql').read()
#          result = oracle.execute(query_tango_in_transaction_sum, params)
#          tango_in_transaction_sum = result.fetchone()[0]
#          end_time = time.strftime("%d/%m/%Y %H:%M:%S")
#          elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(begin_time,"%d/%m/%Y %H:%M:%S"))
#          log.info("1;EME;RUNNING;000;Compliance.py;Tango transaction sum:"+str(tango_in_transaction_sum)+";ExecTime "+str(elapsed_time)+"(s);")
#
#
#          params = {'TS_CURD': TS_CURD, 'MSISDN': runparams['MSISDN']}
#          log.info("1;EME;RUNNING;000;Compliance.py;Fetching tango transaction count;PARAMETER:" + str(params))
#          begin_time_txn = time.strftime("%d/%m/%Y %H:%M:%S")
#          query_tango_in_transaction_count = open('sqls/TANGO_IN_TRANSACTION-0.0.1.sql').read()
#          result = oracle.execute(query_tango_in_transaction_count, params)
#          tango_in_transaction_count = result.fetchone()[0]
#          end_time = time.strftime("%d/%m/%Y %H:%M:%S")
#          elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(begin_time_txn,"%d/%m/%Y %H:%M:%S"))
#          log.info("1;EME;RUNNING;000;Compliance.py;Tango transaction count:"+str(tango_in_transaction_count)+";ExecTime "+str(elapsed_time)+"(s);")
#
#          with open('config.json', 'w') as f:
#               record = data['sqlparams']
#               record[0]['tangocount'] = tango_in_country_count
#               record[1]['tangocount'] = tango_in_customer_count
#               record[2]['tangocount'] = tango_in_customer_count
#               record[3]['tangocount'] = tango_in_account_count
#               record[4]['tangocount'] = tango_in_account_count
#               record[5]['tangocount'] = tango_in_transaction_count
#               record[5]['tangosum'] = tango_in_transaction_sum
#               record[6]['tangocount'] = tango_in_transaction_count
              #reconcilesql_record = data["reconcilesql_params"]
              #reconcilesql_record[0]['executiontime'] = begin_time_txn
              #reconcilesql_record[1]['executiontime'] = begin_time_acc
              #reconcilesql_record[2]['executiontime'] = begin_time_cus
    #           data['genparams']['DBPASSWORD']=Encryption().encrypt(data['genparams']['DBPASSWORD']).decode('utf-8')
    #           f.write(json.dumps(data, indent=4))
    #      log.info("1;EME;SUCCESS;200;;;;;;running countallTango")
    #      return 0
    # except Exception as e:
    #     log.exception("1;EME;FAILURE;700;TANGO QUERY ERROR" + str(e), exc_info=true)
    #     sys.exit(0)

if __name__ == '__main__':
    try:
        parseArg()
    except Exception as e:
        log.exception("1;EME;FAILURE;700;MAIN ERROR" + str(e), exc_info=False)
        sys.exit(0)

