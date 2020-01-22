"""
For creating the monthly channel subscrier full export
It will read grtool configuration, logging configuration
"""
# -*- coding: utf-8 -*-
__author__ = "ZMSH2370"
__version__ = "2.0"

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
    global TS_CURD_YYYYMMDD
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
    TS_CURD_YYYYMMDD = TS_CURD[6:] + TS_CURD[3:5] + TS_CURD[:2]
    try:
        lag = 'DELAY'
        if (data['genparams']['DB_TYPE_TANGO'] == 'Y'):
            log.info("1;EME;RUNNING;000;Compliance.py;DB_TYPE_TANGO:" +
                     data['genparams']['DB_TYPE_TANGO'])
            log.info("1;EME;RUNNING;000;Compliance.py;Skipping checking prerequisite")
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
                log.info("1;EME;RUNNING;000;Compliance.py;;;StartDate="+TS_STARTD+";EndDate="+TS_CURD+";Running:CHECK_DBREF_APPLY_LAG.sql for checking DB LAG")
                log.info("1;EME;RUNNING;000;Compliance.py;;;;;Try: " + str(retry_nbr))
                log.info("1;EME;RUNNING;000;Compliance.py;;;;;DBRef lag: " + lag)
                if lag == 'OK':
                    end_time = time.strftime("%d/%m/%Y %H:%M:%S")
                    elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
                    log.info("1;EME;SUCCESS;200;;;StartDate="+TS_STARTD+";EndDate="+TS_CURD+";ExecTime "+str(elapsed_time)+"(s); checking prerequisites")
                    break
                else:
                    log.info("1;EME;RUNNING;000;Compliance.py;Going to retry after " +
                             data['genparams']['TEMPO'] + " secs")
                    time.sleep(int(data['genparams']['TEMPO']))
            if(retry_nbr == int(data['genparams']['NBRETRY']) + 1):
                raise Exception("Number of retries exhaused")
        if lag == 'OK':
              runextraction(data)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;RETRY ERROR " + str(e), exc_info=False)


def runextraction(data):
    """
    Main extraction logic to create the various EME exports
    Also creates EME KPI report
    """
    log.info("1;EME;RUNNING;000;Complaince.py;;;;StartDate="+TS_STARTD+";EndDate="+TS_CURD+";Running Extraction")
    if runtype in ['full']:
        EME_DIR = data['genparams']['EMEBASEPATH'] + runparams['OUT_DIR'] + data['genparams']['INSTITUTE'] + '_' + TS_CURD_YYYYMMDD
    else:
        EME_DIR = data['genparams']['EMEBASEPATH']+runparams['OUT_DIR'] + data['genparams']['INSTITUTE'] + '_' + TS_STARTD_YYYYMMDD
    params = {'INSTITUTE': data['genparams']['INSTITUTE'], 'TYPOFFILE': runtype.upper(
    ), 'TS_STARTD': TS_STARTD, 'TS_CURD': TS_CURD, 'MSISDN': runparams['MSISDN']}
    begin_time = time.strftime("%d/%m/%Y %H:%M:%S")
    log.info("1;EME;RUNNING;000;Compliance.py;PARAMETER:" + str(params))
    try:
        for index, record in enumerate(data['sqlparams']):
            index = str(index+1)
            realparams = record['bindvars']
            if not os.path.exists(EME_DIR):
                os.makedirs(EME_DIR)
            with codecs.open((EME_DIR + '/' + record['outputfile']), 'w+', 'utf-8') as outputfile:
                with open('sqls/' + record['sqlfile']) as sql_file:
                    log.info(index+";EME;RUNNING;000;Compliance.py;"+EME_DIR+";"+record['outputfile']+";StartDate="+TS_STARTD+";EndDate="+TS_CURD+";Running:"+record['sqlfile'])
                    strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
                    sql = sql_file.read()
                    outputset = oracle.execute(sql, params, realparams)
                    for row in outputset:
                        outputfile.write(''.join(str(s) for s in row) + '\n')
                    end_time = time.strftime("%d/%m/%Y %H:%M:%S")
                    elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(strt_time,"%d/%m/%Y %H:%M:%S"))
                    log.info(index+";EME;SUCCESS;200;"+EME_DIR+";"+record['outputfile']+";StartDate="+TS_STARTD+";EndDate="+TS_CURD+";ExecTime "+str(elapsed_time)+"(s);")
        end_time = time.strftime("%d/%m/%Y %H:%M:%S")
        elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(begin_time,"%d/%m/%Y %H:%M:%S"))
        log.info("1;EME;SUCCESS;200;;;StartDate="+TS_STARTD+";EndDate="+TS_CURD+";ExecTime "+str(elapsed_time)+"(s); Running extraction")
        compressreports(data)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;EXTRACTION ERROR" + str(e), exc_info=False)
        sys.exit(0)

def compressreports(data):
    """
    It compresses the folder with .zip extension
    """
    global ZIP_FILENAME
    ZIP_DIR=data['genparams']['EMEBASEPATH']+runparams['OUT_DIR']
    if runtype in ['full']:
        ZIP_FILENAME = data['genparams']['INSTITUTE'] + "_" + TS_CURD_YYYYMMDD
    else:
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

def md5checksum(data):
    """
    MD5 hashing is done to avoid any tampereing in the network
    """
    MD5_DIR=data['genparams']['EMEBASEPATH']+runparams['OUT_DIR']
    MD5_FILENAME=ZIP_FILENAME + '.zip.md5'
    log.info("1;EME;RUNNING;000;Complaince.py;"+MD5_DIR+";"+MD5_FILENAME+";;;Running md5checksum")
    strt_time = time.strftime("%d/%m/%Y %H:%M:%S")
    try:
        with open((MD5_DIR+MD5_FILENAME), 'w+') as md5_file:
            filehash = hashlib.md5()
            filehash.update(open(MD5_DIR + ZIP_FILENAME + '.zip', 'rb').read())
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
        ZIP_FILENAME_PP = ZIP_FILENAME
        MD5_DIR=data['genparams']['EMEBASEPATH']+runparams['OUT_DIR']
        MD5_FILENAME=ZIP_FILENAME_PP + '.zip.md5'
        KPI_DIR = data['genparams']['KPIBASEPATH']+runparams['KPI_DIR'] + ZIP_FILENAME_PP
        KPI_FILENAME = ZIP_FILENAME_PP + '_Control.csv'
        PP_EME_DIR = data['genparams']['PP_EMEBASEPATH']+runparams['OUT_DIR']
        PP_KPI_DIR = data['genparams']['PP_KPIBASEPATH']+runparams['KPI_DIR']
        if not os.path.exists(PP_EME_DIR):
            os.makedirs(PP_EME_DIR)
        if not os.path.exists(PP_KPI_DIR):
            os.makedirs(PP_KPI_DIR)
        shutil.copy(ZIP_DIR+ZIP_FILENAME_PP+'.zip',PP_EME_DIR+ZIP_FILENAME_PP)
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
        log.info("1;EME;RUNNING;000;Compliance.py;;;;;deleting directory "+runparams['OUT_DIR'] + ZIP_FILENAME)
        shutil.rmtree(data['genparams']['EMEBASEPATH']+runparams['OUT_DIR'] + ZIP_FILENAME)
        with open('config.json', 'w') as f:
            if runtype in ['delta', 'deltamonth','fullactive']:
                log.info("1;EME;RUNNING;000;Compliance.py;;;;;updating configuration TS_STARTD with " + TS_CURD)
                data['genparams'][runtype + 'parameters']['TS_STARTD'] = TS_CURD
            data['genparams']['DBPASSWORD'] = Encryption().encrypt(data['genparams']['DBPASSWORD']).decode('utf-8')
            f.write(json.dumps(data, indent=4))
        log.info("1;EME;SUCCESS;200;;;;;;running cleanup")
        getlag()
        if data['genparams']['DOUBLE_RUN_PRO_PP']=='Y':
            cptopp(data)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;CLEANUP ERROR" + str(e), exc_info=False)
        sys.exit(0)


def getlag():
         log.info("1;EME;RUNNING;000;Compliance.py;Getting lag info between tango and dbref DB")
         begin_time = time.strftime("%d/%m/%Y %H:%M:%S")
         query_dbref_checklag = open('sqls/CHECK_DBREF_APPLY_LAG.sql').read()
         result = oracle.execute(query_dbref_checklag, {})
         checklagtime = result.fetchone()[1]
         end_time = time.strftime("%d/%m/%Y %H:%M:%S")
         elapsed_time = time.mktime(time.strptime(end_time,"%d/%m/%Y %H:%M:%S")) - time.mktime(time.strptime(begin_time,"%d/%m/%Y %H:%M:%S"))
         log.info("1;EME;SUCCESS;200;Compliance.py; LAG :" + str(checklagtime) + "(s);")
         return str(checklagtime)

if __name__ == '__main__':
    try:
        parseArg()
    except Exception as e:
        log.exception("1;EME;FAILURE;700;MAIN ERROR" + str(e), exc_info=False)
        sys.exit(0)

