# -*- coding: utf-8 -*-
"""
Created to schedule compliance package on delta and fullactive modes
Pass week of month and Day of week to run fullactive
DryRun: pyhton3.5 scheduler.py --weekofmonth 4 --dayofweek 2
First day of week is SATURDAY
"""
__author__ = "ZMSH2370"
__version__ = "0.1"


from datetime import date
from math import ceil
import logging
import logging_ini
import argparse
import os
import sys

#####logging Section#############
log = logging.getLogger("compliance Export")

def parseArg():
    """
    Read command line argument runtype
    Throws error if the choice is not correct
    """
    try:
        parser = argparse.ArgumentParser(
            description='This is the Scheduler export utility')
        parser.add_argument('--weekofmonth', action='store', dest='weekofmonth',
                             required=True, help='select the week of month[1-5]', type = int, choices=[1,2,3,4,5])
        parser.add_argument('--dayofweek', action='store', dest='dayofweek', help='select day of week [1-7]', type=int, choices=[1,2,3,4,5,6,7])
        args = parser.parse_args()
        global WEEK
        global DAY
        WEEK = args.weekofmonth
        DAY = args.dayofweek
        log.info("1;EME;RUNNING;000;Scheduler.py;;;;;STARTING " + os.path.basename(__file__))
        schedule(WEEK, DAY)
    except Exception as e:
        log.exception("1;EME;FAILURE;700;STARTUP ERROR " + str(e), exc_info=False)
        sys.exit(0)

def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    """
    try:
        first_day = dt.replace(day=1)
        dom = dt.day
        adjusted_dom = dom + first_day.weekday()
    
        return int(ceil(adjusted_dom/7.0))
    except Exception as e:
        log.exception("1;EME;FAILURE;700; FUNCTION ERROR " + str(e), exc_info=False)
        sys.exit(0)


def day_of_week(dt):
    """Returns customized day of week as first day
       Default first day of week is Monday
       mday used to set firstweek of day
    """
    cday = dt
    mday = 2
    uday = cday.isocalendar()[2] + mday
    try:
        if uday > 7:
            CURRDAY = uday - 7
            log.debug("1;EME;RUNNING;000;Scheduler.py;Setting customized day of week>7 : ", CURRDAY)
        else:
            CURRDAY = uday
            log.debug("1;EME;RUNNING;000;Scheduler.py;Setting customized day of week : ", CURRDAY)
        return CURRDAY
    except Exception as e:
        log.exception("1;EME;FAILURE;700;SCHEDULE ERROR " + str(e), exc_info=False)
        sys.exit(0)


def schedule(WEEK,DAY):
    try:
        log.debug("1;EME;RUNNING;000;Scheduler.py;Runtime Week of month: ", WEEK)
        log.debug("1;EME;RUNNING;000;Scheduler.py;Runtime Day of week: ", DAY)
        CURRWEEK = week_of_month(date.today())
        log.debug("1;EME;RUNNING;000;Scheduler.py;Current Week: ", CURRWEEK)
        CURRDAY = day_of_week(date.today())
        log.debug("1;EME;RUNNING;000;Scheduler.py;Current Day: ", CURRDAY)

        if (CURRWEEK == WEEK and CURRDAY == DAY):
             os.system("cd /opt/application/compliance && /usr/local/bin/python3.5 compliance.py --runtype fullactive")
           
        else:
            os.system("cd /opt/application/compliance && /usr/local/bin/python3.5 compliance.py --runtype delta")
    except Exception as e:
        log.exception("1;EME;FAILURE;700;SCHEDULE ERROR " + str(e), exc_info=False)
        sys.exit(0)
        
if __name__ == '__main__':
    try:
        parseArg()
    except Exception as e:
        log.exception("1;EME;FAILURE;700;MAIN ERROR" + str(e), exc_info=False)
        sys.exit(0)
