# ChangeLogs

# Chaining script:
00 01 * * 1,2,3,4,5,6 cd /opt/application/compliance && /usr/local/bin/python3.5 Scheduler.py --weekofmonth 3 --dayofweek 1 && cd /opt/application/compliance_double_currency && /usr/local/bin/python3.5 _double_currency_merge.py 
00 06 * * 7 cd /opt/application/compliance && /usr/local/bin/python3.5 compliance.py --runtype delta && cd /opt/application/compliance_double_currency && /usr/local/bin/python3.5 _double_currency_merge.py    ####For delta Sunday

# GR_Activities #610004: 
1. acc_currencyiso has set to tango local currency in in_transaction and in_transaction_ext.
2. In in_customer, CUST_SPH_12, CUST_SPH_13, CUST_SPH_14, CUST_SPH_15 and CUST_SPH_16 columns are set to null.

# GR_Activities #610419:
1. In In_Customer_ext_init, CUST_SPH_23, CUST_SPH_24, CUST_SPH_25, CUST_SPH_26, CUST_NPH_01 et CUST_NPH_02 columnes set to null

# New criteria compliance package
  New way of scheduling compliance task with Scheduler.py using: 
  "cd /home/osadmin/compliance && /usr/local/bin/python3.5 Scheduler.py --weekofmonth 2 --dayofweek 1"
  dayofweek starts from Monday

# Latest version of sql used:
IN_CUSTOMER-1.1.9.sql
IN_CUSTOMER_EXT-1.1.5.sql
IN_ACCOUNT-1.1.6.sql
IN_ACCOUNT_EXT-1.1.4.sql
IN_TRANSACTION-1.2.0.sql
IN_TRANSACTION_EXT-1.1.0.sql

# Few parameters in config are set to empty, will be filled as per country:
 AFFILIATENAME
 INSTITUTE
 DBHOSTNAME
 DBUSERNAME
 DBPASSWORD
 EMEBASEPATH
 KPIBASEPATH
 PP_EMEBASEPATH
 PP_KPIBASEPATH
 TS_STARTD
 Delimiter like XOF