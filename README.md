# ChangeLogs
## VERSION 2.1
#### OMGR-98
1. Fix for contra_acc_currencyiso in In_transaction.sql: Earlier the preference to get value of column was from txn_mode. Now it is given to imt_country_code.
2. Management of hints/NO Hints SQL scripts: Two set of rpts are present now(Hints and without_hints) for 4 scripts(Customer, Customer_ext, Account, Account_ext). With hints as default and without_hints are only for OCD(cdf).
3. Symbolic link is been created for all 4 scripts, maintained under gitlab pipeline.
####OMGR-152
1. Excluded deleted users:  Inactive users are removed from customer and customer_ext export via fullactive mode.
2. New In_country script: Earlier 2 countries(“Guadeloupe” and “Panama” ) were excluded because of wrong country_code. Now its been corrected and included.

## VERSION 2.0
#### OMGR-8
1. Removal of the temporary tables from transactions sql: No more DBREF_TXN_DWH, DBREF_USERS_DATA temporary tables in use.
2. Removal of reconcilliation feature.
3. Remove use of delimiters.
4. Clean up extra parameters from config.json :
SKIP_FRESHNESS_MODE, KPIBASEPATH, dbreftablesparams, stats, rowparam, Versioning, reconcilesql_params.
5. Removal of checking DBREF TABLES: Tables consists DBREF_TXN_DWH, DBREF_USERS_DATA.
6. Removal of dynamic parameters from config file: tangocount, tangosum.
7. Removal of use of numericals in versioning of the transaction sql.
8. Headers and comments added in transaction script to detail the changes.
9. Use of same customer_ext sql for FULL, fullactive and delta.
10. Optimization of sqls: Creation of single script of customer_ext for all modes, Check for the execution plan and removal of full tables scans where not needed, Apply INDEX hints to the query.
11. Removal of the delimiters from config file and code for customer files.
12. Clean up parameters for customer file and SQL in config.json: "stats": [
                "Nombre d'Enregistrements ",
                "Clients distincts "
            ],"delimiter": " ","rowparam": [
                0,
                4,
                8
            ],"tangocount":
13. Change of naming for FULL export from startdate to current date.
14. Removal of use of numericals in versioning of the Customer sql.
15. Headers and comments added in Customer script to detail the changes.
16. Deployment of compliance package through GITLAB CI/CD:
a. An environment will be created on all EME servers for gitlab ci/cd.
b. A yml file is added in compliance repo which will run and work to deploy package on servers.
c. It will run manually by passing server name on which package needs to deploy.
17. Add changelog in the package.
18. Add the package version in the config file.
19. Implemention for OGN as a workaround to get acc_currencyiso as 'GBP' instead of 'GNF' in transaction and account sqls.
20. Disabled the CONTROL FILE for the time being until file get redesigned.

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