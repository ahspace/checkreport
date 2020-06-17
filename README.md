# ChangeLogs

## VERSION 2.3.3.C1

Improvements in the cloud package:

1. Instead of one, 2 dbs connection established(omreporting, Tango) for In_Account, In_Account_ext.

2. Files effected:

Compliance.py
config.json
Oracle.py

3. Removal of hints(FULL) as it degrading the performance on cloud.

4. Adaptation of local timezone.

## VERSION 2.3.3

1. Fix file renaming issue : in_transaction_extension_hist.txt renamed  to in_transaction_extension.txt 

## VERSION 2.3.2

1. Temporary fix of currency as delimiter for OCI
-	Set currency as XOF in config

2. Update hint IN_CUSTOMER_EXT suggested by AKILI to improve elapsed time
-	Update subquery mv_txn_header to add INDEX hint
-	Update subquery mv_txn_header txn to add INDEX hint
-	Remove hint /*+ INDEX(tira) INDEX(tisa)*/ on subquery mv_txn_header

## VERSION 2.3.1

1. Fix length of ACC_BALANCE in IN_ACCOUNT.sql.

2. Add HINT suggested by Akili in IN_CUSTOMER_EXT(update the in_customer_ext.sql script by adding only 1 Oracle Hint in mtx_party)


## VERSION 2.3

## Data supply supported version: V10.5
## Control file supported version: V2.0

#### OMGR-159

Changes corresponding to Marie's remarks:

In In_transaction and In_transaction_ext :
-	In the With clause, Remove mv_txn_header_as_of_date subquery.

In In_account and In_account_ext:
-	Apply currency factor to calculate the balance and wallet_maximum_balance.
- 	In the subquery “txn”, Require list of wallets which performed transaction(s). Remove commissions and service charge wallets.

Optimization corresponding to Akili's remarks:

1.	IN_CUSTOMER, IN_CUSTOMER_EXT : Adding 2 Oracle FULL HINT, on MTX_PARTY and MTX_TRANSACTION_ITEMS in the subquery mv_mtx_transaction_items.
2.	IN_ACCOUNT, IN_ACCOUNT_EXT : 

    a.  Adding 1 Oracle FULL HINT on MTX_PARTY
    
    b.  Replace INDEX hints to FULL in txn subquery.
    
3.  Modification in CHECK_LAG script:

    Modify checking of LAG between the tango and the DBREF to no longer take into account the start time of the creation of the GR tables
    
        From: to_date(substr(value,4),'HH24:MI:SS')>to_date('00:30:00','HH24:MI:SS') 
        To: to_date(substr(value,4),'HH24:MI:SS')>to_date(to_char(sysdate,'HH24:MI:SS'),'HH24:MI:SS') 
 
  
Redesign of control file:
    
    Additional features:
        Duration of the treatment
        Compliance Package Version
        End of extraction
        Translation of file into English
        
     Deleted features:
         Tango content; Cumulative amount of Transactions (Tango); Cumulative amount of Transactions (File); Result OK / KO; Error message
         Contrôle global
#### OMGR-202
Changes in In_customer and In_customer_ext sqls regarding:

    If the id_type (CUST_SPH_08) is equal to PASSPORT then fill the PASS_NO field of in_customer.txt 
    with  the passport number and set the CUST_SPH_06 to empty
    else set PASS_NO to blank and fill the CUST_SPH_06 with the Id Number.
#### OMGR-9

In_Account:

    1. Changes in the script to have wallets which performed the transaction.
    2. Fill the field ACCBALANCE by:
        -	add a join between mtx_wallet and MTX_WALLET_BALANCES on wallet_number where MTX_WALLET_BALANCES. WALLET_SEQUENCE_NUMBER = 0
        -	add in the select : MTX_WALLET_BALANCES. BALANCE as wallet_balance, that we will be use in the main query to fill ACCBALANCE.
        
In_Account_Ext:

    1. Changes in the script to have wallets which performed the transaction_ext.
   


## VERSION 2.2

#### OMGR-128
1. In customer and customer_ext: For new subscribers, created via addons will be filled by the addons with the right values and not by external_code field

    a. CUST_SPH_08 (ID_TYPE): the id type (ex : CONSULAR_CARD)

    b. CUST_SPH_06 (ID_NO): the id number (ex : 384002004003005403OM02)
 
    c. H_COUNTRYFLAG (RESIDENCE_COUNTRY): the home country (ex: BF)

    d. NAT_COUNTRYFLAG (NATIONALITY): the nationality (ex: BF)
#### OMGR-9
1. IN_CUSTOMER: 

   a. Remove the join with mtx_trf_cntrl_profile, channel_grades and sys_payment_method_subtypes.
   
   b. In the select, add the wallet modification date : mw.modified_on as wallet_modified_on.
   
   c. In the subquery “u”, add the filter on the wallet modification date.
   
2. IN_CUSTOMER_EXT:

    a. In the subquery “mv_users_data”, remove join with mtx_trf_cntrl_profile and sys_payment_method_subtypes.
    
    b. Join channel_grades with mtx_wallet table on user_grade field.
    
    c. In the select statement add
    
	    mw.modified_on as wallet_modified_on
	   
	    grade_name as cust_sph_26
	    
	d. In the subquery “u”, add the filter on the wallet modification date.
 


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

## GR_Activities #610004: 
1. acc_currencyiso has set to tango local currency in in_transaction and in_transaction_ext.
2. In in_customer, CUST_SPH_12, CUST_SPH_13, CUST_SPH_14, CUST_SPH_15 and CUST_SPH_16 columns are set to null.

## GR_Activities #610419:
1. In In_Customer_ext_init, CUST_SPH_23, CUST_SPH_24, CUST_SPH_25, CUST_SPH_26, CUST_NPH_01 et CUST_NPH_02 columnes set to null

# New criteria compliance package
  New way of scheduling compliance task with Scheduler.py using: 
  
  "cd /home/osadmin/compliance && /usr/local/bin/python3.5 Scheduler.py --weekofmonth 2 --dayofweek 1"
  dayofweek starts from Saturday

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