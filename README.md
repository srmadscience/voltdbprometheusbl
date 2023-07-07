# README #

voltprometheusbl - allow user bl code to generate prometheus stats

## What is this repository for? ##

voltprometheusbl is a utility that runs a prometheus web server with BL-related stats.


## How do I get set up? ##

* Compile or use JAR file.
* Run the Jar file on *one* node in your cluster
* Default output will be on http://localhost:1235/metrics

## Parameters ##

## --servers ##
comma delimited list of VoltDB servers. Defaults to localhost.

## --port ##
VoltDB port. Defaults to 21212

## --webserverport ##
Port our web server runs on. Default to 1235. VoltDB uses 1234 for its own stats.

## --user= ##
Username.

## --password= ##
password

## --name= ##
prefix for generated stats. Defaults to 'voltdbbl'.

## --procedureList= ##

comma delimited list of procedures to run. The assumption is each procedure has no parameters and produces one or more VoltTables like this:

```
STATNAME (VARCHAR), STATHELP (VARCHAR), LabelCol1 (VARCHAR) [ ... LabelColn (VARCHAR) ] , STATVALUE FLOAT)
```

the STATNAME, STATHELP and STATVALUE columns are mandatory and must occur in the locations shown above.

An example of such a procedure would be:
```
create procedure fred as select 'totalbalance' statname,  'a help message' stathelp , '7' shoesize, '12' vendorname,balance statvalue from total_balances;
```

Output would be:
```
STATNAME      STATHELP        SHOESIZE  VENDORNAME  STATVALUE  
------------- --------------- --------- ----------- -----------
totalbalance  a help message  7         12           9547644634
```

and the webserver would say:

```
voltdbbl_totalbalance{SHOESIZE="7",VENDORNAME="12",} 9.376709053E9
```

--procedureList defaults to 'ORGANIZEDINDEXSTATS,ORGANIZEDTABLESTATS'. See Below.

## Built in procedures ##

The following procedures are included by default:

### ORGANIZEDTABLESTATS ###

Produces one row for each table, accounting for k factor.

```
voltdbbl_organizedtablestats{table="ALLOCATED_BY_PRODUCT",partitioned="true",type="VIEW",dr="false",} 40.0
voltdbbl_organizedtablestats{table="USER_RECENT_TRANSACTIONS",partitioned="true",type="TABLE",dr="true",} 649538.0
voltdbbl_organizedtablestats{table="TOTAL_BALANCES",partitioned="true",type="VIEW",dr="false",} 8.0
voltdbbl_organizedtablestats{table="USER_BALANCES",partitioned="true",type="TABLE",dr="true",} 100000.0
voltdbbl_organizedtablestats{table="USER_TABLE",partitioned="true",type="TABLE",dr="true",} 100000.0
voltdbbl_organizedtablestats{table="PRODUCT_TABLE",partitioned="false",type="TABLE",dr="true",} 5.0
voltdbbl_organizedtablestats{table="USER_FINANCIAL_EVENTS",partitioned="true",type="EXPORT",dr="false",} 0.0
voltdbbl_organizedtablestats{table="USER_USAGE_TABLE",partitioned="true",type="TABLE",dr="true",} 476404.0
voltdbbl_organizedtablestats{table="USER_BALANCE_TOTAL_VIEW",partitioned="true",type="VIEW",dr="false",} 100000.0
```


### ORGANIZEDINDEXSTATS ###

Produces one row for each index, accounting for parent table's k factor.

```
voltdbbl_organizedindexstats{index="URT_DEL_IDX3",partitioned="true",parenttype="TABLE",dr="true",} 0.0
voltdbbl_organizedindexstats{index="UT_DEL",partitioned="true",parenttype="TABLE",dr="true",} 100000.0
voltdbbl_organizedindexstats{index="URT_DEL_IDX2",partitioned="true",parenttype="TABLE",dr="true",} 649538.0
voltdbbl_organizedindexstats{index="UUT_DEL_IDX2",partitioned="true",parenttype="TABLE",dr="true",} 476404.0
voltdbbl_organizedindexstats{index="VOLTDB_AUTOGEN_IDX_PK_PRODUCT_TABLE_PRODUCTID",partitioned="false",parenttype="TABLE",dr="true",} 5.0
voltdbbl_organizedindexstats{index="VOLTDB_AUTOGEN_IDX_PK_USER_BALANCES_USERID",partitioned="true",parenttype="TABLE",dr="true",} 100000.0
voltdbbl_organizedindexstats{index="UUT_DEL_IDX",partitioned="true",parenttype="TABLE",dr="true",} 476404.0
voltdbbl_organizedindexstats{index="MATVIEW_PK_INDEX",partitioned="true",parenttype="VIEW",dr="false",} 100040.0
voltdbbl_organizedindexstats{index="VOLTDB_AUTOGEN_IDX_PK_USER_USAGE_TABLE_USERID_PRODUCTID_SESSIONID",partitioned="true",parenttype="TABLE",dr="true",} 476404.0
voltdbbl_organizedindexstats{index="VOLTDB_AUTOGEN_IDX_PK_USER_TABLE_USERID",partitioned="true",parenttype="TABLE",dr="true",} 100000.0
voltdbbl_organizedindexstats{index="URT_DEL_IDX",partitioned="true",parenttype="TABLE",dr="true",} 649538.0
voltdbbl_organizedindexstats{index="VOLTDB_AUTOGEN_IDX_PK_USER_RECENT_TRANSACTIONS_USERID_USER_TXN_ID",partitioned="true",parenttype="TABLE",dr="true",} 649538.0
```

### Procedure Profile Stats ###

Call PROCEDUREPROFILE and turn into user friendly stats. This is non trivial,
as PROCEDUREPROFILE get results from each node in the cluster in a round
robin fashion, which could result in some odd behavior if a node is
restarted. The restarted node will start counting from zero....


###  ORGANIZEDSQLSTMTSTATS ###

Get SQL statement level stats

### SNAPSHOTSTATS ###

 Create stats on Snapshot performance for the latest snapshot.
