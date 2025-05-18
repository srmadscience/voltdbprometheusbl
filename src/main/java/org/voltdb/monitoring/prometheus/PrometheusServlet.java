package org.voltdb.monitoring.prometheus;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.io.IOException;
import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.monitoring.ThirdPartyMetricEngineInterface;
import org.voltdb.monitoring.VoltDBGenericMetricsAgent;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.MetricsServlet;

@SuppressWarnings("serial")
public class PrometheusServlet extends MetricsServlet implements ThirdPartyMetricEngineInterface {

    private static final String STATVALUE = "STATVALUE";
    private static final String STATHELP = "STATHELP";
    private static final String STATNAME = "STATNAME";
    public static final String BL_TABLE_FORMAT = "STATNAME (VARCHAR), STATHELP (VARCHAR), LabelCol1 (VARCHAR) [ ... LabelColn (VARCHAR) ] , STATVALUE FLOAT) ";
    private static final String PROCEDUREPROFILE = "PROCEDUREPROFILE";
    private static final String ORGANIZEDTABLESTATS = "ORGANIZEDTABLESTATS";
    private static final String ORGANIZEDINDEXSTATS = "ORGANIZEDINDEXSTATS";
    private static final String ORGANIZEDSQLSTMTSTATS = "ORGANIZEDSQLSTMTSTATS";
    private static final String SNAPSHOTSTATS = "SNAPSHOTSTATS";
    private static final String[] PROCEDUREPROFILE_LATENCIES = { "AVG", "MIN", "MAX" };
    private static final String[] PROCEDUREPROFILE_DELTAS = { "INVOCATIONS", "ABORTS", "FAILURES" };

    protected static final VoltLogger logger = new VoltLogger("CONSOLE");
    private static final String VGMA = "VGMA";

    /**
     * VoltDB client
     */
    private Client client = null;

    /**
     * List of user procedures we run
     */
    private ArrayList<String> procedureList = new ArrayList<String>();

    /**
     * We only usage Gauge to store metrics.
     */
    private HashMap<String, Gauge> gauge = new HashMap<String, Gauge>();

    /**
     * Used to keep track of changes to Invocations per procedure.
     */
    private HashMap<String, Double> procedureProfileDeltas = new HashMap<String, Double>();

    /**
     * Needed so we can get correct table row counts.
     */
    int kFactor = -1;

    /**
     * HashMap of Volt tables. needed so we know if they are partitioned or not.
     */
    HashMap<String, VoltDDL> voltDDLHashMap = new HashMap<String, VoltDDL>();

    /**
     * Maps index to parent table so we know if an index is replicated or not...
     */
    private HashMap<String, String> indexAndTables = new HashMap<String, String>();

    /**
     * List of readings we've sent in past, so we can send zero readings in the
     * future. We need this because our stats don't report on inactive procs.
     */
    private Set<String> knownGaugeSignatures = new HashSet<>();

    /**
     * last time someone called doGet()
     */
    long lastScrapeTime = 0;

    /**
     * When doGet was last called.
     */
    long thisScrapeTime = 0;

    /**
     * Various parameters
     */
    TopLevelServletData data = null;

    /**
     * How many times in a row we've skipped PROCEDUREPROFILE stats.
     */
    private long invocationTotalNotIncreasingAborts = 0;

    /**
     * Number of nodes in cluster.
     */
    private int nodeCount = Byte.MAX_VALUE;

    /**
     * How often we update our DDL
     */
    final long DDL_REFRESH_INTERVAL_SECONDS = 120;

    private VoltDBGenericMetricsAgent vgma = null;

    public PrometheusServlet(TopLevelServletData data) {
        super();

        this.data = data;

        if (data.procedureList != null) {
            String[] arrayListAsAtringArray = data.procedureList.split(",");

            for (int i = 0; i < arrayListAsAtringArray.length; i++) {

                String newProc = arrayListAsAtringArray[i].trim();
                if (newProc != null && newProc.length() > 0) {
                    procedureList.add(arrayListAsAtringArray[i].trim());
                }
            }
        }
    }

    /**
     * @param metricName
     * @param labelNames
     * @param labelValues
     * @param value
     * @param help
     */
    public void reportMetric(String metricName, String[] labelNames, String[] labelValues, Number value, String help) {

        Gauge thisGauge = gauge.get(metricName);

        if (thisGauge == null) {
            thisGauge = Gauge.build().name(metricName).labelNames(labelNames).help(help).register();
            gauge.put(metricName, thisGauge);
        }

        thisGauge.labels(labelValues).set(value.doubleValue());

        addKnownGaugeReading(metricName, labelNames, labelValues);

    }

    /**
     * Add this reading to out list of known readings..
     * 
     * @param metricName
     * @param labelNames
     */
    private void addKnownGaugeReading(String metricName, String[] labelNames, String[] labelValues) {

        StringBuilder b = new StringBuilder(metricName);

        b.append(System.lineSeparator());

        for (int i = 0; i < labelNames.length; i++) {
            b.append(labelNames[i]);
            b.append(':');
        }

        b.append(System.lineSeparator());

        for (int i = 0; i < labelValues.length; i++) {
            b.append(labelValues[i]);
            b.append(':');
        }

        knownGaugeSignatures.add(b.toString());

    }

    /*
     * (non-Javadoc)
     * 
     * @see io.prometheus.client.exporter.MetricsServlet#doGet(javax.servlet.http.
     * HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        thisScrapeTime = System.currentTimeMillis();

        if (client == null) {

            try {
                getClient();
                if (client == null) {
                    data.lasterror = "PrometheusServlet.doGet: Unable to obtain client";
                    logger.error(data.lasterror);
                    System.exit(1);
                }

            } catch (Exception e) {
                logger.error(this.getClass().getName());
                logger.error(e);
            }
        }

        if (client != null) {

            if (vgma == null) {
                vgma = new VoltDBGenericMetricsAgent(this, 0);
            }

            fillInZeroReadings();

            try {

                if (data.lastDdlTime.getTime() + (DDL_REFRESH_INTERVAL_SECONDS * 1000) < System.currentTimeMillis()) {
                    getVoltDDL();
                }

                Iterator<String> procedureNameIterator = procedureList.iterator();

                while (procedureNameIterator.hasNext()) {
                    String procedureName = procedureNameIterator.next();

                    if (procedureName.equalsIgnoreCase(PROCEDUREPROFILE)) {

                        getProcedureProfileStats();

                    } else if (procedureName.equalsIgnoreCase(ORGANIZEDTABLESTATS)) {

                        getOrganizedTableStats();

                    } else if (procedureName.equalsIgnoreCase(ORGANIZEDINDEXSTATS)) {

                        getOrganizedIndexStats();

                    } else if (procedureName.equalsIgnoreCase(ORGANIZEDSQLSTMTSTATS)) {

                        getOrganizedSQLStatementStats();

                    } else if (procedureName.equalsIgnoreCase(SNAPSHOTSTATS)) {

                        getSnapshotStats();

                    } else if (procedureName.equalsIgnoreCase(VGMA)) {

                        vgma.gatherMetrics();

                    } else {

                        callUserProcedure(procedureName);
                    }

                }

            } catch (Exception e) {
                data.lasterror = "Call to doGet() failed:" + e.getMessage();
                logger.error(data.lasterror);
                logger.error("disconnecting...");
                disconnect();
            }
        }

        reportScrapeInterval(lastScrapeTime, thisScrapeTime);

        super.doGet(req, resp);

        lastScrapeTime = thisScrapeTime;

    }

    /**
     * VoltDB only generates stats for procedures that have run since stats were
     * last called, so make sure we create 0 entries for everything before get a new
     * round of stats
     */
    private void fillInZeroReadings() {

        Iterator<String> itr = knownGaugeSignatures.iterator();

        while (itr.hasNext()) {

            String rawString = itr.next();

            String[] gaugeSignature = rawString.split(System.lineSeparator());

            String metricName = gaugeSignature[0];
            String[] labelNames = new String[0];
            String[] labelValues = new String[0];

            if (gaugeSignature.length == 3) {

                labelNames = gaugeSignature[1].split(":");
                labelValues = gaugeSignature[2].split(":");

            }

            if (!metricName.equals(data.name + "_systeminformation")) {
                reportMetric(metricName, labelNames, labelValues, 0, null);
            }

        }

    }

    /**
     * Get SQL statement level stats
     */
    private void getOrganizedSQLStatementStats() {

        try {
            ClientResponse cr = client.callProcedure("@Statistics", "PROCEDUREDETAIL", 0);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                throw new Exception("getOrganizedSQLStatementStats():" + cr.getStatusString());
            }

            HashMap<String, ProcedureDetailResult> results = new HashMap<String, ProcedureDetailResult>();

            while (cr.getResults()[0].advanceRow()) {

                ProcedureDetailResult newResult = new ProcedureDetailResult(cr.getResults()[0].getString("PROCEDURE"),
                        cr.getResults()[0].getString("STATEMENT"), cr.getResults()[0].getLong("PARTITION_ID"),
                        cr.getResults()[0].getLong("INVOCATIONS"), cr.getResults()[0].getLong("AVG_EXECUTION_TIME"),
                        cr.getResults()[0].getLong("MAX_EXECUTION_TIME"), kFactor);

                if (!results.containsKey(newResult.getName())) {
                    results.put(newResult.getName(), newResult);
                }

            }

            Iterator<String> statementIterator = results.keySet().iterator();

            while (statementIterator.hasNext()) {
                ProcedureDetailResult asResult = results.get(statementIterator.next());

                reportMetric(data.name + "_" + ORGANIZEDSQLSTMTSTATS.toLowerCase() + "_invocations",
                        asResult.returnLabels(), asResult.returnLabelValues(), asResult.invocations,
                        ORGANIZEDSQLSTMTSTATS.toLowerCase() + " stats");

                reportMetric(data.name + "_" + ORGANIZEDSQLSTMTSTATS.toLowerCase() + "_avgtimens",
                        asResult.returnLabels(), asResult.returnLabelValues(), asResult.getAvgExecTimeNs(),
                        ORGANIZEDSQLSTMTSTATS.toLowerCase() + " stats");

                reportMetric(data.name + "_" + ORGANIZEDSQLSTMTSTATS.toLowerCase() + "_maxtimens",
                        asResult.returnLabels(), asResult.returnLabelValues(), asResult.getMaxExecTimeNs(),
                        ORGANIZEDSQLSTMTSTATS.toLowerCase() + " stats");

            }

        } catch (Exception e) {
            data.lasterror = "Call to " + ORGANIZEDSQLSTMTSTATS + " failed:" + e.getMessage();
            logger.error(data.lasterror);
        }

    }

    /**
     * Create stats on Snapshot performance for the latest snapshot.
     */
    private void getSnapshotStats() {
        try {

            ClientResponse cr = client.callProcedure("@Statistics", "SNAPSHOTSTATUS", 0);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                throw new Exception("getSnapshotStats():" + cr.getStatusString());
            }

            HashMap<String, SnapshotData> lastSnapshotData = new HashMap<String, SnapshotData>();

            // First pass: get list of hosts and identify max snapshot times...
            while (cr.getResults()[0].advanceRow()) {

                String hostName = cr.getResults()[0].getString("HOSTNAME");
                SnapshotData hostData = lastSnapshotData.get(hostName);

                if (hostData == null) {
                    hostData = new SnapshotData(hostName);
                    hostData.latestTimestamp = 0;
                    hostData.priorTimestamp = 0;
                    lastSnapshotData.put(hostName, hostData);
                }

                long snapshotTime = cr.getResults()[0].getLong("START_TIME");

                if (hostData.latestTimestamp < snapshotTime) {
                    hostData.latestTimestamp = snapshotTime;
                }

            }

            // Second pass: find prior snapshot for each node...
            cr.getResults()[0].resetRowPosition();
            while (cr.getResults()[0].advanceRow()) {

                String hostName = cr.getResults()[0].getString("HOSTNAME");
                SnapshotData hostData = lastSnapshotData.get(hostName);

                long snapshotTime = cr.getResults()[0].getLong("START_TIME");

                if (hostData.priorTimestamp < snapshotTime && snapshotTime < hostData.latestTimestamp) {
                    hostData.priorTimestamp = snapshotTime;
                }

            }

            // Third pass: get throughput, times and size

            Iterator<String> snapshotIterator = lastSnapshotData.keySet().iterator();
            while (snapshotIterator.hasNext()) {

                SnapshotData hostData = lastSnapshotData.get(snapshotIterator.next());
                hostData.size = 0;
                hostData.durationSeconds = 0;
                hostData.throughput = 0;

                cr.getResults()[0].resetRowPosition();
                while (cr.getResults()[0].advanceRow()) {

                    String hostName = cr.getResults()[0].getString("HOSTNAME");
                    long snapshotTime = cr.getResults()[0].getLong("START_TIME");

                    if (hostName.equals(hostData.hostname) && snapshotTime == hostData.latestTimestamp) {

                        hostData.size = hostData.size + cr.getResults()[0].getLong("SIZE");
                        hostData.durationSeconds = cr.getResults()[0].getLong("DURATION");
                        hostData.throughput = cr.getResults()[0].getDouble("THROUGHPUT");
                    }
                }

            }

            // Now create metrics
            snapshotIterator = lastSnapshotData.keySet().iterator();

            while (snapshotIterator.hasNext()) {

                SnapshotData theData = lastSnapshotData.get(snapshotIterator.next());

                String[] sizeLabels = new String[2];
                String[] sizeValues = new String[2];

                sizeLabels[0] = "hostname";
                sizeValues[0] = theData.hostname;

                sizeLabels[1] = "type";
                sizeValues[1] = "size";

                reportMetric(data.name + "_" + SNAPSHOTSTATS.toLowerCase(), sizeLabels, sizeValues, theData.size,
                        "snapshot stats");

                String[] durationLabels = new String[2];
                String[] durationValues = new String[2];

                durationLabels[0] = "hostname";
                durationValues[0] = theData.hostname;

                durationLabels[1] = "type";
                durationValues[1] = "duration";

                reportMetric(data.name + "_" + SNAPSHOTSTATS.toLowerCase(), durationLabels, durationValues,
                        theData.durationSeconds, "snapshot stats");

                String[] throughputLabels = new String[2];
                String[] throughputValues = new String[2];

                throughputLabels[0] = "hostname";
                throughputValues[0] = theData.hostname;

                throughputLabels[1] = "type";
                throughputValues[1] = "throughput";

                reportMetric(data.name + "_" + SNAPSHOTSTATS.toLowerCase(), throughputLabels, throughputValues,
                        theData.throughput, "snapshot stats");

                String[] timestampLabels = new String[2];
                String[] timestampValues = new String[2];

                timestampLabels[0] = "hostname";
                timestampValues[0] = theData.hostname;

                timestampLabels[1] = "type";
                timestampValues[1] = "timestamp";

                reportMetric(data.name + "_" + SNAPSHOTSTATS.toLowerCase(), timestampLabels, timestampValues,
                        theData.latestTimestamp, "snapshot stats");

                if (theData.priorTimestamp > 0) {

                    String[] priorTimestampLabels = new String[2];
                    String[] priorTimestampValues = new String[2];

                    priorTimestampLabels[0] = "hostname";
                    priorTimestampValues[0] = theData.hostname;

                    priorTimestampLabels[1] = "type";
                    priorTimestampValues[1] = "timesincelastsnapshotseconds";

                    reportMetric(data.name + "_" + SNAPSHOTSTATS.toLowerCase(), priorTimestampLabels,
                            priorTimestampValues, (theData.latestTimestamp - theData.priorTimestamp) / 1000,
                            "snapshot stats");

                    String[] snapshotPctLabels = new String[2];
                    String[] snapshotPctValues = new String[2];

                    snapshotPctLabels[0] = "hostname";
                    snapshotPctValues[0] = theData.hostname;

                    snapshotPctLabels[1] = "type";
                    snapshotPctValues[1] = "pcttimedoingsnapshots";

                    // start with time between snapshots, in seconds
                    double timeBetweenSnapshotsInSeconds = ((theData.latestTimestamp - theData.priorTimestamp) / 1000);

                    // factor in how long we spent doing the snapshot
                    double pct = theData.durationSeconds / timeBetweenSnapshotsInSeconds;

                    // Turn into percentage
                    pct = pct * 100;

                    reportMetric(data.name + "_" + SNAPSHOTSTATS.toLowerCase(), snapshotPctLabels, snapshotPctValues,
                            pct, "pct time spent on snapshots");

                }

            }

        } catch (Exception e) {
            data.lasterror = "Call to " + SNAPSHOTSTATS + " failed:" + e.getMessage();
            logger.error(data.lasterror);
        }

    }

    private void reportScrapeInterval(long lastScrapeTime, long thisScrapeTime) {

        String[] labelNames = { "scrape" };
        String[] labelValues = { "interval" };

        reportMetric(data.name + "_scrape", labelNames, labelValues, (thisScrapeTime - lastScrapeTime),
                "time between scrapes");

    }

    /**
     * Get user friendly table stats
     */
    private void getOrganizedTableStats() {

        try {
            ClientResponse cr = client.callProcedure("@Statistics", "TABLE", 0);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                throw new Exception("getOrganizedTableStats():" + cr.getStatusString());
            }

            Iterator<String> ddlIterator = voltDDLHashMap.keySet().iterator();

            while (ddlIterator.hasNext()) {
                String objectName = ddlIterator.next();
                VoltDDL thisObject = voltDDLHashMap.get(objectName);

                String[] labelNames = new String[4];
                String[] labelValues = new String[4];

                labelNames[0] = "table";
                labelValues[0] = objectName;

                labelNames[1] = "partitioned";
                labelValues[1] = "false";

                if (thisObject.partitioned) {
                    labelValues[1] = "true";
                }

                labelNames[2] = "type";
                labelValues[2] = thisObject.tableType;

                labelNames[3] = "dr";
                labelValues[3] = "false";

                if (thisObject.dr) {
                    labelValues[3] = "true";
                }

                double rowcount = 0;

                // For partitioned tables count all the entries, and then allow for k factor...
                if (thisObject.partitioned) {

                    cr.getResults()[0].resetRowPosition();
                    while (cr.getResults()[0].advanceRow()) {
                        String tableName = cr.getResults()[0].getString("TABLE_NAME");

                        if (tableName.equals(objectName)) {

                            rowcount += cr.getResults()[0].getLong("TUPLE_COUNT");

                        }

                    }

                    rowcount = rowcount / (kFactor + 1);

                } else {

                    // For non partitioned tables count the first entry for *this* table...

                    cr.getResults()[0].resetRowPosition();
                    while (cr.getResults()[0].advanceRow()) {
                        String tableName = cr.getResults()[0].getString("TABLE_NAME");

                        if (tableName.equals(objectName)) {

                            rowcount = cr.getResults()[0].getLong("TUPLE_COUNT");
                            break;
                        }

                    }
                }

                reportMetric(data.name + "_" + ORGANIZEDTABLESTATS.toLowerCase(), labelNames, labelValues, rowcount,
                        "organized table stats");

            }
        } catch (Exception e) {
            data.lasterror = "Call to " + ORGANIZEDTABLESTATS + " failed:" + e.getMessage();
            logger.error(data.lasterror);
        }

    }

    /**
     * Get user friendly index stats
     */
    private void getOrganizedIndexStats() {

        try {

            ClientResponse cr = client.callProcedure("@Statistics", "INDEX", 0);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                throw new Exception("getOrganizedIndexStats():" + cr.getStatusString());
            }

            Iterator<String> ddlIterator = indexAndTables.keySet().iterator();

            while (ddlIterator.hasNext()) {
                String objectName = ddlIterator.next();
                VoltDDL parentObject = voltDDLHashMap.get(indexAndTables.get(objectName));

                String[] labelNames = new String[4];
                String[] labelValues = new String[4];

                labelNames[0] = "index";
                labelValues[0] = objectName;

                labelNames[1] = "partitioned";
                labelValues[1] = "false";

                if (parentObject.partitioned) {
                    labelValues[1] = "true";
                }

                labelNames[2] = "parenttype";
                labelValues[2] = parentObject.tableType;

                labelNames[3] = "dr";
                labelValues[3] = "false";

                if (parentObject.dr) {
                    labelValues[3] = "true";
                }

                double entryCount = 0;

                // If index is on a partitioned table count all entries and then allow for
                // kfactor
                if (parentObject.partitioned) {

                    cr.getResults()[0].resetRowPosition();
                    while (cr.getResults()[0].advanceRow()) {
                        String tableName = cr.getResults()[0].getString("INDEX_NAME");

                        if (tableName.equals(objectName)) {

                            entryCount += cr.getResults()[0].getLong("ENTRY_COUNT");

                        }

                    }

                    entryCount = entryCount / (kFactor + 1);

                } else {

                    // If replicated use first entry..
                    cr.getResults()[0].resetRowPosition();
                    while (cr.getResults()[0].advanceRow()) {
                        String tableName = cr.getResults()[0].getString("INDEX_NAME");

                        if (tableName.equals(objectName)) {

                            entryCount = cr.getResults()[0].getLong("ENTRY_COUNT");
                            break;
                        }

                    }
                }

                reportMetric(data.name + "_" + ORGANIZEDINDEXSTATS.toLowerCase(), labelNames, labelValues, entryCount,
                        "organized table stats");

            }

        } catch (Exception e) {
            data.lasterror = "Call to " + ORGANIZEDINDEXSTATS + " failed:" + e.getMessage();
            logger.error(data.lasterror);
        }

    }

    /**
     * Call a named user procedure
     */
    private void callUserProcedure(String procedureName) {

        try {

            ClientResponse cr = client.callProcedure(procedureName);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                throw new Exception("callUserProcedure():" + cr.getStatusString());
            }

            for (int voltTableId = 0; voltTableId < cr.getResults().length; voltTableId++) {

                ArrayList<String> statLabelsList = new ArrayList<String>();

                for (int i = 0; i < cr.getResults()[voltTableId].getColumnCount(); i++) {
                    String possibleLabelName = cr.getResults()[voltTableId].getColumnName(i);
                    if (possibleLabelName.equalsIgnoreCase(STATNAME) || possibleLabelName.equalsIgnoreCase(STATHELP)
                            || possibleLabelName.equalsIgnoreCase(STATVALUE)) {
                        // ignore it
                    } else {
                        statLabelsList.add(possibleLabelName);
                    }
                }

                String[] statLabels = new String[statLabelsList.size()];
                statLabels = statLabelsList.toArray(statLabels);

                while (cr.getResults()[voltTableId].advanceRow()) {

                    try {
                        String statName = data.name + "_" + cr.getResults()[voltTableId].getString(STATNAME);
                        String statHelp = cr.getResults()[voltTableId].getString(STATHELP);
                        String[] statLabelValues = new String[statLabels.length];

                        for (int i = 0; i < statLabels.length; i++) {

                            // Stat labels can actually be numbers...
                            VoltType statLabelType = cr.getResults()[voltTableId]
                                    .getColumnType(cr.getResults()[voltTableId].getColumnIndex(statLabels[i]));

                            if (statLabelType == VoltType.STRING) {
                                statLabelValues[i] = cr.getResults()[voltTableId].getString(statLabels[i]);
                            } else if (statLabelType == VoltType.BIGINT || statLabelType == VoltType.INTEGER
                                    || statLabelType == VoltType.SMALLINT || statLabelType == VoltType.TINYINT) {
                                statLabelValues[i] = cr.getResults()[voltTableId].getLong(statLabels[i]) + "";
                            } else {
                                statLabelValues[i] = cr.getResults()[voltTableId].getDouble(statLabels[i]) + "";
                            }

                        }

                        double statValue = VoltType.NULL_FLOAT;

                        int columnLocation = cr.getResults()[voltTableId].getColumnIndex(STATVALUE);
                        VoltType statNumberType = cr.getResults()[voltTableId].getColumnType(columnLocation);

                        if (statNumberType == VoltType.BIGINT || statNumberType == VoltType.INTEGER
                                || statNumberType == VoltType.SMALLINT || statNumberType == VoltType.TINYINT) {
                            statValue = cr.getResults()[voltTableId].getLong(STATVALUE);
                        } else if (statNumberType  == VoltType.DECIMAL) {
                            BigDecimal aBigDecimal = cr.getResults()[voltTableId].getDecimalAsBigDecimal(STATVALUE);
                            statValue = aBigDecimal.doubleValue();
                        } else {
                            statValue = cr.getResults()[voltTableId].getDouble(STATVALUE);
                        }

                        // only if not null...
                        if (!cr.getResults()[voltTableId].wasNull()) {
                            reportMetric(statName, statLabels, statLabelValues, statValue, statHelp);
                        }

                    } catch (Exception e) {
                        data.lasterror = "Call to " + procedureName + " failed:" + e.getMessage();
                        logger.error(data.lasterror);
                        logger.error(procedureName + " needs to return one more VoltTables with this format:");
                        logger.error(BL_TABLE_FORMAT);
                    }
                }
            }

        } catch (Exception e) {
            data.lasterror = "Call to " + procedureName + " failed:" + e.getMessage();
            logger.error(data.lasterror);
        }

    }

    /**
     * Call a named user procedure
     */
    private void callUserProcedureOLD(String procedureName) {

        try {

            ClientResponse cr = client.callProcedure(procedureName);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                throw new Exception("callUserProcedure():" + cr.getStatusString());
            }

            for (int voltTableId = 0; voltTableId < cr.getResults().length; voltTableId++) {

                ArrayList<String> statLabelsList = new ArrayList<String>();

                String[] statLabels = new String[cr.getResults()[voltTableId].getColumnCount() - 3];

                for (int i = 2; i < cr.getResults()[voltTableId].getColumnCount() - 1; i++) {
                    statLabels[i - 2] = cr.getResults()[voltTableId].getColumnName(i);

                }

                while (cr.getResults()[voltTableId].advanceRow()) {

                    try {
                        String statName = data.name + "_" + cr.getResults()[voltTableId].getString(STATNAME);
                        String statHelp = cr.getResults()[voltTableId].getString(STATHELP);
                        String[] statLabelValues = new String[cr.getResults()[voltTableId].getColumnCount() - 3];

                        for (int j = 2; j < cr.getResults()[voltTableId].getColumnCount() - 1; j++) {

                            // Stat labels can actually be numbers...
                            VoltType statLabelType = cr.getResults()[voltTableId].getColumnType(j);

                            statLabelValues[j - 2] = cr.getResults()[voltTableId].getColumnName(j);
                            if (statLabelType == VoltType.STRING) {
                                statLabelValues[j - 2] = cr.getResults()[voltTableId].getString(j);
                            } else if (statLabelType == VoltType.BIGINT || statLabelType == VoltType.INTEGER
                                    || statLabelType == VoltType.SMALLINT || statLabelType == VoltType.TINYINT) {
                                statLabelValues[j - 2] = cr.getResults()[voltTableId].getLong(j) + "";
                            } else {
                                statLabelValues[j - 2] = cr.getResults()[voltTableId].getDouble(j) + "";
                            }

                        }
                        double statValue = VoltType.NULL_FLOAT;

                        int columnLocation = cr.getResults()[voltTableId].getColumnIndex(STATVALUE);
                        VoltType statNumberType = cr.getResults()[voltTableId].getColumnType(columnLocation);

                        if (statNumberType == VoltType.BIGINT || statNumberType == VoltType.INTEGER
                                || statNumberType == VoltType.SMALLINT || statNumberType == VoltType.TINYINT) {
                            statValue = cr.getResults()[voltTableId].getLong(STATVALUE);
                        } else {
                            statValue = cr.getResults()[voltTableId].getDouble(STATVALUE);
                        }

                        // only if not null...
                        if (!cr.getResults()[voltTableId].wasNull()) {
                            reportMetric(statName, statLabels, statLabelValues, statValue, statHelp);
                        }

                    } catch (Exception e) {
                        data.lasterror = "Call to " + procedureName + " failed:" + e.getMessage();
                        logger.error(data.lasterror);
                        logger.error(procedureName + " needs to return one more VoltTables with this format:");
                        logger.error(BL_TABLE_FORMAT);
                    }
                }
            }

        } catch (Exception e) {
            data.lasterror = "Call to " + procedureName + " failed:" + e.getMessage();
            logger.error(data.lasterror);
        }

    }

    /**
     * Call PROCEDUREPROFILE and turn into user friendly stats. This is non trivial,
     * as PROCEDUREPROFILE get results from each node in the cluster in a round
     * robin fashion, which could result in some odd behavior if a node is
     * restarted. The restarted node will start counting from zero....
     */
    private void getProcedureProfileStats() {

        final String statName = data.name + "_" + PROCEDUREPROFILE.toLowerCase();

        try {

            boolean allInvocationCountsAreIncreasing = true;

            double totalInvocationFootprint = 0;

            ClientResponse cr = client.callProcedure("@Statistics", PROCEDUREPROFILE, 0);

            if (cr.getStatus() != ClientResponse.SUCCESS) {
                throw new Exception("getProcedureProfileStats():" + cr.getStatusString());
            }

            cr.getResults()[0].resetRowPosition();
            while (cr.getResults()[0].advanceRow()) {

                String procName = cr.getResults()[0].getString("PROCEDURE");

                double avg = cr.getResults()[0].getLong("AVG");
                double invocations = cr.getResults()[0].getLong("INVOCATIONS");
                Double priorInvocations = procedureProfileDeltas.get(procName + '\t' + "invocations");

                if (priorInvocations != null && priorInvocations > invocations) {
                    logger.info("procedure " + procName + " Invocations going down, not up. Was "
                            + priorInvocations.doubleValue() + ", now " + invocations);
                    allInvocationCountsAreIncreasing = false;
                    break;
                }

                double footprint = 0;

                if (priorInvocations == null) {
                    footprint = avg * invocations;

                } else {
                    footprint = avg * (invocations - priorInvocations);
                }

                totalInvocationFootprint += footprint;

            }

            if (!allInvocationCountsAreIncreasing) {
                // the number of invocations is *not* going up, which is
                // weird. it happens when a node is rebooted or DDL is done...
                // Don't send stats this time.
                logger.info("Resettting internal delta counters");
                procedureProfileDeltas.clear();
                invocationTotalNotIncreasingAborts++;

            } else {

                cr.getResults()[0].resetRowPosition();
                while (cr.getResults()[0].advanceRow()) {

                    String procName = cr.getResults()[0].getString("PROCEDURE");
                    double avg = cr.getResults()[0].getLong("AVG");
                    double invocations = cr.getResults()[0].getLong("INVOCATIONS");
                    Double priorInvocations = procedureProfileDeltas.get(procName + '\t' + "invocations");

                    if (priorInvocations != null) {

                        double footprint = avg * (invocations - priorInvocations);
                        String[] labelNames = new String[2];
                        String[] labelValues = new String[2];
                        labelNames[0] = "procedure";
                        labelValues[0] = cr.getResults()[0].getString("PROCEDURE");

                        labelNames[1] = "measuretype";
                        labelValues[1] = "workloadnanoseconds";

                        reportMetric(statName, labelNames, labelValues, footprint, "procedure profile");

                        if (totalInvocationFootprint > 0) {

                            String[] extraLabelNames = new String[2];
                            String[] extraLabelValues = new String[2];

                            extraLabelNames[0] = "procedure";
                            extraLabelValues[0] = cr.getResults()[0].getString("PROCEDURE");

                            extraLabelNames[1] = "measuretype";
                            extraLabelValues[1] = "workloadpct";

                            reportMetric(statName, extraLabelNames, extraLabelValues,
                                    ((footprint * 100d) / totalInvocationFootprint), "procedure profile");
                        }
                    }

                }

                cr.getResults()[0].resetRowPosition();
                while (cr.getResults()[0].advanceRow()) {

                    for (int j = 0; j < PROCEDUREPROFILE_LATENCIES.length; j++) {

                        String[] labelNames = new String[2];
                        String[] labelValues = new String[2];
                        labelNames[0] = "procedure";
                        labelValues[0] = cr.getResults()[0].getString("PROCEDURE");

                        labelNames[1] = "measuretype";
                        labelValues[1] = PROCEDUREPROFILE_LATENCIES[j].toLowerCase();

                        long value = cr.getResults()[0].getLong(PROCEDUREPROFILE_LATENCIES[j]);

                        reportMetric(statName, labelNames, labelValues, value, "time in nanoseconds");

                    }

                    for (int j = 0; j < PROCEDUREPROFILE_DELTAS.length; j++) {

                        String[] labelNames = new String[2];
                        String[] labelValues = new String[2];
                        labelNames[0] = "procedure";
                        labelValues[0] = cr.getResults()[0].getString("PROCEDURE");

                        labelNames[1] = "measuretype";
                        labelValues[1] = PROCEDUREPROFILE_DELTAS[j].toLowerCase();

                        long value = cr.getResults()[0].getLong(PROCEDUREPROFILE_DELTAS[j]);
                        reportMetric(statName, labelNames, labelValues, value, "number of changes");

                        // Store details of
                        calculateDelta(cr.getResults()[0].getString("PROCEDURE"),
                                PROCEDUREPROFILE_DELTAS[j].toLowerCase(), value);

                    }

                }
            }

            String[] abortLabels = { "internalmetric" };
            String[] abortLabelValues = { "invocationsdecreasingabort" };

            reportMetric(statName + "_metrics", abortLabels, abortLabelValues, invocationTotalNotIncreasingAborts,
                    "number of invocation aborts");

        } catch (Exception e) {
            data.lasterror = "Call to " + PROCEDUREPROFILE + " failed:" + e.getMessage();
            logger.error(data.lasterror);
        }

    }

    /**
     * Calculate PROCEDUREPROFILE invocation deltas
     */
    private Double calculateDelta(String procName, String stat, double reportedValue) {

        double deltaValue = reportedValue;
        Double existingValue = procedureProfileDeltas.get(procName + '\t' + stat);

        if (existingValue != null) {
            if (existingValue > reportedValue) {
                // Report zero when counter appears to have gone backwards...
                deltaValue = 0;
            } else {
                deltaValue = reportedValue - existingValue;
            }
        } else {
            // Ignore first value as it will not be a delta...
            deltaValue = 0;
        }

        procedureProfileDeltas.put(procName + '\t' + stat, reportedValue);

        return deltaValue;
    }

    @Override
    protected void finalize() throws Throwable {

        if (client != null) {
            client.drain();
            client.close();
            client = null;
        }
        super.finalize();
    }

    /**
     * Get Client and connect to the servers if we fail we will try again in next
     * polling cycle.
     *
     * @throws UnknownHostException
     * @throws IOException
     * @throws ProcCallException
     */
    private void getClient() {

        try {

            if (client != null && client.getConnectedHostList().size() > 0) {
                return;
            }

            String[] serverlist = data.serverList.split(",");
            ClientConfig c_config = new ClientConfig(data.user, data.password);
            c_config.setProcedureCallTimeout(0); // Set procedure all to infinite
            client = ClientFactory.createClient(c_config);
            for (String s : serverlist) {
                client.createConnection(s.trim(), data.port);
                logger.info("Connected to server: " + s);
                data.setConnectedServerList(data.getConnectedServerList() + s.trim());
            }

            getSystemInformation();
            getVoltDDL();

        } catch (Exception e) {

            data.lasterror = "Call to " + PROCEDUREPROFILE + " failed:" + e.getMessage();
            logger.error(data.lasterror);

            if (client != null) {
                logger.error("Disconnected");
                client = null;

            }
        }

    }

    @SuppressWarnings("deprecation")
    private void getSystemInformation() throws Exception {

        final String statName = data.name + "_systeminformation";
        final String[] labelnames = { "property" };

        ClientResponse cr = client.callProcedure("@SystemInformation", "DEPLOYMENT");

        if (cr.getStatus() != ClientResponse.SUCCESS) {
            throw new Exception("getSystemInformation() DEPLOYMENT:" + cr.getStatusString());
        }

        cr.getResults()[0].resetRowPosition();
        while (cr.getResults()[0].advanceRow()) {

            String property = cr.getResults()[0].getString("PROPERTY");
            String value = cr.getResults()[0].getString("VALUE");
            String[] labelValues = { property.toLowerCase() };

            if (property.equalsIgnoreCase("kfactor")) {
                kFactor = new Integer(value);
                reportMetric(statName, labelnames, labelValues, kFactor, "systeminformation");
            } else if (property.equalsIgnoreCase("sitesperhost")) {
                int sitesPerHost = new Integer(value);
                reportMetric(statName, labelnames, labelValues, sitesPerHost, "systeminformation");
            }

        }

        cr = client.callProcedure("@SystemInformation", "OVERVIEW");

        if (cr.getStatus() != ClientResponse.SUCCESS) {
            throw new Exception("getSystemInformation() OVERVIEW:" + cr.getStatusString());
        }

        Set<Integer> hostIds = new HashSet<Integer>();

        cr.getResults()[0].resetRowPosition();
        while (cr.getResults()[0].advanceRow()) {

            int hostId = (int) cr.getResults()[0].getLong("HOST_ID");
            hostIds.add(hostId);

            String property = cr.getResults()[0].getString("KEY");
            String value = cr.getResults()[0].getString("VALUE");
            String[] labelValues = { property.toLowerCase() };

            if (property.equalsIgnoreCase("FULLCLUSTERSIZE")) {
                nodeCount = new Integer(value);
                reportMetric(statName, labelnames, labelValues, nodeCount, "full cluster size");
            }

        }

        String[] labelValues = { "current_cluster_size" };
        reportMetric(statName, labelnames, labelValues, hostIds.size(), "current cluster size");

    }

    /**
     * Get latest TABLE and INDEX metadata
     * 
     * @throws Exception
     */
    private void getVoltDDL() throws Exception {

        ClientResponse cr = client.callProcedure("@SystemCatalog", "TABLES");

        if (cr.getStatus() != ClientResponse.SUCCESS) {
            throw new Exception("getProcedureProfileStats():" + cr.getStatusString());
        }

        while (cr.getResults()[0].advanceRow()) {

            String tablename = cr.getResults()[0].getString("TABLE_NAME");
            String tableType = cr.getResults()[0].getString("TABLE_TYPE");
            String remarks = cr.getResults()[0].getString("REMARKS");

            boolean isDr = false;
            boolean isPartitioned = false;

            if (remarks != null) {
                if (remarks.indexOf("partitionColumn") > -1) {
                    isPartitioned = true;
                }

                if (remarks.indexOf("drEnabled") > -1) {
                    isDr = true;
                }
            }

            VoltDDL newDDL = new VoltDDL(tablename, tableType, isDr, isPartitioned);
            voltDDLHashMap.put(tablename, newDDL);

        }

        cr = client.callProcedure("@SystemCatalog", "INDEXINFO");

        cr.getResults()[0].resetRowPosition();
        while (cr.getResults()[0].advanceRow()) {

            String tablename = cr.getResults()[0].getString("TABLE_NAME");
            String indexname = cr.getResults()[0].getString("INDEX_NAME");

            indexAndTables.put(indexname, tablename);

        }

        cr = client.callProcedure("@SystemCatalog", "PROCEDURES");

        cr.getResults()[0].resetRowPosition();
        while (cr.getResults()[0].advanceRow()) {

            String procedurename = cr.getResults()[0].getString("PROCEDURE_NAME");

            if (procedurename.endsWith(data.procedureSuffix) && (!procedureList.contains(procedurename))) {
                procedureList.add(procedurename);
            }

        }

    }

    /**
     * disconnect from voltdb.
     */
    public void disconnect() {
        data.setConnectedServerList("");
        if (client != null) {
            try {
                client.drain();
                client.close();
            } catch (NoConnectionsException | InterruptedException e) {
                e.printStackTrace();
            }
            client = null;
        }

    }

    /*
     * Extend this class for processing results and reporting to newrelic.
     */
    private abstract class AbstractStatsProcedureCallback implements ProcedureCallback {

        CountDownLatch cbwaiters;
        String namespace;
        int callCount = 0;
        HashMap<String, String> helpText = new HashMap<String, String>();
        HashMap<String, String> m_labelText = new HashMap<String, String>();

        public AbstractStatsProcedureCallback(CountDownLatch cbwaiters, String namespace,
                HashMap<String, String> helpText, HashMap<String, String> m_labelText) {
            this.cbwaiters = cbwaiters;
            this.namespace = namespace;
            this.helpText = helpText;
            this.m_labelText = m_labelText;
        }

    }

    @Override
    public String getName() {
        return "VGMA";
    }

    @Override
    public String getServers() {
        return data.serverList;
    }

    @Override
    public String getUser() {
        // TODO Auto-generated method stub
        return data.user;
    }

    @Override
    public String getPassword() {
        return data.password;
    }

    @Override
    public int getPort() {
        return data.port;
    }

    @Override
    public void reportMetric(String metricName, String units, Number value) {

        String[] labelNames = { "label_name" };
        String[] labelValues = { units };
        String help = "help";

        reportMetric(metricName, labelNames, labelValues, value, help);
    }

}
