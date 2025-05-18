/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.monitoring;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.HdrHistogram_voltpatches.Histogram;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CompressionStrategySnappy;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class VoltDBGenericMetricsAgent implements ThirdPartyMetricEngineInterface {

    protected static final VoltLogger logger = new VoltLogger("CONSOLE");

    /**
     * How many nanoseconds an event has to take before we report on it.
     */
    private static final long DEFAULT_THRESHOLD_NS = 500 * 1000;

    /**
     * How many rows or kilobytes an index has to have before we report on it.
     */
    private static final long DEFAULT_IDX_THRESH = 10000;

    /**
     * For Newrelic and all implementations that don't require messing with how we
     * format metric names.
     */
    public static final int FLAVOR_NEWRELIC = 0;

    /**
     * For prometheus.
     */
    public static final int FLAVOR_PROMETHEUS = 1;

    /**
     * We populate this on startup.
     */
    private static final int CLUSTERID_UNKNOWN = -1;

    /**
     * Default port.
     */
    public static int DEFAULT_PORT = 21212;

    /**
     * VoltDB m_client
     */
    private Client m_client = null;

    /**
     * Used in testing
     */
    private boolean isTest = false;

    /**
     * Used in testing
     */
    private AtomicInteger statReportForTest = new AtomicInteger(0);

    /**
     * Used for tracking application latency
     */
    private Histogram m_histogram = null;

    /**
     * A thing we're sending metrics to.
     */
    private ThirdPartyMetricEngineInterface m_metricEngine = null;

    /**
     * Prometheus expects m_helpText for metrics.
     */
    final HashMap<String, String> m_helpText = new HashMap<>();

    /**
     * Prometheus expects labels for metrics.
     */
    final HashMap<String, String> m_labelText = new HashMap<>();

    /**
     * ReportIoStatsCallback needs to track deltas, so it remains in existence
     * between calls.
     */
    ReportIoStatsCallback iostatsCallback = new ReportIoStatsCallback(m_helpText, m_labelText);

    /**
     * ReportProcedureProfileCallback needs to track deltas, so it remains in
     * existence between calls.
     */
    ReportProcedureProfileCallback reportProcedureProfileCB = new ReportProcedureProfileCallback(m_helpText,
            m_labelText);

    /**
     * ReportProcedureProfileDetailCallback needs to track deltas, so it remains in
     * existence between calls.
     */
    ReportProcedureProfileDetailCallback reportProcedureProfileDetailCB = new ReportProcedureProfileDetailCallback(
            DEFAULT_THRESHOLD_NS, m_helpText, m_labelText);

    /**
     * ReportImportCallback needs to track deltas, so it remains in existence
     * between calls.
     */
    ReportImportCallback reportImportCB = new ReportImportCallback(m_helpText, m_labelText);

    /**
     * ReportExportCallback needs to track deltas, so it remains in existence
     * between calls.
     */
    ReportExportCallback reportExportCB = new ReportExportCallback(m_helpText, m_labelText);

    /**
     * Cluster ID
     */
    int clusterId = CLUSTERID_UNKNOWN;

    /**
     * Count of stats written this pass..
     */
    int m_statCount = 0;

    /**
     * Create an agent for metrics reporting. This is called for each agent thats
     * defined in JSON document. One can have many clusters with servers and port
     * specified in JSON document.
     *
     * @param name
     * @param servers
     * @param user
     * @param password
     * @param port
     */
    public VoltDBGenericMetricsAgent(ThirdPartyMetricEngineInterface tpme, int flavor) {
        this.m_metricEngine = tpme;
    }

    @Override
    public void reportMetric(String metricName, String units, Number value) {
        m_metricEngine.reportMetric(metricName.toLowerCase().replace("/", "_"), units, value);
        m_statCount++;
    }

    @Override
    public String getName() {
        return m_metricEngine.getName();
    }

    @Override
    public String getServers() {
        return m_metricEngine.getServers();
    }

    @Override
    public String getUser() {
        return m_metricEngine.getUser();
    }

    @Override
    public String getPassword() {
        return m_metricEngine.getPassword();
    }

    @Override
    public int getPort() {
        return m_metricEngine.getPort();
    }

    Client getAgentClient() {
        return m_client;
    }

    int getStatReportForTest() {
        return statReportForTest.get();
    }

    void setStatReportForTest(int statReportForTest) {
        this.statReportForTest.set(statReportForTest);
    }

    void setIsTest(boolean isTest) {
        this.isTest = isTest;
    }

    /*
     * Extend this class for processing results and reporting to newrelic.
     */
    private abstract class AbstractStatsProcedureCallback implements ProcedureCallback {

        CountDownLatch cbwaiters;
        String namespace;
        int callCount = 0;
        HashMap<String, String> helpText = new HashMap<>();
        HashMap<String, String> m_labelText = new HashMap<>();

        public AbstractStatsProcedureCallback(CountDownLatch cbwaiters, String namespace,
                HashMap<String, String> helpText, HashMap<String, String> m_labelText) {
            this.cbwaiters = cbwaiters;
            this.namespace = namespace;
            this.helpText = helpText;
            this.m_labelText = m_labelText;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            try {
                if (response.getStatus() == ClientResponse.SUCCESS) {
                    VoltTable tbls[] = response.getResults();
                    VoltTable table = tbls[0];
                    processResult(table, namespace);
                }
            } catch (Throwable ex) {
                logger.error("Failed to process stats for namespace " + namespace + ":" + ex.getMessage());
            } finally {
                cbwaiters.countDown();
            }
        }

        public abstract void processResult(VoltTable table, String namespace);

    }

    private abstract class AbstractPersistentStatsProcedureCallback implements ProcedureCallback {

        protected static final String PATTERN_EXACT = "EXACT";
        protected static final String PATTERN_REGEX = "REGEX";

        CountDownLatch cbwaiters;
        String namespace;

        Map<String, Number> valuesFromPriorCalls = new HashMap<>();

        Map<String, Long> valuesFromThisCall = new HashMap<>();

        HashMap<String, String> deltaColumns = new HashMap<>();

        HashMap<String, String> helpText = new HashMap<>();

        HashMap<String, String> m_labelText = new HashMap<>();

        long latestProcessResultsTimeMS = System.currentTimeMillis();
        long lastProcessResultsTimeMS = System.currentTimeMillis();

        public AbstractPersistentStatsProcedureCallback(String namespace, HashMap<String, String> helpText,
                HashMap<String, String> m_labelText) {

            this.namespace = namespace;
            this.helpText = helpText;
            this.m_labelText = m_labelText;

        }

        public void setWaiters(CountDownLatch cbwaiters) {
            this.cbwaiters = cbwaiters;
        }

        public long calculateOldValue(String statName, Number latestValue) {
            long oldValue = 0;

            if (latestValue != null) {
                if (isADeltaCol(statName)) {
                    Number oldNumber = valuesFromPriorCalls.get(statName);

                    if (oldNumber != null) {
                        oldValue = oldNumber.longValue();
                    }

                }
            }

            return oldValue;
        }

        protected void reportValuesForThisCall(String prefix) {
            for (String key : valuesFromThisCall.keySet()) {
                long theValue = valuesFromThisCall.get(key).longValue();
                reportMetric(prefix + key, "value", new Long(theValue));

            }

        }

        protected void incValueForThisCall(String valueName, long value) {
            Long currentValue = valuesFromThisCall.get(valueName);

            if (currentValue == null) {
                currentValue = new Long(0);
            }

            valuesFromThisCall.put(valueName, currentValue + value);

        }

        private boolean isADeltaCol(String statName) {
            boolean isADeltaCol = false;

            if (deltaColumns.containsKey(statName) && deltaColumns.get(statName).equals(PATTERN_EXACT)) {
                isADeltaCol = true;
            } else {
                // Iterate through list of patterns and see if one matches
                for (String key : deltaColumns.keySet()) {
                    String thePatternMatchKind = deltaColumns.get(key);

                    if (thePatternMatchKind.equals(PATTERN_REGEX)) {
                        if (statName.matches(key)) {
                            isADeltaCol = true;
                            break;
                        }
                    }
                }
            }

            return isADeltaCol;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            try {
                if (response.getStatus() == ClientResponse.SUCCESS) {
                    VoltTable tbls[] = response.getResults();
                    VoltTable table = tbls[0];
                    lastProcessResultsTimeMS = latestProcessResultsTimeMS;
                    latestProcessResultsTimeMS = System.currentTimeMillis();
                    processResult(table, namespace);
                }
            } catch (Throwable ex) {
                logger.error("Failed to process stats for namespace " + namespace + ":" + ex.getMessage());
            } finally {
                cbwaiters.countDown();
            }
        }

        public long getDeltaPerSecond(long value) {

            final float durationInS = (latestProcessResultsTimeMS - lastProcessResultsTimeMS) / 1000;

            if (durationInS == 0) {
                return 0;
            }

            return (long) (value / durationInS);

        }

        protected void reportDeltaIfKnown(String keyprefix, String rowPK, String valueName, VoltTable table) {

            long theValue = table.getLong(valueName);

            if (theValue > Long.MIN_VALUE) {

                Long oldValue = (Long) valuesFromPriorCalls.get(keyprefix + valueName + "/" + rowPK);

                if (oldValue != null) {
                    // Send delta...
                    reportMetric(keyprefix + valueName + "/" + rowPK, "value", theValue - oldValue.longValue());
                    reportMetric(keyprefix + valueName + "_PER_SECOND" + "/" + rowPK, "value",
                            getDeltaPerSecond(theValue - oldValue.longValue()));
                }

                valuesFromPriorCalls.put(keyprefix + valueName + "/" + rowPK, theValue);

            }

        }

        public abstract void processResult(VoltTable table, String namespace);

    }

    private final class ReportMemoryCallback extends AbstractStatsProcedureCallback implements ProcedureCallback {

        public ReportMemoryCallback(CountDownLatch cbwaiters, HashMap<String, String> helpText,
                HashMap<String, String> m_labelText) {
            super(cbwaiters, "Memory", helpText, m_labelText);
        }

        /**
         * Memory reporting does not use base method as we dont report TUPLECOUNT and
         * units are not consistent e.g. POOLEDMEMORY is in MB and rest in KB.
         *
         * @param table
         * @param namespace
         */
        @Override
        public void processResult(VoltTable table, String namespace) {

            Map<String, Number> procValue = new HashMap<>();

            // Add number of 'up' nodes
            String nodeCountKey = "VoltDB/" + namespace + "/Nodecount";
            Long nodeCount = new Long(table.getRowCount());
            reportMetric(nodeCountKey, "nodes", nodeCount);

            long rss = 0;
            long physicalMem = 0;

            for (int rc = 0; rc < table.getRowCount(); rc++) {
                if (!table.advanceRow()) {
                    return;
                }

                rss = table.getLong("RSS");
                physicalMem = table.getLong("PHYSICALMEMORY");

                int ccount = table.getColumnCount();
                for (int i = 0; i < ccount; i++) {
                    VoltType vtype = table.getColumnType(i);
                    String cname = table.getColumnName(i);
                    if (cname.equalsIgnoreCase("timestamp") || cname.equalsIgnoreCase("site_id")
                            || cname.equalsIgnoreCase("PARTITION_ID") || cname.equalsIgnoreCase("TUPLECOUNT")
                            || cname.equalsIgnoreCase("host_id")) {
                        continue;
                    }
                    String key = "VoltDB/" + namespace + "/" + cname;
                    Number oldVal = procValue.get(key);
                    Number cvalue = null;
                    int multiplier = 1024;
                    if (cname.equalsIgnoreCase("POOLEDMEMORY")) {
                        multiplier = (1024 * 1024);
                    }
                    switch (vtype) {
                    case INTEGER:
                    case BIGINT:
                    case SMALLINT:
                        Long clvalue = table.getLong(i);
                        clvalue *= multiplier;
                        if (oldVal != null) {
                            clvalue += oldVal.longValue();
                        }
                        cvalue = clvalue;
                        break;
                    case FLOAT:
                        Double cdvalue = table.getDouble(i);
                        cdvalue *= multiplier;
                        if (oldVal != null) {
                            cdvalue += oldVal.doubleValue();
                        }
                        cvalue = cdvalue;
                        break;
                    default:
                        cname = null;
                    }
                    if (cname != null) {
                        procValue.put(key, cvalue);
                        m_labelText.put(key, makeDefaultLabel(cname));
                        helpText.put(key, "See MEMORY in @Statistics ");
                    }
                }
            }

            statReportForTest.incrementAndGet();
            for (String key : procValue.keySet()) {
                reportMetric(key, "bytes", procValue.get(key));
            }

            long ramUsed = 0;

            if (rss > 0) {
                ramUsed = (rss * 100) / physicalMem;

            }

            reportMetric("VoltDB/" + "Dashboard" + "/" + "ramUsed", "pct", ramUsed);

        }
    }

    private int reportMemory(CountDownLatch cbwaiters) throws IOException, ProcCallException {
        ReportMemoryCallback cb = new ReportMemoryCallback(cbwaiters, m_helpText, m_labelText);
        return callProcedure(cb, "@Statistics", "MEMORY", 0);
    }

    private void reportValueColumns(String keyprefix, String rowPK, String[] reportValueColumns, VoltTable table) {
        for (String reportValueColumn : reportValueColumns) {
            long aValue = table.getLong(reportValueColumn);
            reportMetric(keyprefix + reportValueColumn + "/" + rowPK, "value", aValue);
        }

    }

    private final class ReportExportCallback extends AbstractPersistentStatsProcedureCallback
            implements ProcedureCallback {

        final String keyprefix = "VoltDB/Export/";
        final String[] numberValueColumns = { "TUPLE_PENDING", "AVERAGE_LATENCY", "MAX_LATENCY", "QUEUE_GAP" };
        final String[] dateValueAsBigIntColumns = { "LAST_QUEUED_TIMESTAMP", "LAST_ACKED_TIMESTAMP" };

        public ReportExportCallback(HashMap<String, String> helpText, HashMap<String, String> m_labelText) {
            super("EXPORT", helpText, m_labelText);

        }

        @Override
        public void processResult(VoltTable table, String namespace) {

            valuesFromThisCall = new HashMap<>();

            for (int rc = 0; rc < table.getRowCount(); rc++) {
                if (!table.advanceRow()) {
                    return;
                }

                String rowPK = getRowPK(table);
                String aggPK = getAggPK(table);

                reportDeltaIfKnown(keyprefix, rowPK, "TUPLE_COUNT", table);

                incValueForThisCall(aggPK + "/TUPLE_PENDING", table.getLong("TUPLE_PENDING"));

                reportValueColumns(keyprefix, rowPK, numberValueColumns, table);
                reportTimestampColumns(keyprefix, rowPK, dateValueAsBigIntColumns, table);

                final String status = table.getString("STATUS");
                final String active = table.getString("ACTIVE");

                if (status.equalsIgnoreCase("ACTIVE")) {
                    reportMetric(keyprefix + "Status/" + rowPK, "value", 1L);
                } else if (status.equalsIgnoreCase("BLOCKED")) {
                    reportMetric(keyprefix + "Status/" + rowPK, "value", 0L);
                } else if (status.equalsIgnoreCase("DROPPED")) {
                    reportMetric(keyprefix + "Status/" + rowPK, "value", -1L);
                }

                if (active.equalsIgnoreCase("TRUE")) {
                    reportMetric(keyprefix + "Active/" + rowPK, "value", 1L);
                } else {
                    reportMetric(keyprefix + "Active/" + rowPK, "value", 0L);
                }
            }

            reportValuesForThisCall(keyprefix);

        }

        private String getAggPK(VoltTable table) {
            StringBuffer sb = new StringBuffer();

            sb.append(table.getString("SOURCE"));
            sb.append('_');

            String target = table.getString("TARGET");
            if (target.length() == 0) {
                target = "NOTARGET";
            }
            sb.append(target);

            return sb.toString();

        }

        private void reportTimestampColumns(String keyprefix, String rowPK, String[] dateValueColumns,
                VoltTable table) {

            for (String dateValueColumn : dateValueColumns) {
                long aValue = table.getTimestampAsLong(dateValueColumn);
                reportMetric(keyprefix + dateValueColumn + "/" + rowPK, "value", aValue);
            }

        }

        private String getRowPK(VoltTable table) {
            StringBuffer sb = new StringBuffer();

            sb.append(table.getString("SOURCE"));
            sb.append('_');

            String target = table.getString("TARGET");
            if (target.length() == 0) {
                target = "NOTARGET";
            }
            sb.append(target);
            sb.append('/');

            sb.append(table.getLong("HOST_ID"));
            sb.append('_');

            sb.append(table.getString("HOSTNAME"));
            sb.append('_');

            sb.append(table.getLong("SITE_ID"));
            sb.append('_');

            sb.append(table.getLong("PARTITION_ID"));

            return sb.toString();

        }

    }

    private int reportExport(CountDownLatch cbwaiters) throws IOException, ProcCallException {
        reportExportCB.setWaiters(cbwaiters);
        return callProcedure(reportExportCB, "@Statistics", "EXPORT", 0);
    }

    private final class ReportImportCallback extends AbstractPersistentStatsProcedureCallback
            implements ProcedureCallback {

        final String keyprefix = "VoltDB/Import/";
        final String[] numberValueColumns = { "OUTSTANDING_REQUESTS" };

        public ReportImportCallback(HashMap<String, String> helpText, HashMap<String, String> m_labelText) {
            super("EXPORT", helpText, m_labelText);
            valuesFromPriorCalls = new HashMap<>();

        }

        @Override
        public void processResult(VoltTable table, String namespace) {

            valuesFromThisCall = new HashMap<>();

            for (int rc = 0; rc < table.getRowCount(); rc++) {
                if (!table.advanceRow()) {
                    return;
                }

                String rowPK = getRowPK(table);

                reportDeltaIfKnown(keyprefix, rowPK, "SUCCESSES", table);
                reportDeltaIfKnown(keyprefix, rowPK, "FAILURES", table);
                reportDeltaIfKnown(keyprefix, rowPK, "RETRIES", table);
                reportValueColumns(keyprefix, rowPK, numberValueColumns, table);

            }

        }

        private String getRowPK(VoltTable table) {
            StringBuffer sb = new StringBuffer();

            sb.append(table.getString("IMPORTER_NAME"));
            sb.append('_');

            sb.append(table.getString("PROCEDURE_NAME"));
            sb.append('/');

            sb.append(table.getLong("HOST_ID"));
            sb.append('_');

            sb.append(table.getString("HOSTNAME"));
            sb.append('_');

            sb.append(table.getLong("SITE_ID"));

            return sb.toString();

        }
    }

    private int reportImport(CountDownLatch cbwaiters) throws IOException, ProcCallException {
        reportImportCB.setWaiters(cbwaiters);
        return callProcedure(reportImportCB, "@Statistics", "IMPORT", 0);
    }

    private final class ReportCPUCallback extends AbstractStatsProcedureCallback implements ProcedureCallback {

        final String cpuMin = "voltdb_cpu";
        final String cpuMax = "voltdb_cpu";
        final String cpu = "voltdb_cpu";

        public ReportCPUCallback(CountDownLatch cbwaiters, HashMap<String, String> helpText,
                HashMap<String, String> m_labelText) {
            super(cbwaiters, "CPU", helpText, m_labelText);

            helpText.put(cpuMin,
                    "Minimum CPU load and Max CPU load should be almost the same. If not there may be an unbalanced workload.");
            helpText.put(cpuMax,
                    "Minimum CPU load and Max CPU load should be almost the same. If not there may be an unbalanced workload.");

            m_labelText.put(cpuMin, "Minimum CPU load for a node in this cluster");
            m_labelText.put(cpuMin, "Minimum CPU load for a node in this cluster");
            m_labelText.put(cpu, "Average CPU load for a node in this cluster");
        }

        /**
         * Memory reporting does not use base method as we dont report TUPLECOUNT and
         * units are not consistent e.g. POOLEDMEMORY is in MB and rest in KB.
         *
         * @param table
         * @param namespace
         */
        @Override
        public void processResult(VoltTable table, String namespace) {

            String[] labelNames = { "hostname", "cpumaxorminoractual" };
            String[] labelValues = { null, null };

            long maxPct = 0;
            long minPct = 100;

            for (int rc = 0; rc < table.getRowCount(); rc++) {
                if (!table.advanceRow()) {
                    return;
                }

                String hostname = table.getString("HOSTNAME");
                long thisPct = table.getLong("PERCENT_USED");
                maxPct = Math.max(maxPct, thisPct);
                minPct = Math.min(minPct, thisPct);

                labelValues[0] = hostname;
                labelValues[1] = "actual";

                reportMetric(cpu, labelNames, labelValues, thisPct, "CPU");

            }

            labelValues[0] = "all";
            labelValues[1] = "max";
            reportMetric(cpuMax, labelNames, labelValues, maxPct, "CPUMax");
            labelValues[1] = "min";
            reportMetric(cpuMin, labelNames, labelValues, maxPct, "CPUMin");

        }
    }

    private int reportCPU(CountDownLatch cbwaiters) throws IOException, ProcCallException {
        ReportCPUCallback cb = new ReportCPUCallback(cbwaiters, m_helpText, m_labelText);
        return callProcedure(cb, "@Statistics", "CPU", 0);
    }

    /**
     * We use a dedicated class for this as it needs to handle some of the columns
     * as running totals and also does procedure level stats.
     *
     * @author drolfe
     *
     */
    private final class ReportProcedureProfileCallback extends AbstractPersistentStatsProcedureCallback {

        public ReportProcedureProfileCallback(HashMap<String, String> helpText, HashMap<String, String> m_labelText) {
            super("PP", helpText, m_labelText);

            deltaColumns.put("VoltDB/PP/INVOCATIONS", PATTERN_EXACT);
            deltaColumns.put("VoltDB/PP/ABORTS", PATTERN_EXACT);
            deltaColumns.put("VoltDB/PP/FAILURES", PATTERN_EXACT);

            deltaColumns.put("VoltDB/PPProc/INVOCATIONS/.*", PATTERN_REGEX);
            deltaColumns.put("VoltDB/PPProc/ABORTS/.*", PATTERN_REGEX);
            deltaColumns.put("VoltDB/PPProc/FAILURES/.*", PATTERN_REGEX);

        }

        /**
         * This method handles default processing of statistics aggregrating them on
         * column and reporting to newrelic. Namespace define what the metric reported
         * as e.g. "Components/VoltDB/" + namespace + "MetricName"
         *
         * @param table
         * @param namespace
         */
        @Override
        public void processResult(VoltTable table, String namespace) {

            final String PROC_SUFFIX = "Proc";

            Map<String, Number> procValue = new HashMap<>();

            for (int r = 0; r < table.getRowCount(); r++) {

                if (!table.advanceRow()) {
                    continue;
                }

                int ccount = table.getColumnCount();

                String procedureName = getProcName(table.getString("PROCEDURE"));

                for (int i = 0; i < ccount; i++) {
                    VoltType vtype = table.getColumnType(i);
                    String cname = table.getColumnName(i);
                    if (cname.equalsIgnoreCase("timestamp") || cname.equalsIgnoreCase("site_id")
                            || cname.equalsIgnoreCase("PARTITION_ID")
                            // || cname.equalsIgnoreCase("WEIGHTED_PERC")
                            || cname.equalsIgnoreCase("host_id")) {
                        continue;
                    }

                    // Keep track of both total workload and workload by
                    // procedure
                    String key = "VoltDB/" + namespace + "/" + cname;
                    String procedureKey = "VoltDB/" + namespace + PROC_SUFFIX + "/" + cname + "/" + procedureName;

                    Number oldVal = null;
                    Number cvalue = null;

                    Number oldProcedureVal = null;
                    Number procedureCValue = null;

                    // Only a subset of stats we have make sense to aggregate...
                    // WEIGHTED_PERC, AVG, MIN and MAX are left alone...
                    if (cname.equalsIgnoreCase("INVOCATIONS") || cname.equalsIgnoreCase("ABORTS")
                            || cname.equalsIgnoreCase("FAILURES")) {
                        oldVal = procValue.get(key);
                        oldProcedureVal = procValue.get(procedureKey);
                    }

                    switch (vtype) {
                    case INTEGER:
                    case BIGINT:
                    case SMALLINT:
                        Long clvalue = table.getLong(i);
                        Long clProcedurVvalue = table.getLong(i);

                        if (oldVal != null) {
                            clvalue += oldVal.longValue();
                        }
                        cvalue = clvalue;

                        if (oldProcedureVal != null) {
                            clProcedurVvalue += oldProcedureVal.longValue();
                        }
                        procedureCValue = clProcedurVvalue;

                        break;
                    case FLOAT:
                        Double cdvalue = table.getDouble(i);
                        if (oldVal != null) {
                            cdvalue += oldVal.doubleValue();
                        }
                        cvalue = cdvalue;
                        break;
                    default:
                        cname = null;
                    }
                    if (cname != null) {
                        procValue.put(key, cvalue);
                        procValue.put(procedureKey, procedureCValue);
                        m_labelText.put(procedureKey, makeDefaultLabel(cname));
                        helpText.put(procedureKey,
                                "See the PROCEDUREPROFILE section of @Statistics for more information");

                    }
                }
            }
            statReportForTest.incrementAndGet();
            if (valuesFromPriorCalls != null) {
                for (String key : procValue.keySet()) {
                    long theValue = procValue.get(key).longValue();
                    theValue -= calculateOldValue(key, valuesFromPriorCalls.get(key));

                    theValue = getDeltaPerSecond(theValue);
                    reportMetric(key.replace("$", "_"), "value", new Long(theValue));

                }

            }

            valuesFromPriorCalls = procValue;
        }

    }

    private int reportProcedureProfile(CountDownLatch cbwaiters)
            throws IOException, NoConnectionsException, ProcCallException {

        reportProcedureProfileCB.setWaiters(cbwaiters);
        return callProcedure(reportProcedureProfileCB, "@Statistics", "PROCEDUREPROFILE", 0);
    }

    /**
     * We use a custom processResult method as we need table level stats
     *
     * @author drolfe
     *
     */
    private final class ReportTableCallback extends AbstractStatsProcedureCallback {

        public ReportTableCallback(CountDownLatch cbwaiters, HashMap<String, String> helpText,
                HashMap<String, String> m_labelText) {
            super(cbwaiters, "Table", helpText, m_labelText);
        }

        /**
         * This method handles default processing of statistics aggregrating them on
         * column and reporting to newrelic. Namespace define what the metric reported
         * as e.g. "Components/VoltDB/" + namespace + "MetricName"
         *
         * @param table
         * @param namespace
         */
        public void processResultFirstPass(VoltTable table, String namespace) {
            Map<String, Number> procValue = new HashMap<>();
            for (int r = 0; r < table.getRowCount(); r++) {
                if (!table.advanceRow()) {
                    continue;
                }
                int ccount = table.getColumnCount();
                for (int i = 0; i < ccount; i++) {
                    VoltType vtype = table.getColumnType(i);
                    String cname = table.getColumnName(i);
                    if (cname.equalsIgnoreCase("timestamp") || cname.equalsIgnoreCase("site_id")
                            || cname.equalsIgnoreCase("PARTITION_ID") || cname.equalsIgnoreCase("WEIGHTED_PERC")
                            || cname.equalsIgnoreCase("host_id")) {
                        continue;
                    }
                    String key = "VoltDB/" + namespace + "/" + cname;
                    Number oldVal = procValue.get(key);
                    Number cvalue = null;
                    switch (vtype) {
                    case INTEGER:
                    case BIGINT:
                    case SMALLINT:
                        Long clvalue = table.getLong(i);
                        if (oldVal != null) {
                            clvalue += oldVal.longValue();
                        }
                        cvalue = clvalue;
                        break;
                    case FLOAT:
                        Double cdvalue = table.getDouble(i);
                        if (oldVal != null) {
                            cdvalue += oldVal.doubleValue();
                        }
                        cvalue = cdvalue;
                        break;
                    default:
                        cname = null;
                    }
                    if (cname != null) {
                        procValue.put(key, cvalue);
                        m_labelText.put(key, makeDefaultLabel(cname + " for all tables"));
                        helpText.put(key, "See the TABLE section of @Statistics for more information");
                    }
                }
            }
            statReportForTest.incrementAndGet();
            for (String key : procValue.keySet()) {
                reportMetric(key, "value", procValue.get(key));

            }
        }

        @Override
        public void processResult(VoltTable table, String namespace) {

            // First call this routine to get system level table totals...
            processResultFirstPass(table, namespace);

            // get ready for another pass
            table.resetRowPosition();

            // Now do it again, but this time break it down by table.
            Map<String, Number> procValue = new HashMap<>();
            for (int r = 0; r < table.getRowCount(); r++) {
                if (!table.advanceRow()) {
                    continue;
                }

                String tableName = table.getString("TABLE_NAME");

                int ccount = table.getColumnCount();
                for (int i = 0; i < ccount; i++) {
                    VoltType vtype = table.getColumnType(i);
                    String cname = table.getColumnName(i);
                    if (cname.equalsIgnoreCase("timestamp") || cname.equalsIgnoreCase("site_id")
                            || cname.equalsIgnoreCase("PARTITION_ID") || cname.equalsIgnoreCase("WEIGHTED_PERC")
                            || cname.equalsIgnoreCase("host_id")) {
                        continue;
                    }

                    String key = "VoltDB/" + namespace + "/" + tableName + "/" + cname;
                    Number oldVal = null;

                    // Do not do rolling addition of 'Percent full' or tuple
                    // limit - take last
                    // number we see...
                    if (!cname.equalsIgnoreCase("PERCENT_FULL") && !cname.equalsIgnoreCase("TUPLE_LIMIT")) {
                        oldVal = procValue.get(key);
                    }

                    Number cvalue = null;
                    switch (vtype) {
                    case INTEGER:
                    case BIGINT:
                    case SMALLINT:
                        Long clvalue = table.getLong(i);
                        if (oldVal != null) {
                            clvalue += oldVal.longValue();
                        }
                        cvalue = clvalue;
                        break;

                    default:
                        cname = null;
                    }

                    if (cname != null) {

                        // Sanity check -- skip Null values
                        if (cvalue.longValue() > Integer.MIN_VALUE) {

                            procValue.put(key, cvalue);
                            m_labelText.put(key, makeDefaultLabel(cname + " for " + tableName));
                            helpText.put(key, "See the TABLE section of @Statistics for more information");

                        }
                    }

                }
            }
            statReportForTest.incrementAndGet();
            for (String key : procValue.keySet()) {

                reportMetric(key, "value", procValue.get(key));

            }
        }

    }

    private int reportTable(CountDownLatch cbwaiters) throws IOException, NoConnectionsException, ProcCallException {
        ReportTableCallback cb = new ReportTableCallback(cbwaiters, m_helpText, m_labelText);
        return callProcedure(cb, "@Statistics", "TABLE", 0);
    }

    private final class ReportCommandLogCallback extends AbstractStatsProcedureCallback {

        public ReportCommandLogCallback(CountDownLatch cbwaiters, HashMap<String, String> helpText,
                HashMap<String, String> m_labelText) {
            super(cbwaiters, "CommandLog", helpText, m_labelText);
        }

        @Override
        public void processResult(VoltTable table, String namespace) {
            Map<String, Number> procValue = new HashMap<>();
            for (int r = 0; r < table.getRowCount(); r++) {
                if (!table.advanceRow()) {
                    continue;
                }
                int ccount = table.getColumnCount();
                for (int i = 0; i < ccount; i++) {
                    VoltType vtype = table.getColumnType(i);
                    String cname = table.getColumnName(i);
                    if (cname.equalsIgnoreCase("timestamp") || cname.equalsIgnoreCase("site_id")
                            || cname.equalsIgnoreCase("PARTITION_ID") || cname.equalsIgnoreCase("WEIGHTED_PERC")
                            || cname.equalsIgnoreCase("host_id")) {
                        continue;
                    }
                    String key = "VoltDB/" + namespace + "/" + cname;
                    Number oldVal = null;
                    Number cvalue = null;

                    // Only a subset of stats we have make sense to aggregate...
                    // Everything else is left alone...
                    if (!cname.equalsIgnoreCase("FSYNC_INTERVAL")) {
                        oldVal = procValue.get(key);
                    }

                    switch (vtype) {
                    case INTEGER:
                    case BIGINT:
                    case SMALLINT:
                        Long clvalue = table.getLong(i);
                        if (oldVal != null) {
                            clvalue += oldVal.longValue();
                        }
                        cvalue = clvalue;
                        break;
                    case FLOAT:
                        Double cdvalue = table.getDouble(i);
                        if (oldVal != null) {
                            cdvalue += oldVal.doubleValue();
                        }
                        cvalue = cdvalue;
                        break;
                    default:
                        cname = null;
                    }
                    if (cname != null) {
                        procValue.put(key, cvalue);
                        m_labelText.put(key, makeDefaultLabel(cname));
                        helpText.put(key, "See the COMMANDLOG section of @Statistics for more information");
                    }
                }
            }
            statReportForTest.incrementAndGet();
            for (String key : procValue.keySet()) {
                reportMetric(key, "value", procValue.get(key));
                m_labelText.put(key, makeDefaultLabel(key));

            }

        }

    }

    private int reportCommandLog(CountDownLatch cbwaiters)
            throws IOException, NoConnectionsException, ProcCallException {
        ReportCommandLogCallback cb = new ReportCommandLogCallback(cbwaiters, m_helpText, m_labelText);
        return callProcedure(cb, "@Statistics", "COMMANDLOG", 0);
    }

    private final class ReportSnapshotCallback extends AbstractStatsProcedureCallback {

        private static final long POLL_INTERVAL = 600 * 1000;

        public ReportSnapshotCallback(CountDownLatch cbwaiters, HashMap<String, String> helpText,
                HashMap<String, String> m_labelText) {
            super(cbwaiters, "Snapshot", helpText, m_labelText);
        }

        /**
         * Non-trivial processResult. We need to find a single snapshot that finished in
         * the last pool interval and report on it. 'table' is de-normalized in this
         * case.
         */
        @Override
        public void processResult(VoltTable table, String namespace) {

            // First pass - go through and find the most recent snapshot...

            table.resetRowPosition();

            long lastSnapshotEndTimeMs = 0;

            while (table.advanceRow()) {
                if (table.getLong("END_TIME") > lastSnapshotEndTimeMs) {
                    lastSnapshotEndTimeMs = table.getLong("END_TIME");
                }
            }

            // See if last snapshot took place within the last 60 seconds...
            if (lastSnapshotEndTimeMs > (System.currentTimeMillis() - POLL_INTERVAL)) {

                table.resetRowPosition();
                Map<String, Number> procValue = new HashMap<>();

                long priorSnapshotEndTime = 0;
                long lastSnapshotBeginTime = 0;
                Long durationSeconds = null;

                while (table.advanceRow()) {

                    if (table.getLong("END_TIME") == lastSnapshotEndTimeMs) {

                        // Report how long this latest snapshot took...

                        durationSeconds = new Long(table.getLong("DURATION"));
                        Double throughputBytesPerSecond = new Double(table.getDouble("THROUGHPUT"));

                        String key = "VoltDB/" + namespace + "/";
                        procValue.put(key + "seconds/durationSeconds", durationSeconds);
                        procValue.put(key + "io/throughputBytesPerSecond", throughputBytesPerSecond);

                        lastSnapshotBeginTime = table.getLong("START_TIME");
                        break;

                    }
                }

                // Now find oldest prior snapshot ...
                table.resetRowPosition();
                while (table.advanceRow()) {

                    if (table.getLong("END_TIME") > priorSnapshotEndTime
                            && table.getLong("END_TIME") < lastSnapshotBeginTime) {
                        priorSnapshotEndTime = table.getLong("END_TIME");
                    }
                }

                // ... and calculate time not spent doing snapshots...

                // if we have > 1 snapshots...
                if (priorSnapshotEndTime > 0) {

                    float snapshottimePct = 0;

                    if (durationSeconds > 0) {

                        long quietSeconds = (lastSnapshotBeginTime - priorSnapshotEndTime) / 1000;
                        procValue.put("VoltDB/" + namespace + "/seconds/quietSeconds", quietSeconds);
                        long totalSeconds = (durationSeconds.longValue() + quietSeconds);
                        snapshottimePct = ((durationSeconds * 100) / totalSeconds);

                    }

                    procValue.put("VoltDB/" + namespace + "/pct/snapshottimePct", snapshottimePct);
                    reportMetric("VoltDB/Dashboard/snapshottimePct", "ms", snapshottimePct);

                }

                statReportForTest.incrementAndGet();
                for (String key : procValue.keySet()) {
                    reportMetric(key, "value", procValue.get(key));

                }

            }

        }

    }

    private int reportSnapshot(CountDownLatch cbwaiters) throws IOException, NoConnectionsException, ProcCallException {
        ReportSnapshotCallback cb = new ReportSnapshotCallback(cbwaiters, m_helpText, m_labelText);
        return callProcedure(cb, "@Statistics", "SNAPSHOTSTATUS", 0);
    }

    private final class ReportIoStatsCallback extends AbstractPersistentStatsProcedureCallback {

        public ReportIoStatsCallback(HashMap<String, String> helpText, HashMap<String, String> m_labelText) {
            super("IoStats", helpText, m_labelText);

            deltaColumns.put("VoltDB/IoStats/Messages/MESSAGES_READ", PATTERN_EXACT);
            deltaColumns.put("VoltDB/IoStats/Bytes/BYTES_READ", PATTERN_EXACT);
            deltaColumns.put("VoltDB/IoStats/Bytes/BYTES_WRITTEN", PATTERN_EXACT);
            deltaColumns.put("VoltDB/IoStats/Messages/MESSAGES_WRITTEN", PATTERN_EXACT);

            m_labelText.put("VoltDB/IoStats/Messages/MESSAGES_READ", "Messages Read");
            m_labelText.put("VoltDB/IoStats/Bytes/BYTES_READ", "Bytes Read");
            m_labelText.put("VoltDB/IoStats/Bytes/BYTES_WRITTEN", "Bytes Written");
            m_labelText.put("VoltDB/IoStats/Messages/MESSAGES_WRITTEN", "Messages Written");

            helpText.put("VoltDB/IoStats/Messages/MESSAGES_READ", "Messages Read - See IOStats in @Statistics");
            helpText.put("VoltDB/IoStats/Bytes/BYTES_READ", "Bytes Read - See IOStats in @Statistics");
            helpText.put("VoltDB/IoStats/Bytes/BYTES_WRITTEN", "Bytes Written - See IOStats in @Statistics");
            helpText.put("VoltDB/IoStats/Messages/MESSAGES_WRITTEN", "Messages Written - See IOStats in @Statistics");

        }

        @Override
        public void processResult(VoltTable table, String namespace) {
            Map<String, Number> procValue = new HashMap<>();
            for (int r = 0; r < table.getRowCount(); r++) {
                if (!table.advanceRow()) {
                    continue;
                }
                int ccount = table.getColumnCount();
                for (int i = 0; i < ccount; i++) {
                    VoltType vtype = table.getColumnType(i);
                    String cname = table.getColumnName(i);
                    if (cname.equalsIgnoreCase("timestamp") || cname.equalsIgnoreCase("CONNECTION_ID")
                            || cname.equalsIgnoreCase("CONNECTION_HOST​NAME") || cname.equalsIgnoreCase("host_id")
                            || cname.equalsIgnoreCase("HOSTNAME")) {
                        continue;
                    }
                    String key = "VoltDB/" + namespace;

                    if (cname.startsWith("MESSAGES")) {
                        key = key + "/" + "Messages";
                    } else if (cname.startsWith("BYTES")) {
                        key = key + "/" + "Bytes";
                    }

                    key = key + "/" + cname;
                    Number oldVal = procValue.get(key);
                    Number cvalue = null;

                    switch (vtype) {
                    case INTEGER:
                    case BIGINT:
                    case SMALLINT:
                        Long clvalue = table.getLong(i);
                        if (oldVal != null) {
                            clvalue += oldVal.longValue();
                        }
                        cvalue = clvalue;
                        break;
                    case FLOAT:
                        Double cdvalue = table.getDouble(i);
                        if (oldVal != null) {
                            cdvalue += oldVal.doubleValue();
                        }
                        cvalue = cdvalue;
                        break;
                    default:
                        cname = null;
                    }
                    if (cname != null) {
                        procValue.put(key, cvalue);
                    }
                }
            }
            statReportForTest.incrementAndGet();

            if (valuesFromPriorCalls != null) {
                for (String key : procValue.keySet()) {
                    long theValue = procValue.get(key).longValue();
                    theValue -= calculateOldValue(key, valuesFromPriorCalls.get(key));

                    theValue = getDeltaPerSecond(theValue);

                    if (theValue >= 0) {
                        reportMetric(key, "value", new Long(theValue));
                    }

                }

            }

            valuesFromPriorCalls = procValue;

        }

    }

    private int reportIoStats(CountDownLatch cbwaiters) throws IOException, NoConnectionsException, ProcCallException {

        iostatsCallback.setWaiters(cbwaiters);
        return callProcedure(iostatsCallback, "@Statistics", "IOSTATS", 0);
    }

    class ReportLatencyCallback extends AbstractStatsProcedureCallback {
        public ReportLatencyCallback(CountDownLatch cbwaiters, HashMap<String, String> helpText,
                HashMap<String, String> m_labelText) {
            super(cbwaiters, "Latency", helpText, m_labelText);
        }

        @Override
        public void processResult(VoltTable table, String namespace) {
            final String[] columns = { "P50", "P95", "P99", "P99.9", "P99.99", "P99.999", "MAX" };
            String[] labelNames = { "hostname", "measurement" };

            while (table.advanceRow()) {
                String[] labelValues = { null, null };

                for (int i = 0; i < columns.length; i++) {

                    labelValues[0] = new String(table.getString("HOSTNAME"));
                    labelValues[1] = new String(columns[i]);
                    reportMetric("voltdb_latency", labelNames, labelValues, table.getLong(columns[i]),
                            "voltdb_latency");
                    m_labelText.put("voltdb_latency", columns[i] + " percentile latency");
                    helpText.put("voltdb_latency", columns[i] + " percentile latency");

                }

                labelValues[0] = new String(table.getString("HOSTNAME"));
                labelValues[1] = new String("TPS");
                reportMetric("voltdb_hostname_tps", labelNames, labelValues, table.getLong("TPS"), "voltdb_tps");
                m_labelText.put("voltdb_hostname_tps", "voltdb_hostname_tps");
                helpText.put("voltdb_hostname_tps", "voltdb_hostname_tps");

            }
        }
    }

    private int reportLatency(CountDownLatch cbwaiters) throws IOException, NoConnectionsException, ProcCallException {
        ReportLatencyCallback cb = new ReportLatencyCallback(cbwaiters, m_helpText, m_labelText);
        return callProcedure(cb, "@Statistics", "LATENCY", 0);
    }

    /**
     * 
     * class ReportLatencyCallback extends AbstractStatsProcedureCallback {
     * 
     * @param cbwaiters
     * @param helpText
     * @param m_labelText
     */
    /*
     * public ReportLatencyCallback(CountDownLatch cbwaiters, HashMap<String,
     * String> helpText, HashMap<String, String> m_labelText) { super(cbwaiters,
     * "Latency", helpText, m_labelText); }
     * 
     * @Override public void processResult(VoltTable table, String namespace) {
     * table.advanceRow(); Histogram hist =
     * AbstractHistogram.fromCompressedBytes(table.getVarbinary(4),
     * CompressionStrategySnappy.INSTANCE); double latency;
     * 
     * if (m_histogram == null) { m_histogram = hist; } else {
     * hist.subtract(m_histogram); m_histogram =
     * AbstractHistogram.fromCompressedBytes(table.getVarbinary(4),
     * CompressionStrategySnappy.INSTANCE); }
     * 
     * statReportForTest.incrementAndGet(); double[] measuredvalues = { 50D, 99D,
     * 99.9D, 99.99D, 99.999D };
     * 
     * for (int i = 0; i < measuredvalues.length; i++) {
     * 
     * String iAsAsString = measuredvalues[i] + ""; iAsAsString =
     * iAsAsString.replace(".", "dot");
     * 
     * latency = hist.getValueAtPercentile(i);
     * 
     * reportMetric("VoltDB/Latency_" + iAsAsString, "milliseconds", latency /
     * 1000.0D); m_labelText.put("VoltDB/Latency" + iAsAsString,
     * "99%th percentile latency"); helpText.put("VoltDB/Latency" + iAsAsString,
     * "99%th percentile latency");
     * 
     * reportMetric("VoltDB/Dashboard/LatencyMS" + iAsAsString, "ms", latency /
     * 1000.0D); m_labelText.put("VoltDB/Dashboard/LatencyMS" + iAsAsString,
     * "99%th percentile latency"); helpText.put("VoltDB/Dashboard/LatencyMS" +
     * iAsAsString, "99%th percentile latency"); } } }
     * 
     * private int reportLatency(CountDownLatch cbwaiters) throws IOException,
     * NoConnectionsException, ProcCallException { ReportLatencyCallback cb = new
     * ReportLatencyCallback(cbwaiters, m_helpText, m_labelText); return
     * callProcedure(cb, "@Statistics", "LATENCY_HISTOGRAM", 0); }
     * 
     **/

    class ReportLiveClientsCallback extends AbstractStatsProcedureCallback {

        public ReportLiveClientsCallback(CountDownLatch cbwaiters, HashMap<String, String> helpText,
                HashMap<String, String> m_labelText) {
            super(cbwaiters, "Connections", helpText, m_labelText);
        }

        @Override
        public void processResult(VoltTable table, String namespace) {
            if (table == null) {
                return;
            }

            String[] labelNames = { "stattype" };
            String[] valueNames = { "" };

            final String key = "voltdb_connections";

            // Take out our own connection.
            long count = table.getRowCount() - 1;
            statReportForTest.incrementAndGet();
            m_labelText.put(key, "Database connections");
            helpText.put(key, "Number of connections - see LIVECLIENTS in @Statistics ");

            valueNames[0] = "connections";
            reportMetric(key, labelNames, valueNames, count, "count");

            long txCount = 0;
            long responseCount = 0;

            while (table.advanceRow()) {
                txCount += table.getLong("OUTSTANDING_TRANSACTIONS");
                responseCount += table.getLong("OUTSTANDING_RESPONSE_MESSAGES");
            }

            String[] valueNames2 = { "OUTSTANDING_TRANSACTIONS" };
            reportMetric(key, labelNames, valueNames2, txCount, "tx");

            String[] valueNames3 = { "OUTSTANDING_RESPONSE_MESSAGES" };
            reportMetric(key, labelNames, valueNames3, responseCount, "msg");

        }
    }

    /**
     * This needs a custom class as it generates stats for each sql statement in
     * each proc.
     *
     * @author drolfe
     *
     */
    private final class ReportProcedureProfileDetailCallback extends AbstractPersistentStatsProcedureCallback {

        long thresholdNs;

        public ReportProcedureProfileDetailCallback(long thresholdNs, HashMap<String, String> helpText,
                HashMap<String, String> m_labelText) {
            super("PPDetail", helpText, m_labelText);
            this.thresholdNs = thresholdNs;

        }

        /**
         * This method does SQL statement level statistics.
         *
         * @param table
         * @param namespace
         * @author drolfe
         */
        @Override
        public void processResult(VoltTable table, String namespace) {

            final String prefix = "VoltDB/PPDetailProc/";
            final String PROC_SUFFIX = "Proc";

            Map<String, Number> procValue = new HashMap<>();
            for (int r = 0; r < table.getRowCount(); r++) {

                if (!table.advanceRow()) {
                    continue;
                }

                long avgExecTime = table.getLong("AVG_EXECUTION_TIME");
                String statementName = getStatementName(table.getString("STATEMENT"));

                if (avgExecTime >= thresholdNs && !statementName.equals("All")) {

                    int ccount = table.getColumnCount();

                    String procedureName = getProcName(table.getString("PROCEDURE"));

                    for (int i = 0; i < ccount; i++) {
                        VoltType vtype = table.getColumnType(i);
                        String cname = table.getColumnName(i);
                        if (cname.equalsIgnoreCase("timestamp") || cname.equalsIgnoreCase("site_id")
                                || cname.equalsIgnoreCase("PARTITION_ID") || cname.equalsIgnoreCase("HOSTNAME")
                                || cname.equalsIgnoreCase("AVG_PARAMETER_SET_SIZE")
                                || cname.equalsIgnoreCase("MIN_PARAMETER_SET_SIZE")
                                || cname.equalsIgnoreCase("MAX_PARAMETER_SET_SIZE")
                                || cname.equalsIgnoreCase("AVG_RESULT_SIZE")
                                || cname.equalsIgnoreCase("MIN_RESULT_SIZE")
                                || cname.equalsIgnoreCase("MAX_RESULT_SIZE") || cname.equalsIgnoreCase("host_id")) {
                            continue;
                        }

                        // Keep track of both total workload and workload by
                        // procedure
                        String procedureDetailKey = "VoltDB/" + namespace + PROC_SUFFIX + "/" + cname + "/"
                                + procedureName + "." + statementName;

                        Number oldProcedureVal = null;
                        Number procedureCValue = null;

                        // Only a subset of stats we have make sense to
                        // aggregate...
                        // Everything else is left alone...
                        if (cname.equalsIgnoreCase("INVOCATIONS") || cname.equalsIgnoreCase("TIMED_INVOCATIONS")
                                || cname.equalsIgnoreCase("ABORTS") || cname.equalsIgnoreCase("FAILURES")) {
                            oldProcedureVal = procValue.get(procedureDetailKey);
                        }

                        switch (vtype) {
                        case INTEGER:
                        case BIGINT:
                        case SMALLINT:
                            Long clvalue = table.getLong(i);
                            Long clProcedurVvalue = table.getLong(i);

                            if (oldProcedureVal != null) {
                                clProcedurVvalue += oldProcedureVal.longValue();
                            }
                            procedureCValue = clProcedurVvalue;

                            break;

                        default:
                            cname = null;
                        }
                        if (cname != null) {
                            procValue.put(procedureDetailKey, procedureCValue);
                            m_labelText.put(procedureDetailKey,
                                    "SQL Statement " + statementName + " in procedure " + procedureName);
                            helpText.put(procedureDetailKey,
                                    "See the PROCEDUREDETAIL section of @Statistics for more information");

                        }
                    }
                }

            }

            statReportForTest.incrementAndGet();
            if (valuesFromPriorCalls != null) {
                for (String key : procValue.keySet()) {
                    long theValue = procValue.get(key).longValue();
                    if (isCounter(prefix, key)) {
                        theValue -= calculateOldValue(key, valuesFromPriorCalls.get(key));
                        theValue = getDeltaPerSecond(theValue);
                    }

                    reportMetric(key, "value", new Long(theValue));

                }

            }

            valuesFromPriorCalls = procValue;

        }

        private boolean isCounter(String prefix, String key) {

            if (key.startsWith(prefix + "INVOCATIONS") || key.startsWith(prefix + "TIMED_INVOCATIONS")
                    || key.startsWith(prefix + "FAILURES") || key.startsWith(prefix + "ABORTS")) {
                return true;
            }

            return false;
        }

        private String getStatementName(String statementName) {

            if (statementName.equalsIgnoreCase("<ALL>")) {
                statementName = "All";
            }

            return statementName;

        }

    }

    private int reportProcedureProfileDetail(CountDownLatch cbwaiters)
            throws IOException, NoConnectionsException, ProcCallException {
        reportProcedureProfileDetailCB.setWaiters(cbwaiters);
        return callProcedure(reportProcedureProfileDetailCB, "@Statistics", "PROCEDUREDETAIL", 0);
    }

    private final class ReportIndexCallback extends AbstractStatsProcedureCallback implements ProcedureCallback {

        long indexThreshhold = 0;

        public ReportIndexCallback(CountDownLatch cbwaiters, HashMap<String, String> helpText,
                HashMap<String, String> m_labelText, long indexThreshhold) {
            super(cbwaiters, "Index", helpText, m_labelText);
            this.indexThreshhold = indexThreshhold;
        }

        @Override
        public void processResult(VoltTable table, String namespace) {

            final String totalKey = "VoltDB/" + namespace + "/" + "totalIndexUsed";

            Map<String, Long> procValue = new HashMap<>();

            long totalIndexMem = 0;

            for (int rc = 0; rc < table.getRowCount(); rc++) {
                if (!table.advanceRow()) {
                    return;
                }

                String indexName = table.getString("INDEX_NAME");

                // PK for a matview always has 'MATVIEW_PK_INDEX' as index name...
                if (indexName.equals("MATVIEW_PK_INDEX")) {
                    indexName = indexName + "_for_" + table.getString("TABLE_NAME");
                }

                String rowKey = "VoltDB/" + namespace + "/Rows/" + indexName;
                String kbKey = "VoltDB/" + namespace + "/Kb/" + indexName;

                Long oldRows = procValue.get(rowKey);

                if (oldRows == null) {
                    oldRows = new Long(0);
                }

                Long oldKb = procValue.get(kbKey);

                if (oldKb == null) {
                    oldKb = new Long(0);
                }

                totalIndexMem += table.getLong("MEMORY_ESTIMATE");

                procValue.put(rowKey, oldRows.longValue() + table.getLong("ENTRY_COUNT"));
                m_labelText.put(rowKey, "Entries in index " + indexName);
                helpText.put(rowKey, "See INDEX in @Statistics ");

                procValue.put(kbKey, oldKb.longValue() + table.getLong("MEMORY_ESTIMATE"));
                m_labelText.put(kbKey, "KB used by index " + indexName);
                helpText.put(kbKey, "See INDEX in @Statistics ");

            }

            for (String key : procValue.keySet()) {

                if (procValue.get(key).longValue() > indexThreshhold) {

                    if (key.startsWith("VoltDB/Index/Rows")) {
                        reportMetric(key, "rows", procValue.get(key));
                    } else {
                        reportMetric(key, "kb", procValue.get(key));
                    }

                }
            }

            reportMetric(totalKey, "kb", totalIndexMem);
            m_labelText.put(totalKey, "Total KB used by indexes");
            helpText.put(totalKey, "See INDEX in @Statistics ");

        }
    }

    private int reportIndex(CountDownLatch cbwaiters, long indexThreshhold) throws IOException, ProcCallException {
        ReportIndexCallback cb = new ReportIndexCallback(cbwaiters, m_helpText, m_labelText, indexThreshhold);
        return callProcedure(cb, "@Statistics", "INDEX", 0);
    }

    /**
     * Get number of connected clients and report connection count to newrelic.
     *
     * @param cbwaiters
     * @return
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    private int reportNumberOfClients(CountDownLatch cbwaiters)
            throws IOException, NoConnectionsException, ProcCallException {
        ReportLiveClientsCallback cb = new ReportLiveClientsCallback(cbwaiters, m_helpText, m_labelText);
        return callProcedure(cb, "@Statistics", "LIVECLIENTS", 0);
    }

    private int callProcedure(AbstractStatsProcedureCallback cb, String procName, String param0, int interval)
            throws IOException {
        try {
            m_client.callProcedure(cb, procName, param0, interval);
            return 0;
        } catch (Exception ex) {
            logger.error("Failed to call stats procedure.");
            if (cb.cbwaiters != null) {
                cb.cbwaiters.countDown();
            }
        }
        return 1;
    }

    private int callProcedure(AbstractPersistentStatsProcedureCallback cb, String procName, String param0, int interval)
            throws IOException {
        try {
            m_client.callProcedure(cb, procName, param0, interval);
            return 0;
        } catch (Exception ex) {
            logger.error("Failed to call stats procedure.");
            if (cb.cbwaiters != null) {
                cb.cbwaiters.countDown();
            }
        }
        return 1;
    }

    /**
     * Create Client if it has not been created, and connect to the servers if we
     * fail we will try again in next polling cycle.
     *
     * @throws UnknownHostException
     * @throws IOException
     */
    private void checkedClient() throws UnknownHostException, IOException {
        if (m_client == null) {
            m_client = ThirdPartyMetricEngineInterface.createClient(this);
        }
    }

    /**
     * This method is called by an external class every 'n' sec for each component
     * defined in JSON document. We get Client Connection if it does not exists and
     * any failure we dump m_client and retry on next attempt.
     */

    public int gatherMetrics() {

        final int STAT_COUNT = 13;
        int errCnt = 0;

        try {

            checkedClient();
            assert m_client != null;
            logInfo("VoltDBGenericMetricsAgent: Starting Metrics Collection for server: " + getServers());

            if (clusterId == CLUSTERID_UNKNOWN) {
                clusterId = getClusterId();
            }

            m_statCount = 0;
            final long starttimeMS = System.currentTimeMillis();

            final CountDownLatch cbwaiters = new CountDownLatch(STAT_COUNT);
            errCnt += reportProcedureProfileDetail(cbwaiters);
            errCnt += reportMemory(cbwaiters);
            errCnt += reportTable(cbwaiters);
            errCnt += reportNumberOfClients(cbwaiters);
            errCnt += reportLatency(cbwaiters);
            errCnt += reportProcedureProfile(cbwaiters);

            errCnt += reportCommandLog(cbwaiters);
            errCnt += reportIoStats(cbwaiters);
            errCnt += reportSnapshot(cbwaiters);
            errCnt += reportCPU(cbwaiters);

            errCnt += reportIndex(cbwaiters, DEFAULT_IDX_THRESH);

            errCnt += reportExport(cbwaiters);
            errCnt += reportImport(cbwaiters);

            // Lets wait for callbacks to finish.
            m_client.drain();
            cbwaiters.await();
            if (errCnt > 0) {
                m_client.close();
                m_client = null;
                logError(
                        "VoltDBGenericMetricsAgent: Error in collecting Metrics Collection for server, will reconnect on next polling cycle.: "
                                + m_metricEngine.getServers());
            }

            logInfo("VoltDBGenericMetricsAgent: End Metrics Collection for server: " + m_metricEngine.getServers()
                    + "; collected " + m_statCount + " stats in " + (System.currentTimeMillis() - starttimeMS) + "ms.");

        } catch (Exception nc) {
            logError("VoltDBGenericMetricsAgent: Failed to poll for statistics for server : "
                    + m_metricEngine.getServers() + " " + nc.getMessage());
            // We failed in collecting reconnect to m_client.
            if (m_client != null) {
                try {
                    m_client.drain();
                    m_client.close();
                    m_client = null;
                } catch (NoConnectionsException ex) {
                    m_client = null;
                } catch (InterruptedException ex) {
                    m_client = null;
                }
            }
        }

        return errCnt;
    }

    private int getClusterId() {
        try {
            ClientResponse cr = m_client.callProcedure("@SystemInformation", "overview");
            while (cr.getResults()[0].advanceRow()) {
                if (cr.getResults()[0].getString("KEY").equals("CLUSTERID")) {
                    return Integer.parseInt(cr.getResults()[0].getString("VALUE"));
                }
            }

            return CLUSTERID_UNKNOWN;

        } catch (Exception ex) {
            logger.error("Failed to call getClusterId: " + ex.getMessage());
            return CLUSTERID_UNKNOWN;
        }
    }

    /**
     * Return user friendly procedure name
     *
     * @param procnameWithDots
     * @return
     * @author drolfe
     */
    public static String getProcName(String procnameWithDots) {
        String[] procnameArray = procnameWithDots.split(Pattern.quote("."));
        return procnameArray[procnameArray.length - 1];
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {

        if (m_client != null) {
            m_client.drain();
            m_client.close();
            m_client = null;
        }
        super.finalize();
    }

    public String getLabel(String metricName) {

        String label = m_labelText.get(metricName);

        if (label != null) {
            return label;
        }

        return metricName;
    }

    public String getHelp(String metricName) {

        String help = m_helpText.get(metricName);

        if (help != null) {
            return help;
        }

        return metricName;
    }

    public static String makeDefaultLabel(String metricName) {

        return metricName.replace("/", " ").replace(".", "").toLowerCase();
    }

    public static void logInfo(String s) {
        logger.info(s);
    }

    public static void logError(String s) {
        logger.error(s);
    }

    public static void logError(Exception e) {
        logger.error(e);
    }

    public void disconnect() {
        if (m_client != null) {
            try {
                m_client.drain();
                m_client.close();
            } catch (NoConnectionsException | InterruptedException e) {
                e.printStackTrace();
            }
            m_client = null;
        }

    }

    public Client getVoltDBClient() {

        return m_client;

    }

    @Override
    public void reportMetric(String metricName, String[] labelNames, String[] labelValues, Number value, String help) {

        m_metricEngine.reportMetric(metricName, labelNames, labelValues, value, help);
        m_statCount++;

    }

}
