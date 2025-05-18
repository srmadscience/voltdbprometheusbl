/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 */

package org.voltdb.monitoring;

import com.google_voltpatches.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

/*
 * Extend this class for processing results and reporting to newrelic.
 */
public abstract class AbstractStatsProcedureCallback implements ProcedureCallback {

   /**
    * How many nanoseconds an event has to take before we report on it.
    */
   protected static final long POLL_INTERVAL = 600 * 1000;

   /**
    * How many rows or kilobytes an index has to have before we report on it.
    */
   protected static final long DEFAULT_IDX_THRESHOLD_KB = 10_000;
   protected final VoltDBGenericMetricsAgent m_agent;

   private final CountDownLatch m_cbwaiters;
   private final String m_namespace;
   protected final String m_prefixedNamespace;
   protected final Map<String, String> m_helpText;
   protected final Map<String, String> m_labelText;
   public AbstractStatsProcedureCallback(VoltDBGenericMetricsAgent agent,
         CountDownLatch cbwaiters, String namespace) {
      Preconditions.checkNotNull("CountDownLatch cannot be null", cbwaiters);
      m_agent = agent;
      m_cbwaiters = cbwaiters;
      m_namespace = namespace;
      m_prefixedNamespace = "VoltDB/" + m_namespace;
      m_helpText = m_agent.m_helpText;
      m_labelText = m_agent.m_labelText;
   }
   @Override
   public void clientCallback(ClientResponse response) throws Exception {
      try {
         if (response.getStatus() == ClientResponse.SUCCESS) {
            processResult(response.getResults()[0]);
         }
      } catch (Throwable ex) {
         VoltDBGenericMetricsAgent.logError("Failed to process stats for namespace " +
               m_namespace + ":" + ex.getMessage());
      } finally {
         m_cbwaiters.countDown();
      }
   }
   public abstract void processResult(VoltTable table);

   protected static Number updateValue(VoltTable table, int index, Number prevVal) {
      return updateValue(table, index, 1, prevVal);
   }

   protected static String makeDefaultLabel(String metricName) {
      return metricName.replace("/", " ").replace(".", "").toLowerCase();
   }

   /**
    * Reports all metrics as is to the monitoring agent.
    */
   protected void report(Map<String, Number> values) {
      values.entrySet().forEach(entry -> m_agent.reportMetric(entry.getKey(), "value", entry.getValue()));
   }

   protected static Number updateValue(VoltTable table, int index, int multiplier, Number prevVal) {
      final Number value;
      switch (table.getColumnType(index)) {
         case INTEGER:
         case BIGINT:
         case SMALLINT:
            Long clvalue = table.getLong(index) * multiplier;
            if (prevVal != null) {
               clvalue += prevVal.longValue();
            }
            value = clvalue;
            break;
         case FLOAT:
            Double cdvalue = table.getDouble(index) * multiplier;
            if (prevVal != null) {
               cdvalue += prevVal.doubleValue();
            }
            value = cdvalue;
            break;
         default:
            value = null;
      }
      return value;
   }
}

