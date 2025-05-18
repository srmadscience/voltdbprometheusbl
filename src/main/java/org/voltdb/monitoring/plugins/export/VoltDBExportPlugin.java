/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 */
package org.voltdb.monitoring.plugins.export;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.monitoring.ThirdPartyMetricEngineInterface;
import org.voltdb.monitoring.VoltDBGenericMetricsAgent;
import org.voltdb.monitoring.plugins.AbstractVoltDBPlugin;

/**
 *
 * Class to send metrics to files at periodic intervals.
 *
 * @author drolfe
 *
 */
public class VoltDBExportPlugin extends AbstractVoltDBPlugin {
   /**
    * File nonce
    */
   private final String m_nonce;

   /**
    * stream name
    */
   private final String m_streamName;

   /**
    * export target name
    */
   private final String m_targetName;

   /**
    * We have our own VoltDB client
    */
   Client m_client = null;

   /**
    * Callback to trap errors
    */
   private static final ComplainOnErrorCallback coec = new ComplainOnErrorCallback();

   public VoltDBExportPlugin(String name, String servers, int port, String user, String password, String nonce,
         String streamName, String targetName, boolean forceCreate, boolean printDDL) {
      super(name, servers, port, user, password);
      this.m_nonce = nonce;
      this.m_streamName = streamName;
      this.m_targetName = targetName;
      if (printDDL) {
         String ddl = makeMetricsStreamDDL(m_streamName, m_targetName);
         System.out.println(ddl);
         System.exit(0);
      } else if (forceCreate) {
         try {
            checkedClient();
            String ddl = makeMetricsStreamDDL(m_streamName, m_targetName);
            System.out.println(ddl);
            ClientResponse cr = m_client.callProcedure("@AdHoc", ddl);
            if (cr.getStatus() != ClientResponse.SUCCESS) {
               System.err.println(cr.getStatusString());
            }
         } catch (IOException e) {
            e.printStackTrace();
         } catch (ProcCallException e) {
            if (e.getMessage().indexOf("object name already exists") > -1) {
               System.out.println("Stream " + m_streamName + " already exists");
            } else {
               e.printStackTrace();
            }
         }
      }
   }

   private String makeMetricsStreamDDL(String streamName, String targetName) {
      final String sep = System.lineSeparator();
      return new StringBuilder("CREATE STREAM ")
         .append(streamName)
         .append(sep)
         .append(" PARTITION ON COLUMN cluster_name EXPORT TO TARGET ")
         .append(targetName)
         .append(sep)
         .append("(metric_date timestamp not null")
         .append(sep)
         .append(",cluster_name varchar(255) not null")
         .append(sep)
         .append(",nonce  varchar(255)")
         .append(sep)
         .append(",metric_name varchar(255) not null")
         .append(sep)
         .append(",metric_units varchar(30) not null")
         .append(sep)
         .append(",metric_value decimal not null);")
         .append(sep)
         .toString();
   }

   @Override
   public void reportMetric(String metricName, String units, Number value) {
      try {
         checkedClient();
         m_client.callProcedure(coec, m_streamName + ".insert",
               new Date(System.currentTimeMillis()), getName(), m_nonce, metricName, units,
               new Double(value.doubleValue()));
      } catch (Exception e) {
         try {
            m_client.close();
         } catch (InterruptedException e1) {
            e1.printStackTrace();
         }
         m_client = null;
         VoltDBGenericMetricsAgent.logError(e);
      }
   }

   /**
    * Get Client and connect to the servers if we fail we will try again in
    * next polling cycle.
    *
    * @throws UnknownHostException
    * @throws IOException
    */
   private void checkedClient() throws UnknownHostException, IOException {
      if (m_client == null) {
         m_client = ThirdPartyMetricEngineInterface.createClient(this);
      }
   }

   /*
    * (non-Javadoc)
    *
    * @see java.lang.Object#finalize()
    */
   @Override
   protected void finalize() throws Throwable {
      if (m_client != null) {
         m_client.close();
      }
      super.finalize();
   }

   public static void main(String[] args) {
      // Initialize parameter defaults
      String name = "VoltDB";
      String servers = "localhost";
      int port = 21212;
      String user = "";
      String password = "";
      int delaySeconds = 60;
      String nonce = "";
      String streamName = "VOLTDB_METRICS";
      String targetName = "VOLTDB_METRICS_TGT";
      boolean forceCreate = false;
      boolean printDDL = false;

      // Parse out parameters
      for (int i = 0; i < args.length; i++) {
         final String arg = args[i];
         if (arg.startsWith("--servers=")) {
            servers = extractArg(arg, "servers");
         } else if (arg.startsWith("--delay=")) {
            delaySeconds = Integer.valueOf(extractArg(arg, "delay"));
         } else if (arg.startsWith("--port=")) {
            port = Integer.valueOf(extractArg(arg, "port"));
         } else if (arg.startsWith("--user=")) {
            user = extractArg(arg, "user");
         } else if (arg.startsWith("--forcecreate=")) {
            forceCreate = extractArg(arg, "forcecreate").equalsIgnoreCase("TRUE");
         } else if (arg.startsWith("--printddl=")) {
            printDDL = extractArg(arg, "printddl").equalsIgnoreCase("TRUE");
         } else if (arg.startsWith("--password=")) {
            password = extractArg(arg, "password");
         } else if (arg.startsWith("--name=")) {
            name = extractArg(arg, "name");
         } else if (arg.startsWith("--streamname=")) {
            streamName = extractArg(arg, "streamname");
         } else if (arg.startsWith("--targetname=")) {
            targetName = extractArg(arg, "targetname");
         } else if (arg.startsWith("--nonce=")) {
            nonce = extractArg(arg, "nonce");
         } else {
            helpOrQuite(arg);
         }
      }
      new VoltDBExportPlugin(name, servers, port, user, password,
            nonce, streamName, targetName, forceCreate, printDDL)
         .run(delaySeconds);
   }

@Override
public void reportMetric(String metricName, String[] labelNames, String[] labelValues, Number value, String help) {
    // TODO Auto-generated method stub
    
}
}

