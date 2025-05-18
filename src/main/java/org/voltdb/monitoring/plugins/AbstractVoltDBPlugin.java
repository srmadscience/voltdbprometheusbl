/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 */
package org.voltdb.monitoring.plugins;

import org.voltdb.monitoring.ThirdPartyMetricEngineInterface;
import org.voltdb.monitoring.VoltDBGenericMetricsAgent;

public abstract class AbstractVoltDBPlugin implements ThirdPartyMetricEngineInterface {
   private static final long serialVersionUID = 0l;
   /**
    * Used to do the actual metric generation.
    */
   private final VoltDBGenericMetricsAgent m_metricsAgent;

   /**
    * Cluster name
    */
   private final String m_clusterName;

   /**
    * comma delimited list of hosts
    */
   private final String m_servers;

   /**
    * VoltDB port
    */
   private final int m_port;

   /**
    * VoltDB user
    */
   private final String m_user;

   /**
    * VoltDB password
    */
   private final String m_password;
   public AbstractVoltDBPlugin(String name, String servers, int port, String user, String password) {
      m_clusterName = name;
      m_servers = servers;
      m_port = port;
      m_user = user;
      m_password = password;
      m_metricsAgent = new VoltDBGenericMetricsAgent(this, VoltDBGenericMetricsAgent.FLAVOR_NEWRELIC);
   }

   @Override
   public String getName() {
      return m_clusterName;
   }

   @Override
   public String getServers() {
      return m_servers;
   }

   @Override
   public String getUser() {
      return m_user;
   }

   @Override
   public String getPassword() {
      return m_password;
   }

   @Override
   public int getPort() {
      return m_port;
   }

   @Override
   protected void finalize() throws Throwable {
      m_metricsAgent.disconnect();
   }

   public void run(int delaySeconds) {
      try {
         while (true) {
            gatherMetrics();
            Thread.sleep(1000 * delaySeconds);
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public void gatherMetrics() {
      m_metricsAgent.gatherMetrics();
   }

   protected static String extractArgInput(String arg) {
      // the input arguments has "=" character when this function is called
      final String[] splitStrings = arg.split("=", 2);
      if (splitStrings[1].isEmpty()) {
         System.err.println("Missing input value for " + splitStrings[0]);
         return null;
      }
      return splitStrings[1];
   }

   protected static void printUsage() {
      System.out.println("Parameters:");
      System.out.println("--name=clustername");
      System.out.println("--servers=server1,server2,server3");
      System.out.println("--delay=delay in seconds between passes");
      System.out.println("--port=voltdb server port");
      System.out.println("--user=user");
      System.out.println("--password=password");
      System.out.println("--nonce=file nonce");
      System.out.println("--fileDir=directory for files. Defaults to working directory");
      System.out.println("--dateformatFile=simple date format for file name");
      System.out.println("--dateformatRecord=simple date format for timestamps in file");
   }

   protected static String extractArg(String arg, String key) {
      final String value = extractArgInput(arg);
      if (value == null) {
         System.err.println("--" + key + " must have a value");
         System.exit(1);
      }
      return value;
   }

   protected static void helpOrQuite(String arg) {
      if (arg.startsWith("--usage") || arg.startsWith("--help")) {
         printUsage();
         System.exit(0);
      } else {
         System.err.println("Invalid Parameter: " + arg);
         System.exit(1);
      }
   }
}

