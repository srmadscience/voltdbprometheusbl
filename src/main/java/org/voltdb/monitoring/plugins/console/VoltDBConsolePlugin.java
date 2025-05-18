/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 */

package org.voltdb.monitoring.plugins.console;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.voltdb.monitoring.ThirdPartyMetricEngineInterface;
import org.voltdb.monitoring.plugins.AbstractVoltDBPlugin;

/**
 *
 * Class to send metrics to Console at periodic intervals.
 *
 * @author drolfe
 *
 */
public class VoltDBConsolePlugin extends AbstractVoltDBPlugin {

   /**
    * Date Format for records in the file - as per SimpleDateFormat
    */
   private final String m_dateformatRecord;

   /**
    * naming format for timestamp of records in file
    */
   private final DateFormat m_sdfRecord;

   /**
    * Character to seperate entries in a row
    */
   private static final String SEPCHAR = ":";

   public VoltDBConsolePlugin(String name, String servers, int port, String user, String password,
         String dateformatRecord) {
      super(name, servers, port, user, password);
      m_dateformatRecord = dateformatRecord;
      m_sdfRecord = new SimpleDateFormat(m_dateformatRecord);
   }

   @Override
   public void reportMetric(String metricName, String units, Number value) {
      System.out.println(String.format("%s:%s:%s:%f",
               m_sdfRecord.format(new Date(System.currentTimeMillis())),
               metricName, units, value.doubleValue()));
   }

   public static void main(String[] args) {
      // Initialize parameter defaults
      String name = "VoltDB";
      String servers = "localhost";
      int port = 21212;
      String user = "";
      String password = "";
      int delaySeconds = 60;
      String dateformatRecord = "yyyy-MM-dd_Hms";
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
         } else if (arg.startsWith("--password=")) {
            password = extractArg(arg, "password");
         } else if (arg.startsWith("--name=")) {
            name = extractArg(arg, "name");
         } else if (arg.startsWith("--dateformatRecord=")) {
            dateformatRecord = extractArg(arg, "dateformatRecord");
            // See if we have a valid format...
            try {
               @SuppressWarnings("unused")
               Date testDate = new Date(System.currentTimeMillis());
               @SuppressWarnings("unused")
               SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateformatRecord);
            } catch (Exception e) {
               System.err.println("--dateformatRecord is not a valid format.");
               System.exit(1);
            }
         } else {
            helpOrQuite(arg);
         }
      }
      new VoltDBConsolePlugin(name, servers, port, user, password, dateformatRecord)
         .run(delaySeconds);
   }

@Override
public void reportMetric(String metricName, String[] labelNames, String[] labelValues, Number value, String help) {
    // TODO Auto-generated method stub
    
}
}

