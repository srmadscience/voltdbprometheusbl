/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 */
package org.voltdb.monitoring.plugins.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.voltdb.monitoring.plugins.AbstractVoltDBPlugin;

/**
 *
 * Class to send metrics to files at periodic intervals.
 *
 * @author drolfe
 *
 */
final public class VoltDBFilePlugin extends AbstractVoltDBPlugin {
   /**
    * File nonce
    */
   private final String m_nonce;

   /**
    * Date Format for file names - as per SimpleDateFormat
    */
   private final String m_dateformatFile;

   /**
    * Date Format for records in the file - as per SimpleDateFormat
    */
   private final String m_dateformatRecord;

   /**
    * File directory
    */
   private final String m_fileDir;

   /**
    * File to write to - name is controlled by sdfFile.
    */
   File m_reportFile = null;

   /**
    * BufferedWriter for current File
    */
   BufferedWriter m_bw = null;

   /**
    * FileWriter for current File
    */
   FileWriter m_fw = null;

   /**
    * naming format for file
    */
   private final DateFormat m_sdfFile;

   /**
    * naming format for timestamp of records in file
    */
   private final DateFormat m_sdfRecord;

   /**
    * Character to seperate entries in file row
    */
   private static final String SEPCHAR = ":";

   /**
    * How the file name ends.
    */
   private static final String FILE_EXTENSION = "stat";

   public VoltDBFilePlugin(String name, String servers, int port,
         String user, String password, String nonce, String fileDir,
         String dateformatFile, String dateformatRecord) {
      super(name, servers, port, user, password);
      m_nonce = nonce;
      m_fileDir = fileDir;
      m_dateformatFile = dateformatFile;
      m_dateformatRecord = dateformatRecord;
      m_sdfFile = new SimpleDateFormat(m_dateformatFile);
      m_sdfRecord = new SimpleDateFormat(m_dateformatRecord);
   }

   @Override
   public void reportMetric(String metricName, String units, Number value) {
      try {
         final Date nowDate = new Date(System.currentTimeMillis());
         confirmBw(nowDate);
         if (m_bw != null) {
            m_bw.write(String.format("%s:%s:%s:%f", m_sdfRecord.format(nowDate), metricName, units, value.doubleValue()));
            m_bw.newLine();
         } else {
            System.err.println("File '" + m_reportFile + "' not writable");
         }
      } catch (IOException e1) {
         m_bw = null;
         e1.printStackTrace();
      }
   }

   /**
    * Make sure that the BufferedWriter we are using (a) exists and
    * (b) has the correct file name...
    * @param nowDate
    * @throws IOException
    */
   private void confirmBw(Date nowDate) throws IOException {
      final String correctFileName = getFileName(getName(), m_nonce, nowDate);
      if (m_bw != null && !correctFileName.equals(m_reportFile.getName())) {
         m_bw.close();
         m_fw.close();
         m_bw = null;
         m_fw = null;
      }
      if (m_bw == null) {
         m_reportFile = new File(m_fileDir, correctFileName);
         m_fw = new FileWriter(m_reportFile);
         m_bw = new BufferedWriter(m_fw);
      }
   }

   private String getFileName(String name, String nonce, Date nowDate) {
      return new StringBuffer(name)
         .append(nonce)
         .append(m_sdfFile.format(nowDate))
         .append('.')
         .append(FILE_EXTENSION)
         .toString();
   }

   @Override
   public void gatherMetrics() {
      super.gatherMetrics();
      try { // Somebody may be using 'tail -f' on the file, so keep it up to date..
         m_bw.flush();
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   /*
    * (non-Javadoc)
    *
    * @see java.lang.Object#finalize()
    */
   @Override
   protected void finalize() throws Throwable {
      if (m_bw != null) {
         m_bw.close();
      }
      if (m_fw != null) {
         m_fw.close();
      }
      m_bw = null;
      m_fw = null;
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
      String fileDir = System.getProperty("user.dir");
      String dateformatFile = "yyyy-MM-dd_H";
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
         } else if (arg.startsWith("--nonce=")) {
            nonce = extractArg(arg, "nonce");
         } else if (arg.startsWith("--fileDir=")) {
            fileDir = extractArg(arg, "fileDir");
            File testFile = new File(fileDir);
            if (!testFile.exists()) {
               System.err.println("fileDir '" + fileDir + "' does not exist");
               System.exit(1);
            } else if (!testFile.isDirectory()) {
               System.err.println("fileDir '" + fileDir + "' is not a directory");
               System.exit(1);
            }
         } else if (arg.startsWith("--dateformatFile=")) {
            dateformatFile = extractArg(arg, "dateformatFile");
            // See if we have a valid format...
            try {
               @SuppressWarnings("unused")
               Date testDate = new Date(System.currentTimeMillis());
               @SuppressWarnings("unused")
               SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateformatFile);
            } catch (Exception e) {
               System.err.println("--dateformatFile is not a valid format.");
               System.exit(1);
            }
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
      new VoltDBFilePlugin(name, servers, port, user, password,
            nonce, fileDir, dateformatFile, dateformatRecord)
         .run(delaySeconds);
   }

@Override
public void reportMetric(String metricName, String[] labelNames, String[] labelValues, Number value, String help) {
    // TODO Auto-generated method stub
    
}
}

