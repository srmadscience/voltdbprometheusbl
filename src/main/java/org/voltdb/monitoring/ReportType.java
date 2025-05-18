/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 */

package org.voltdb.monitoring;

import java.util.ArrayList;
import java.util.List;

public enum ReportType {
   CPU("CPU"),
   Memory("MEMORY"),
   IoStats("IOSTATS"),
   Table("TABLE"),
   LiveClients("LIVECLIENTS"),
   Latency("LATENCY_HISTOGRAM"),
   ProcedureProfile("PROCEDUREPROFILE"),
   ProcedureDetail("PROCEDUREDETAIL"),
   CommandLog("COMMANDLOG"),
   Snapshot("SNAPSHOTSTATUS"),
   Index("INDEX");
   private static final List<ReportType> ALL = new ArrayList<ReportType>() {{
      add(CPU);
      add(Memory);
      add(IoStats);
      add(Table);
      add(LiveClients);
      add(Latency);
      add(ProcedureProfile);
      add(ProcedureDetail);
      add(CommandLog);
      add(Snapshot);
      add(Index);
   }};

   private final String m_name;
   ReportType(String name) {
      m_name = name;
   }
   public String getName() {
      return m_name;
   }
   public static List<ReportType> getAllTypes() {
      return ALL;
   }
}

