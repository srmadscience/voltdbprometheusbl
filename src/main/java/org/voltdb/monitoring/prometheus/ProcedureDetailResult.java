package org.voltdb.monitoring.prometheus;

public class ProcedureDetailResult {
    
    
    private static final String[] LABELS = { "procname","sql","partitionId"};
    
    String procedureName;
    String statementName;
    long partitionId;
    long invocations = 0;
    long avgExecTimeNs = 0;
    long maxExecTimeNs = 0;
    int kFactor;


    public ProcedureDetailResult(String procedureName, String statementName, long partitionId,  long invocations
            , long avgExecTimeNs,long maxExecTimeNs,
            int kFactor) {
        
        super();
        this.procedureName = procedureName;
        this.statementName = statementName;
        this.invocations = invocations;
        this.avgExecTimeNs = avgExecTimeNs;
        this.maxExecTimeNs = maxExecTimeNs;
        this.kFactor = kFactor;
        this.partitionId = partitionId;
    }
    
    public String getName() {
        return procedureName + "\t" + statementName + "\t" + partitionId;
    }

    /**
     * @return the procedureName
     */
    public String getProcedureName() {
        return procedureName;
    }

    /**
     * @return the statementName
     */
    public String getStatementName() {
        return statementName;
    }

    /**
     * @return the invocations
     */
    public long getInvocations() {
        return invocations;
    }

    /**
     * @return the avgExecTimeNs
     */
    public long getAvgExecTimeNs() {
        return avgExecTimeNs;
    }

    /**
     * @return the maxExecTimeNs
     */
    public long getMaxExecTimeNs() {
        return maxExecTimeNs;
    }

    /**
     * @return the kFactor
     */
    public int getkFactor() {
        return kFactor;
    }

    public String[]  returnLabels() {
        return LABELS;
    }
    
    public String[]  returnLabelValues() {
        
        String[] values = {procedureName, statementName.replace("<ALL>","ALL"), partitionId+""};
        return values;
    }
}
