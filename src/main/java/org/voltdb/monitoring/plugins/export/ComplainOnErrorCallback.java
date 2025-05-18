/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 */
package org.voltdb.monitoring.plugins.export;

import org.voltcore.logging.VoltLogger;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

/**
 * Simple callback that complains if something went badly
 * wrong.
 * @author drolfe
 *
 */
public class ComplainOnErrorCallback implements ProcedureCallback {
    protected static final VoltLogger logger = new VoltLogger("CONSOLE");
    @Override
    public void clientCallback(ClientResponse arg0) {
        if (arg0.getStatus() != ClientResponse.SUCCESS) {
            logger.error("Error Code " + arg0.getStatusString());
        }
    }
}

