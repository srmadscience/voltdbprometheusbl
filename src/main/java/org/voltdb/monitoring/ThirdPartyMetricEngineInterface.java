/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 */

package org.voltdb.monitoring;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

/**
 * Interface to be implemented by monitoring solutions.
 * @author drolfe
 *
 */
public interface ThirdPartyMetricEngineInterface {

    /**
     * Implementors of this interface use this method to report metrics as we
     * create them in batches every few seconds. If for whatever reason the
     * format of 'metricName' is not useful for the montitoring platform you
     * can use the 'flavor' parameter of VoltDBGenericMetricsAgent to change
     * the structure of it, but only as a last resort.
     *
     * @param metricName
     * @param units
     * @param value
     */
    void reportMetric(String metricName, String units, Number value);
    void reportMetric(String metricName, String[] labelNames, String[] labelValues, Number value, String help) ;

    String getName();
    String getServers();
    String getUser();
    String getPassword();
    int getPort();

    public static boolean in(String value, String... keys) {
       return Arrays.stream(keys).anyMatch(key -> key.equalsIgnoreCase(value));
    }

    static Client createClient(ThirdPartyMetricEngineInterface engine)
          throws UnknownHostException, IOException {
       final ClientConfig config = new ClientConfig(engine.getUser(), engine.getPassword());
       config.setProcedureCallTimeout(0); // Set procedure all to infinite
       final Client client = ClientFactory.createClient(config);
       for (String s : engine.getServers().split(",")) {
          client.createConnection(s.trim(), engine.getPort());
          VoltDBGenericMetricsAgent.logInfo("Connected to server: " + s);
       }
       return client;
    }
}
