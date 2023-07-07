package org.voltdb.monitoring.prometheus;

import java.util.Date;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

public class TopLevelServletData {
    public String name;
    public String user;
    public String password;
    public String serverList;
    public int port;
    public String procedureList;
    public String procedureSuffix;
    
    public String connectedServerList = "";
    public Date lastDdlTime = new Date();
    public final Date startTime = new Date();
    
    public String lasterror = "";

    public TopLevelServletData(String name, String serverList, int port, String user, String password, String procedureList, String procedureSuffix) {
    	
        this.name = name;
        this.serverList = serverList;
        this.port = port;
        this.user = user;
        this.password=password;
        
        this.procedureList = procedureList;
        this.procedureSuffix = procedureSuffix;
        
    }

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("<html><body><h1>VoltDB Prometheus BL Plugin</h1> <h2><p>[name=");
		builder.append(name);
		builder.append(", </p><p>serverList=");
		builder.append(serverList);
		builder.append(", </p><p>user=");
		builder.append(user);
		builder.append(", </p><p>port=");
		builder.append(port);
		builder.append(", </p><p>procedureList=");
		builder.append(procedureList);
		builder.append(", </p><p>procedureSuffix=");
		builder.append(procedureSuffix);
		builder.append(", </p><p>connectedServerList=");
		builder.append(connectedServerList);
		builder.append(", </p><p>lastDdlTime=");
		builder.append(lastDdlTime);
		builder.append(", </p><p>startTime=");
		builder.append(startTime);
		builder.append(", </p><p>lasterror=");
		builder.append(lasterror);
		builder.append("]</p></h2></body></html>");
		return builder.toString();
	}

	/**
	 * @param connectedServerList the connectedServerList to set
	 */
	public void setConnectedServerList(String connectedServerList) {
		this.connectedServerList = connectedServerList;
	}

	/**
	 * @param lastDdlTime the lastDdlTime to set
	 */
	public void setLastDdlTime(Date lastDdlTime) {
		this.lastDdlTime = lastDdlTime;
	}

	public String getConnectedServerList() {
		return connectedServerList;
	}
}