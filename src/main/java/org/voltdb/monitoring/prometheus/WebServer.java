package org.voltdb.monitoring.prometheus;

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

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class WebServer {

	public static void main(String[] args) throws Exception {

		// Initialize parameter defaults
		String name = "voltdbbl";
		String serverList = "localhost";
		int port = 21212;
		String user = "";
		String password = "";
		int webserverPort = 9102;
		String procedureList = "ORGANIZEDTABLESTATS,ORGANIZEDINDEXSTATS,PROCEDUREPROFILE,SNAPSHOTSTATS,ORGANIZEDSQLSTMTSTATS";
		String procedureSuffix = "__promBL";


		// Parse out parameters
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.startsWith("--servers=")) {
				serverList = extractArgInput(arg);
				if (serverList == null) {
					System.err.println("--servers must have a value");
					System.exit(1);
				}
			} else if (arg.startsWith("--port=")) {
				String portStr = extractArgInput(arg);
				if (portStr == null) {
					System.err.println("--port must have a value");
					System.exit(1);
				}
				port = Integer.valueOf(portStr);
			} else if (arg.startsWith("--webserverport=")) {
				String portStr = extractArgInput(arg);
				if (portStr == null) {
					System.err.println("--webserverport must have a value");
					System.exit(1);
				}
				webserverPort = Integer.valueOf(portStr);
			} else if (arg.startsWith("--user=")) {
				user = extractArgInput(arg);
				if (user == null) {
					System.err.println("--user must have a value");
					System.exit(1);
				}
			} else if (arg.startsWith("--password=")) {
				password = extractArgInput(arg);
				if (password == null) {
					System.err.println("--password must have a value");
					System.exit(1);
				}
			} else if (arg.startsWith("--name=")) {
				name = extractArgInput(arg);
				if (name == null) {
					System.err.println("--name must have a value");
					System.exit(1);
				}
			} else if (arg.startsWith("--procedureList=")) {
				procedureList = extractArgInput(arg);
				if (name == null) {
					System.err.println("--procedureList must have a value");
					System.exit(1);
				}
			} else if (arg.startsWith("--procedureSuffix=")) {
				procedureSuffix = extractArgInput(arg);
				if (name == null) {
					System.err.println("--procedureSuffix must have a value");
					System.exit(1);
				}
			} else {
				System.err.println("Invalid Parameter: " + arg);
				System.exit(1);
			}
		}

		Server server = new Server(webserverPort);
		ServletContextHandler context = new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);

		TopLevelServlet top = new TopLevelServlet(name, serverList, user, password, port,procedureList,procedureSuffix);
		PrometheusServlet prom = new PrometheusServlet(top.getData());

		context.addServlet(new ServletHolder(top), "/");
		context.addServlet(
				new ServletHolder(prom),
				"/metrics");

		// Start the webserver.
		server.start();
		server.join();

	}

	private static String extractArgInput(String arg) {
		// the input arguments has "=" character when this function is called
		String[] splitStrings = arg.split("=", 2);
		if (splitStrings[1].isEmpty()) {
			System.err.println("Missing input value for " + splitStrings[0]);
			return null;
		}
		return splitStrings[1];
	}
}