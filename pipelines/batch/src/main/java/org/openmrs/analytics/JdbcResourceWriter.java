// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.openmrs.analytics;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcResourceWriter {
	
	private static final Logger log = LoggerFactory.getLogger(JdbcResourceWriter.class);
	
	private final String tablePrefix;
	
	private final boolean useSingleTable;
	
	private IParser parser;
	
	private JdbcConnectionUtil jdbcConnectionUtil;
	
	JdbcResourceWriter(JdbcConnectionUtil jdbcConnectionUtil, String tablePrefix, boolean useSingleTable,
	    FhirContext fhirContext) {
		this.tablePrefix = tablePrefix;
		this.useSingleTable = useSingleTable;
		this.parser = fhirContext.newJsonParser();
		this.jdbcConnectionUtil = jdbcConnectionUtil;
	}
	
	private static void createSingleTable(Connection connection, String tableName) throws SQLException {
		// TODO change the key to be id in the case of separate tables!
		// Note for CREATE statements we cannot (and don't need to) use a placeholder for table name.
		String createTemplate = "CREATE TABLE IF NOT EXISTS %s (id VARCHAR(100) NOT NULL, "
		        + "type VARCHAR(50) NOT NULL, datab JSONB, PRIMARY KEY (id, type) );";
		try (PreparedStatement statement = connection.prepareStatement(String.format(createTemplate, tableName))) {
			log.info("Table creations statement is " + statement);
			statement.execute();
		}
	}
	
	static void createTables(FhirEtlOptions options) throws PropertyVetoException, SQLException {
		// This should not be triggered in pipeline workers because concurrent CREATEs lead to failures:
		// https://stackoverflow.com/questions/54351783/duplicate-key-value-violates-unique-constraint
		//
		log.info(
		    String.format("Connecting to DB url %s with user %s.", options.getSinkDbUrl(), options.getSinkDbUsername()));
		JdbcConnectionUtil connectionUtil = new JdbcConnectionUtil(options.getJdbcDriverClass(), options.getSinkDbUrl(),
		        options.getSinkDbPassword(), options.getSinkDbPassword(), options.getJdbcInitialPoolSize(),
		        options.getJdbcMaxPoolSize());
		Connection connection = connectionUtil.getConnectionObject().getConnection();
		if (options.getUseSingleSinkTable()) {
			createSingleTable(connection, options.getSinkDbTablePrefix());
		} else {
			for (String resourceType : options.getResourceList().split(",")) {
				createSingleTable(connection, options.getSinkDbTablePrefix() + resourceType);
			}
		}
		connection.close();
	}
	
	public void writeResource(Resource resource) throws SQLException {
		// TODO add the option for SQL-on-FHIR schema
		String tableName = tablePrefix + resource.getResourceType().name();
		if (useSingleTable) {
			tableName = tablePrefix;
		}
		PreparedStatement statement = jdbcConnectionUtil.getConnectionObject().getConnection()
		        .prepareStatement("INSERT INTO " + tableName + " (id, type, datab) VALUES(?, ?, ?::jsonb) "
		                + "ON CONFLICT (id, type) DO UPDATE SET id=?, type=?, datab=?::jsonb ;");
		try {
			statement.setString(1, resource.getIdElement().getIdPart());
			statement.setString(2, resource.getResourceType().name());
			statement.setString(3, parser.encodeResourceToString(resource));
			statement.setString(4, resource.getIdElement().getIdPart());
			statement.setString(5, resource.getResourceType().name());
			statement.setString(6, parser.encodeResourceToString(resource));
			statement.execute();
		}
		finally {
			jdbcConnectionUtil.closeConnection(statement);
		}
	}
	
}
