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
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: A good amount of functionality/setup here is shared with FetchSearchPageFn. 
// There is room for refactoring in the future.
public class ConvertResourceFn extends DoFn<HapiRowDescriptor, Integer> {
	
	private static final Logger log = LoggerFactory.getLogger(ConvertResourceFn.class);
	
	private final HashMap<String, Counter> numFetchedResources;
	
	private final HashMap<String, Counter> totalParseTimeMillis;
	
	private final HashMap<String, Counter> totalGenerateTimeMillis;
	
	private final HashMap<String, Counter> totalPushTimeMillis;
	
	private final String sinkPath;
	
	private final String sinkUsername;
	
	private final String sinkPassword;
	
	private final String parquetFile;
	
	private final int secondsToFlush;
	
	private final int rowGroupSize;
	
	private final String sinkDbUrl;
	
	private final String sinkDbUsername;
	
	private final String sinkDbPassword;
	
	private final String sinkDbTableName;
	
	private final boolean useSingleSinkDbTable;
	
	private final String jdbcDriverClass;
	
	private final int initialPoolSize;
	
	private final int maxPoolSize;
	
	@VisibleForTesting
	protected ParquetUtil parquetUtil;
	
	protected FhirStoreUtil fhirStoreUtil;
	
	private SimpleDateFormat simpleDateFormat;
	
	private IParser parser;
	
	private JdbcResourceWriter jdbcWriter;
	
	ConvertResourceFn(FhirEtlOptions options) {
		this.sinkPath = options.getFhirSinkPath();
		this.sinkUsername = options.getSinkUserName();
		this.sinkPassword = options.getSinkPassword();
		this.parquetFile = options.getOutputParquetPath();
		this.secondsToFlush = options.getSecondsToFlushParquetFiles();
		this.rowGroupSize = options.getRowGroupSizeForParquetFiles();
		this.sinkDbUrl = options.getSinkDbUrl();
		this.sinkDbTableName = options.getSinkDbTablePrefix();
		this.sinkDbUsername = options.getSinkDbUsername();
		this.sinkDbPassword = options.getSinkDbPassword();
		this.useSingleSinkDbTable = options.getUseSingleSinkTable();
		this.initialPoolSize = options.getJdbcInitialPoolSize();
		this.maxPoolSize = options.getJdbcMaxPoolSize();
		// We are assuming that the potential source and sink DBs are the same type.
		this.jdbcDriverClass = options.getJdbcDriverClass();
		
		this.numFetchedResources = new HashMap<String, Counter>();
		this.totalParseTimeMillis = new HashMap<String, Counter>();
		this.totalGenerateTimeMillis = new HashMap<String, Counter>();
		this.totalPushTimeMillis = new HashMap<String, Counter>();
		List<String> resourceTypes = Arrays.asList(options.getResourceList().split(","));
		for (String resourceType : resourceTypes) {
			this.numFetchedResources.put(resourceType,
			    Metrics.counter(EtlUtils.METRICS_NAMESPACE, "numFetchedResources_" + resourceType));
			this.totalParseTimeMillis.put(resourceType,
			    Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalParseTimeMillis_" + resourceType));
			this.totalGenerateTimeMillis.put(resourceType,
			    Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalGenerateTimeMillis_" + resourceType));
			this.totalPushTimeMillis.put(resourceType,
			    Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalPushTimeMillis_" + resourceType));
		}
	}
	
	@Setup
	public void setup() throws PropertyVetoException, SQLException {
		FhirContext fhirContext = FhirContext.forR4();
		//This increases the socket timeout value for the RESTful client; the default is 10000 ms.
		fhirContext.getRestfulClientFactory().setSocketTimeout(20000);
		fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, sinkUsername, sinkPassword,
		    fhirContext.getRestfulClientFactory());
		parquetUtil = new ParquetUtil(parquetFile, secondsToFlush, rowGroupSize, "");
		parser = fhirContext.newJsonParser();
		simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		if (!sinkDbUrl.isEmpty()) {
			// TODO consider using JdbcIo instead of creating separate connection pools for each worker.
			log.info("Creating a JdbcConnectionUtil in ConvertResourceFn setup for " + sinkDbUrl);
			jdbcWriter = new JdbcResourceWriter(new JdbcConnectionUtil(jdbcDriverClass, sinkDbUrl, sinkDbUsername,
			        sinkDbPassword, initialPoolSize, maxPoolSize), sinkDbTableName, useSingleSinkDbTable, fhirContext);
		}
	}
	
	@Teardown
	public void teardown() throws IOException {
		parquetUtil.closeAllWriters();
	}
	
	public void writeResource(HapiRowDescriptor element) throws IOException, ParseException, SQLException {
		String resourceId = element.resourceId();
		String resourceType = element.resourceType();
		Meta meta = new Meta().setVersionId(element.resourceVersion())
		        .setLastUpdated(simpleDateFormat.parse(element.lastUpdated()));
		String jsonResource = element.jsonResource();
		
		long startTime = System.currentTimeMillis();
		Resource resource = (Resource) parser.parseResource(jsonResource);
		totalParseTimeMillis.get(resourceType).inc(System.currentTimeMillis() - startTime);
		resource.setId(resourceId);
		resource.setMeta(meta);
		
		numFetchedResources.get(resourceType).inc(1);
		
		if (!parquetFile.isEmpty()) {
			startTime = System.currentTimeMillis();
			parquetUtil.write(resource);
			totalGenerateTimeMillis.get(resourceType).inc(System.currentTimeMillis() - startTime);
		}
		if (!this.sinkPath.isEmpty()) {
			startTime = System.currentTimeMillis();
			fhirStoreUtil.uploadResource(resource);
			totalPushTimeMillis.get(resourceType).inc(System.currentTimeMillis() - startTime);
		}
		if (!this.sinkDbUrl.isEmpty()) {
			jdbcWriter.writeResource(resource);
		}
	}
	
	@ProcessElement
	public void processElement(ProcessContext processContext) throws IOException, ParseException, SQLException {
		HapiRowDescriptor element = processContext.element();
		writeResource(element);
	}
}
