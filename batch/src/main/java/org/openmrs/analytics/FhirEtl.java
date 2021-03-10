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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.SummaryEnum;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Beam pipeline for reading FHIR resources from OpenMRS and pushing them into a data warehouse.
 */
public class FhirEtl {
	
	private static final Logger log = LoggerFactory.getLogger(FhirEtl.class);
	
	private static final String METRICS_NAMESPACE = "FhirEtl";
	
	/**
	 * Options supported by {@link FhirEtl}.
	 */
	public interface FhirEtlOptions extends PipelineOptions {
		
		/**
		 * By default, this reads from the OpenMRS instance `openmrs` at the default port on localhost.
		 */
		@Description("OpenMRS server URL")
		@Required
		@Default.String("http://localhost:8099/openmrs")
		String getOpenmrsServerUrl();
		
		void setOpenmrsServerUrl(String value);
		
		@Description("OpenMRS server fhir endpoint")
		@Default.String("/ws/fhir2/R4")
		String getServerFhirEndpoint();
		
		void setServerFhirEndpoint(String value);
		
		@Description("Comma separated list of resource and search parameters to fetch; in its simplest "
		        + "form this is a list of resources, e.g., `Patient,Encounter,Observation` but more "
		        + "complex search paths are possible too, e.g., `Patient?name=Susan.`"
		        + "Please note that complex search params doesn't work when JDBC mode is enabled.")
		@Default.String("Patient,Encounter,Observation")
		String getSearchList();
		
		void setSearchList(String value);
		
		@Description("The number of resources to be fetched in one API call. "
		        + "For the JDBC mode passing > 170 could result in HTTP 400 Bad Request")
		@Default.Integer(100)
		int getBatchSize();
		
		void setBatchSize(int value);
		
		@Description("For the JDBC mode, this is the size of each ID chunk. Setting high values will yield faster query "
		        + "execution.")
		@Default.Integer(10000)
		int getJdbcFetchSize();
		
		void setJdbcFetchSize(int value);
		
		@Description("Openmrs BasicAuth Username")
		@Default.String("admin")
		String getOpenmrsUserName();
		
		void setOpenmrsUserName(String value);
		
		@Description("Openmrs BasicAuth Password")
		@Default.String("Admin123")
		String getOpenmrsPassword();
		
		void setOpenmrsPassword(String value);
		
		@Description("The path to the target generic fhir store, or a GCP fhir store with the format: "
		        + "`projects/[\\w-]+/locations/[\\w-]+/datasets/[\\w-]+/fhirStores/[\\w-]+`, e.g., "
		        + "`projects/my-project/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test`")
		@Required
		@Default.String("")
		String getFhirSinkPath();
		
		void setFhirSinkPath(String value);
		
		@Description("Sink BasicAuth Username")
		@Default.String("")
		String getSinkUserName();
		
		void setSinkUserName(String value);
		
		@Description("Sink BasicAuth Password")
		@Default.String("")
		String getSinkPassword();
		
		void setSinkPassword(String value);
		
		@Description("The base name for output Parquet file; for each resource, one fileset will be created.")
		@Default.String("")
		String getOutputFilePath();
		
		void setOutputFilePath(String value);
		
		/**
		 * JDBC DB settings: defaults values have been pointed to ./openmrs-compose.yaml
		 */
		
		@Description("JDBC URL input")
		@Default.String("jdbc:mysql://localhost:3306/openmrs")
		String getJdbcUrl();
		
		void setJdbcUrl(String value);
		
		@Description("JDBC MySQL driver class")
		@Default.String("com.mysql.cj.jdbc.Driver")
		String getJdbcDriverClass();
		
		void setJdbcDriverClass(String value);
		
		@Description("JDBC maximum pool size")
		@Default.Integer(50)
		int getJdbcMaxPoolSize();
		
		void setJdbcMaxPoolSize(int value);
		
		@Description("JDBC initial pool size")
		@Default.Integer(10)
		int getJdbcInitialPoolSize();
		
		void setJdbcInitialPoolSize(int value);
		
		@Description("MySQL DB user")
		@Default.String("root")
		String getDbUser();
		
		void setDbUser(String value);
		
		@Description("MySQL DB user password")
		@Default.String("debezium")
		String getDbPassword();
		
		void setDbPassword(String value);
		
		@Description("Path to Table-FHIR map config")
		@Default.String("utils/dbz_event_to_fhir_config.json")
		String getTableFhirMapPath();
		
		void setTableFhirMapPath(String value);
		
		@Description("Flag to switch between the 2 modes of batch extract")
		@Default.Boolean(false)
		Boolean isJdbcModeEnabled();
		
		void setJdbcModeEnabled(Boolean value);
		
		@Description("Number of output file shards; 0 leaves it to the runner to decide but is not recommended.")
		@Default.Integer(3)
		int getNumParquetShards();
		
		void setNumParquetShards(int value);
		
		@Description("Whether to output in JSON format; this is currently intended for test only.")
		@Default.Boolean(false)
		boolean getTextOutput();
		
		void setTextOutput(boolean value);
	}
	
	static FhirSearchUtil createFhirSearchUtil(FhirEtlOptions options, FhirContext fhirContext) {
		return new FhirSearchUtil(createOpenmrsUtil(options.getOpenmrsServerUrl() + options.getServerFhirEndpoint(),
		    options.getOpenmrsUserName(), options.getOpenmrsPassword(), fhirContext));
	}
	
	static OpenmrsUtil createOpenmrsUtil(String sourceUrl, String sourceUser, String sourcePw, FhirContext fhirContext) {
		return new OpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
	}
	
	static Map<String, List<SearchSegmentDescriptor>> createSegments(FhirEtlOptions options, FhirContext fhirContext)
	        throws CannotProvideCoderException {
		if (options.getBatchSize() > 100) {
			// TODO: Probe this value from the server and set the maximum automatically.
			log.warn("NOTE batchSize flag is higher than 100; make sure that `fhir2.paging.maximum` "
			        + "is set accordingly on the OpenMRS server.");
		}
		FhirSearchUtil fhirSearchUtil = createFhirSearchUtil(options, fhirContext);
		Map<String, List<SearchSegmentDescriptor>> segmentMap = new HashMap<>();
		for (String search : options.getSearchList().split(",")) {
			List<SearchSegmentDescriptor> segments = new ArrayList<>();
			segmentMap.put(search, segments);
			String searchUrl = options.getOpenmrsServerUrl() + options.getServerFhirEndpoint() + "/" + search;
			log.info("searchUrl is " + searchUrl);
			Bundle searchBundle = fhirSearchUtil.searchByUrl(searchUrl, options.getBatchSize(), SummaryEnum.DATA);
			if (searchBundle == null) {
				log.error("Cannot fetch resources for " + searchUrl);
				throw new IllegalStateException("Cannot fetch resources for " + searchUrl);
			}
			int total = searchBundle.getTotal();
			log.info(String.format("Number of resources for %s search is %d", search, total));
			if (searchBundle.getEntry().size() >= total) {
				// No parallelism is needed in this case; we get all resources in one bundle.
				segments.add(SearchSegmentDescriptor.create(searchUrl, options.getBatchSize()));
			} else {
				for (int offset = 0; offset < total; offset += options.getBatchSize()) {
					String pageSearchUrl = fhirSearchUtil.findBaseSearchUrl(searchBundle) + "&_getpagesoffset=" + offset;
					segments.add(SearchSegmentDescriptor.create(pageSearchUrl, options.getBatchSize()));
				}
				log.info(String.format("Total number of segments for search %s is %d", search, segments.size()));
			}
		}
		return segmentMap;
	}
	
	// TODO: Move this class and a few static methods after it to a separate file with unit-tests.
	static class FetchSearchPageFn extends DoFn<SearchSegmentDescriptor, Bundle> {
		
		private final Counter numFetchedResources;
		
		private final Counter totalFetchTimeMillis;
		
		private final String sourceUrl;
		
		private final String sourceUser;
		
		private final String sourcePw;
		
		private final String sinkPath;
		
		private final String sinkUsername;
		
		private final String sinkPassword;
		
		private final String resourceType;
		
		private FhirContext fhirContext;
		
		private FhirSearchUtil fhirSearchUtil;
		
		private FhirStoreUtil fhirStoreUtil;
		
		private OpenmrsUtil openmrsUtil;
		
		FetchSearchPageFn(FhirEtlOptions options, String resourceType) {
			this.sinkPath = options.getFhirSinkPath();
			this.sinkUsername = options.getSinkUserName();
			this.sinkPassword = options.getSinkPassword();
			this.sourceUrl = options.getOpenmrsServerUrl() + options.getServerFhirEndpoint();
			this.sourceUser = options.getOpenmrsUserName();
			this.sourcePw = options.getOpenmrsPassword();
			this.resourceType = resourceType;
			this.numFetchedResources = Metrics.counter(METRICS_NAMESPACE, "numFetchedResources_" + resourceType);
			this.totalFetchTimeMillis = Metrics.counter(METRICS_NAMESPACE, "totalFetchTimeMillis_" + resourceType);
		}
		
		@Setup
		public void Setup() {
			fhirContext = FhirContext.forR4();
			fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, sinkUsername, sinkPassword,
			    fhirContext.getRestfulClientFactory());
			openmrsUtil = createOpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
			fhirSearchUtil = new FhirSearchUtil(openmrsUtil);
		}
		
		@ProcessElement
		public void ProcessElement(@Element SearchSegmentDescriptor segment, OutputReceiver<Bundle> out) {
			String searchUrl = segment.searchUrl();
			log.info(String.format("Fetching %d %s resources: %s", segment.count(), this.resourceType,
			    searchUrl.substring(0, Math.min(200, searchUrl.length()))));
			long fetchStartTime = System.currentTimeMillis();
			Bundle pageBundle = fhirSearchUtil.searchByUrl(searchUrl, segment.count(), SummaryEnum.DATA);
			totalFetchTimeMillis.inc(System.currentTimeMillis() - fetchStartTime);
			if (pageBundle != null && pageBundle.getEntry() != null) {
				numFetchedResources.inc(pageBundle.getEntry().size());
				out.output(pageBundle);
			}
			if (!this.sinkPath.isEmpty()) {
				fhirStoreUtil.uploadBundle(pageBundle);
			}
		}
	}
	
	static class BundleToAvro extends DoFn<Bundle, GenericRecord> {
		
		private final Counter totalGenerateTimeMillis;
		
		private final String parquetFile;
		
		private ParquetUtil parquetUtil;
		
		BundleToAvro(String parquetFile, String resourceType) {
			this.parquetFile = parquetFile;
			this.totalGenerateTimeMillis = Metrics.counter(METRICS_NAMESPACE, "totalGenerateTimeMillis_" + resourceType);
		}
		
		@Setup
		public void Setup() {
			parquetUtil = new ParquetUtil(this.parquetFile);
		}
		
		@ProcessElement
		public void ProcessElement(@Element Bundle bundle, OutputReceiver<GenericRecord> out) {
			long startTime = System.currentTimeMillis();
			List<GenericRecord> recordList = parquetUtil.generateRecords(bundle);
			for (GenericRecord record : recordList) {
				out.output(record);
			}
			totalGenerateTimeMillis.inc(System.currentTimeMillis() - startTime);
		}
	}
	
	static class BundleToJson extends DoFn<Bundle, String> {
		
		@ProcessElement
		public void ProcessElement(@Element Bundle bundle, OutputReceiver<String> out) {
			for (BundleEntryComponent entry : bundle.getEntry()) {
				out.output(entry.getResource().toString());
			}
		}
	}
	
	static String findSearchedResource(String search) {
		int argsStart = search.indexOf('?');
		if (argsStart >= 0) {
			return search.substring(0, argsStart);
		}
		return search;
	}
	
	private static void logMetrics(MetricResults metricResults) {
		MetricQueryResults metrics = metricResults.queryMetrics(
		    MetricsFilter.builder().addNameFilter(MetricNameFilter.inNamespace(METRICS_NAMESPACE)).build());
		for (MetricResult<Long> counter : metrics.getCounters()) {
			log.info(String.format("Pipeline counter %s : %s", counter.getName(), counter.getCommitted()));
		}
	}
	
	private static void fetchSegments(PCollection<SearchSegmentDescriptor> inputSegments, String search,
	        FhirEtlOptions options) {
		String resourceType = findSearchedResource(search);
		ParquetUtil parquetUtil = new ParquetUtil(options.getOutputFilePath());
		Schema schema = parquetUtil.getResourceSchema(resourceType);
		PCollection<Bundle> bundles = inputSegments.apply(ParDo.of(new FetchSearchPageFn(options, resourceType)));
		if (!options.getOutputFilePath().isEmpty()) { // TODO separate text output.
			if (options.getTextOutput()) {
				PCollection<String> jsonRecords = bundles.apply(ParDo.of(new BundleToJson()));
				jsonRecords.apply("WriteToText", TextIO.write().to(options.getOutputFilePath()).withSuffix(".txt"));
			} else {
				PCollection<GenericRecord> records = bundles
				        .apply(ParDo.of(new BundleToAvro(options.getOutputFilePath(), resourceType)))
				        .setCoder(AvroCoder.of(GenericRecord.class, schema));
				// TODO: Make sure getOutputFilePath() is a directory.
				String outputFile = options.getOutputFilePath() + resourceType;
				ParquetIO.Sink sink = ParquetIO.sink(schema); // TODO add an option for .withCompressionCodec();
				records.apply(FileIO.<GenericRecord> write().via(sink).to(outputFile).withSuffix(".parquet")
				        .withNumShards(options.getNumParquetShards()));
				// TODO add Avro output option
				//records.apply("WriteToAvro", AvroIO.writeGenericRecords(schema).to(outputFile).withSuffix(".avro")
				//        .withNumShards(options.getNumParquetShards()));
			}
		}
	}
	
	static void runFhirFetch(FhirEtlOptions options, FhirContext fhirContext) throws CannotProvideCoderException {
		Map<String, List<SearchSegmentDescriptor>> segmentMap = createSegments(options, fhirContext);
		if (segmentMap.isEmpty()) {
			return;
		}
		
		Pipeline pipeline = Pipeline.create(options);
		for (Map.Entry<String, List<SearchSegmentDescriptor>> entry : segmentMap.entrySet()) {
			PCollection<SearchSegmentDescriptor> inputSegments = pipeline.apply(Create.of(entry.getValue()));
			fetchSegments(inputSegments, entry.getKey(), options);
		}
		PipelineResult result = pipeline.run();
		result.waitUntilFinish();
		logMetrics(result.metrics());
	}
	
	static void runFhirJdbcFetch(FhirEtlOptions options, FhirContext fhirContext)
	        throws PropertyVetoException, IOException, SQLException {
		Pipeline pipeline = Pipeline.create(options);
		JdbcConnectionUtil jdbcConnectionUtil = new JdbcConnectionUtil(options.getJdbcDriverClass(), options.getJdbcUrl(),
		        options.getDbUser(), options.getDbPassword(), options.getJdbcMaxPoolSize(),
		        options.getJdbcInitialPoolSize());
		JdbcFetchUtil jdbcUtil = new JdbcFetchUtil(jdbcConnectionUtil);
		JdbcIO.DataSourceConfiguration jdbcConfig = jdbcUtil.getJdbcConfig();
		int batchSize = Math.min(options.getBatchSize(), 170); // batch size > 200 will result in HTTP 400 Bad Request
		int jdbcFetchSize = options.getJdbcFetchSize();
		Map<String, String> reverseMap = jdbcUtil.createFhirReverseMap(options.getSearchList(),
		    options.getTableFhirMapPath());
		// process each table-resource mappings
		for (Map.Entry<String, String> entry : reverseMap.entrySet()) {
			String tableName = entry.getKey();
			String resourceType = entry.getValue();
			String baseBundleUrl = options.getOpenmrsServerUrl() + options.getServerFhirEndpoint() + "/" + resourceType;
			int maxId = jdbcUtil.fetchMaxId(tableName);
			Map<Integer, Integer> IdRanges = jdbcUtil.createIdRanges(maxId, jdbcFetchSize);
			PCollection<SearchSegmentDescriptor> inputSegments = pipeline.apply(Create.of(IdRanges))
			        .apply(new JdbcFetchUtil.FetchUuids(tableName, jdbcConfig))
			        .apply(new JdbcFetchUtil.CreateSearchSegments(resourceType, baseBundleUrl, batchSize));
			fetchSegments(inputSegments, resourceType, options);
		}
		PipelineResult result = pipeline.run();
		result.waitUntilFinish();
		logMetrics(result.metrics());
	}
	
	public static void main(String[] args)
	        throws CannotProvideCoderException, PropertyVetoException, IOException, SQLException {
		// Todo: Autowire
		FhirContext fhirContext = FhirContext.forR4();
		
		FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
		if (!options.getOutputFilePath().isEmpty() && options.getNumParquetShards() == 0) {
			log.warn("Setting --numParquetShards=0 can hinder Parquet generation performance significantly!");
		}
		
		ParquetUtil.initializeAvroConverters();
		
		if (options.isJdbcModeEnabled()) {
			runFhirJdbcFetch(options, fhirContext);
		} else {
			runFhirFetch(options, fhirContext);
		}
		
	}
}
