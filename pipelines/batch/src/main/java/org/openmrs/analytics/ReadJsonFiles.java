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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.hl7.fhir.r4.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO refactor FetchSearchPageFn and take out pieces needed here and in ConvertResourceFn
public class ReadJsonFiles extends FetchSearchPageFn<FileIO.ReadableFile> {
	
	private static final Logger log = LoggerFactory.getLogger(ReadJsonFiles.class);
	
	private final Set<String> resourceTypes;
	
	ReadJsonFiles(FhirEtlOptions options) {
		super(options, "ReadJsonFiles");
		resourceTypes = Sets.newHashSet(options.getResourceList().split(","));
	}
	
	// This should be the signature of processElement once we refactor FetchSearchPageFn
	// public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<Bundle> out)
	@ProcessElement
	public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<KV<String, Integer>> out)
	        throws IOException, SQLException {
		log.info("Reading file with metadata " + file.getMetadata());
		String fileContent = file.readFullyAsUTF8String();
		Bundle bundle = (Bundle) parser.parseResource(fileContent);
		processBundle(bundle, resourceTypes);
	}
}
