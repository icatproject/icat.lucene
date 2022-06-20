package org.icatproject.lucene;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;

public class DocumentMapping {

	/**
	 * Represents the parent child relationship between two ICAT entities.
	 */
	public static class ParentRelationship {
		public String parentName;
		public String fieldPrefix;

		/**
		 * @param parentName Name of the parent entity.
		 * @param fieldPrefix How nested fields should be prefixed.
		 */
		public ParentRelationship(String parentName, String fieldPrefix) {
			this.parentName = parentName;
			this.fieldPrefix = fieldPrefix;
		}

	}

	public static final Set<String> doubleFields = new HashSet<>();
	public static final Set<String> facetFields = new HashSet<>();
	public static final Set<String> longFields = new HashSet<>();
	public static final Set<String> sortFields = new HashSet<>();
	public static final Set<String> textFields = new HashSet<>();
	public static final Set<String> indexedEntities = new HashSet<>();
	public static final Map<String, ParentRelationship[]> relationships = new HashMap<>();

	public static final IcatAnalyzer analyzer = new IcatAnalyzer();
	public static final StandardQueryParser genericParser = new StandardQueryParser();
	public static final StandardQueryParser datafileParser = new StandardQueryParser();
	public static final StandardQueryParser datasetParser = new StandardQueryParser();
	public static final StandardQueryParser investigationParser = new StandardQueryParser();
	public static final StandardQueryParser sampleParser = new StandardQueryParser();

    static {
		doubleFields.addAll(Arrays.asList("numericValue", "numericValueSI"));
		facetFields.addAll(Arrays.asList("type.name", "datafileFormat.name", "stringValue"));
		longFields.addAll(Arrays.asList("date", "startDate", "endDate", "dateTimeValue", "investigation.startDate"));
		sortFields.addAll(Arrays.asList("datafile.id", "dataset.id", "investigation.id", "instrument.id", "id", "date",
				"startDate", "endDate", "name", "stringValue", "dateTimeValue", "numericValue", "numericValueSI"));
		textFields.addAll(Arrays.asList("name", "visitId", "description", "location", "dataset.name",
				"investigation.name", "instrument.name", "instrument.fullName", "datafileFormat.name", "sample.name",
				"sample.type.name", "title", "summary", "facility.name", "user.fullName", "type.name"));

		indexedEntities.addAll(Arrays.asList("Datafile", "Dataset", "Investigation", "DatafileParameter",
				"DatasetParameter", "InstrumentScientist", "InvestigationInstrument", "InvestigationParameter",
				"InvestigationUser", "Sample"));

		relationships.put("Instrument",
				new ParentRelationship[] { new ParentRelationship("InvestigationInstrument", "instrument") });
		relationships.put("User", new ParentRelationship[] { new ParentRelationship("InvestigationUser", "user"),
				new ParentRelationship("InstrumentScientist", "user") });
		relationships.put("Sample", new ParentRelationship[] { new ParentRelationship("Dataset", "sample") });
		relationships.put("SampleType", new ParentRelationship[] { new ParentRelationship("Sample", "type"),
				new ParentRelationship("Dataset", "sample.type") });
		relationships.put("InvestigationType",
				new ParentRelationship[] { new ParentRelationship("Investigation", "type") });
		relationships.put("DatasetType", new ParentRelationship[] { new ParentRelationship("Dataset", "type") });
		relationships.put("DatafileFormat",
				new ParentRelationship[] { new ParentRelationship("Datafile", "datafileFormat") });
		relationships.put("Facility", new ParentRelationship[] { new ParentRelationship("Investigation", "facility") });
		relationships.put("ParameterType",
				new ParentRelationship[] { new ParentRelationship("DatafileParameter", "type"),
						new ParentRelationship("DatasetParameter", "type"),
						new ParentRelationship("InvestigationParameter", "type") });
		relationships.put("Investigation",
				new ParentRelationship[] { new ParentRelationship("Dataset", "investigation"),
						new ParentRelationship("datafile", "investigation") });

		genericParser.setAllowLeadingWildcard(true);
		genericParser.setAnalyzer(analyzer);

		CharSequence[] datafileFields = { "name", "description", "location", "datafileFormat.name" };
		datafileParser.setAllowLeadingWildcard(true);
		datafileParser.setAnalyzer(analyzer);
		datafileParser.setMultiFields(datafileFields);

		CharSequence[] datasetFields = { "name", "description", "sample.name", "sample.type.name", "type.name" };
		datasetParser.setAllowLeadingWildcard(true);
		datasetParser.setAnalyzer(analyzer);
		datasetParser.setMultiFields(datasetFields);

		CharSequence[] investigationFields = { "name", "visitId", "title", "summary", "facility.name",
				"type.name" };
		investigationParser.setAllowLeadingWildcard(true);
		investigationParser.setAnalyzer(analyzer);
		investigationParser.setMultiFields(investigationFields);

		CharSequence[] sampleFields = { "name", "type.name" };
		sampleParser.setAllowLeadingWildcard(true);
		sampleParser.setAnalyzer(analyzer);
		sampleParser.setMultiFields(sampleFields);
    }
}
