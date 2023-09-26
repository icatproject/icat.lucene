package org.icatproject.lucene;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;

public class DocumentMapping {

	/**
	 * Represents the parent child relationship between two ICAT entities.
	 */
	public static class ParentRelationship {
		public String parentName;
		public String joiningField;
		public Set<String> fields;

		/**
		 * @param parentName   Name of the parent entity.
		 * @param joiningField Field that joins the child to its parent.
		 * @param fields       Fields that should be updated by this relationship.
		 */
		public ParentRelationship(String parentName, String joiningField, String... fields) {
			this.parentName = parentName;
			this.joiningField = joiningField;
			this.fields = new HashSet<>(Arrays.asList(fields));
		}
	}

	private static Analyzer analyzer = new IcatSynonymAnalyzer();;

	public static final Set<String> doubleFields = new HashSet<>();
	public static final Set<String> facetFields = new HashSet<>();
	public static final Set<String> longFields = new HashSet<>();
	public static final Set<String> sortFields = new HashSet<>();
	public static final Set<String> textFields = new HashSet<>();
	public static final Set<String> indexedEntities = new HashSet<>();
	public static final Map<String, ParentRelationship[]> relationships = new HashMap<>();

	public static final StandardQueryParser genericParser = buildParser();
	public static final StandardQueryParser datafileParser = buildParser("name", "description", "location",
			"datafileFormat.name", "visitId", "sample.name", "sample.type.name", "doi");
	public static final StandardQueryParser datasetParser = buildParser("name", "description", "sample.name",
			"sample.type.name", "type.name", "visitId", "doi");
	public static final StandardQueryParser investigationParser = buildParser("name", "visitId", "title", "summary",
			"facility.name", "type.name", "doi");
	public static final StandardQueryParser sampleParser = buildParser("sample.name", "sample.type.name");

	static {
		doubleFields.addAll(Arrays.asList("numericValue", "numericValueSI", "rangeTop", "rangeTopSI", "rangeBottom",
				"rangeBottomSI"));
		facetFields.addAll(Arrays.asList("type.name", "datafileFormat.name", "stringValue", "technique.name"));
		longFields.addAll(
				Arrays.asList("date", "startDate", "endDate", "dateTimeValue", "investigation.startDate", "fileSize",
						"fileCount", "datafile.id", "datafileFormat.id", "dataset.id", "facility.id",
						"facilityCycle.id", "investigation.id", "instrument.id", "id", "sample.id",
						"sample.investigation.id", "sample.type.id", "technique.id", "type.id", "user.id"));
		sortFields.addAll(
				Arrays.asList("datafile.id", "datafileFormat.id", "dataset.id", "facility.id", "facilityCycle.id",
						"investigation.id", "instrument.id", "id", "sample.id", "sample.investigation.id",
						"technique.id", "type.id", "user.id", "date", "name", "stringValue", "dateTimeValue",
						"numericValue", "numericValueSI", "fileSize", "fileCount"));
		textFields.addAll(Arrays.asList("name", "visitId", "description", "location", "dataset.name",
				"investigation.name", "instrument.name", "instrument.fullName", "datafileFormat.name", "sample.name",
				"sample.type.name", "technique.name", "technique.description", "technique.pid", "title", "summary",
				"facility.name", "user.fullName", "type.name", "doi"));

		indexedEntities.addAll(Arrays.asList("Datafile", "Dataset", "Investigation", "DatafileParameter",
				"DatasetParameter", "DatasetTechnique", "InstrumentScientist", "InvestigationFacilityCycle",
				"InvestigationInstrument", "InvestigationParameter", "InvestigationUser", "Sample", "SampleParameter"));

		relationships.put("Instrument",
				new ParentRelationship[] { new ParentRelationship("InvestigationInstrument", "instrument.id",
						"instrument.name", "instrument.fullName") });
		relationships.put("User",
				new ParentRelationship[] {
						new ParentRelationship("InvestigationUser", "user.id", "user.name", "user.fullName"),
						new ParentRelationship("InstrumentScientist", "user.id", "user.name", "user.fullName") });
		relationships.put("Sample", new ParentRelationship[] {
				new ParentRelationship("Dataset", "sample.id", "sample.name", "sample.investigation.id"),
				new ParentRelationship("Datafile", "sample.id", "sample.name", "sample.investigation.id") });
		relationships.put("SampleType",
				new ParentRelationship[] { new ParentRelationship("Sample", "type.id", "type.name"),
						new ParentRelationship("Dataset", "sample.type.id", "sample.type.name"),
						new ParentRelationship("Datafile", "sample.type.id", "sample.type.name") });
		relationships.put("InvestigationType",
				new ParentRelationship[] { new ParentRelationship("Investigation", "type.id", "type.name") });
		relationships.put("DatasetType",
				new ParentRelationship[] { new ParentRelationship("Dataset", "type.id", "type.name") });
		relationships.put("DatafileFormat",
				new ParentRelationship[] {
						new ParentRelationship("Datafile", "datafileFormat.id", "datafileFormat.name") });
		relationships.put("Facility",
				new ParentRelationship[] { new ParentRelationship("Investigation", "facility.id", "facility.name") });
		relationships.put("ParameterType",
				new ParentRelationship[] { new ParentRelationship("DatafileParameter", "type.id", "type.name"),
						new ParentRelationship("DatasetParameter", "type.id", "type.name"),
						new ParentRelationship("InvestigationParameter", "type.id", "type.name"),
						new ParentRelationship("SampleParameter", "type.id", "type.name") });
		relationships.put("Technique",
				new ParentRelationship[] { new ParentRelationship("DatasetTechnique", "technique.id", "technique.name",
						"technique.description", "technique.pid") });
		relationships.put("Investigation",
				new ParentRelationship[] {
						new ParentRelationship("Dataset", "investigation.id", "investigation.name",
								"investigation.title", "investigation.startDate", "visitId"),
						new ParentRelationship("datafile", "investigation.id", "investigation.name", "visitId") });
		relationships.put("Dataset",
				new ParentRelationship[] { new ParentRelationship("Datafile", "dataset.id", "dataset.name") });
	}

	private static StandardQueryParser buildParser(String... defaultFields) {
		StandardQueryParser parser = new StandardQueryParser();
		StandardQueryConfigHandler qpConf = (StandardQueryConfigHandler) parser.getQueryConfigHandler();
		qpConf.set(ConfigurationKeys.ANALYZER, analyzer);
		qpConf.set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, true);
		if (defaultFields.length > 0) {
			qpConf.set(ConfigurationKeys.MULTI_FIELDS, defaultFields);
		}

		return parser;
	}
}
