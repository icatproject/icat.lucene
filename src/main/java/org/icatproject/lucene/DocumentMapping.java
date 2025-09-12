package org.icatproject.lucene;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.icatproject.lucene.analyzers.IcatSeparatorAnalyzer;
import org.icatproject.lucene.analyzers.IcatSynonymAnalyzer;

public class DocumentMapping {

	/**
	 * Represents the parent child relationship between two ICAT entities.
	 */
	public static class ParentRelationship {
		public String parentName;
		public String joiningField;
		public boolean cascadeDelete;
		public Map<String, String> fieldMapping;

		/**
		 * @param parentName    Name of the parent entity.
		 * @param joiningField  Field that joins the child to its parent.
		 * @param cascadeDelete If the child is deleted, whether the parent onto which
		 *                      it is nested should be deleted wholesale or just have
		 *                      its fields pruned.
		 * @param fieldMapping  Fields that should be updated by this relationship. The
		 * 						key and value will be the same for most fields, but for
		 * 						some they will differ to allow fields to be flattened
		 * 						across entities (e.g. dataset.name: name).
		 */
		public ParentRelationship(String parentName, String joiningField, boolean cascadeDelete,
				Map<String, String> fieldMapping) {
			this.parentName = parentName;
			this.joiningField = joiningField;
			this.cascadeDelete = cascadeDelete;
			this.fieldMapping = fieldMapping;
		}
	}

	public static final Set<String> doubleFields = Set.of("numericValue", "numericValueSI", "rangeTop", "rangeTopSI",
			"rangeBottom", "rangeBottomSI");
	public static final Set<String> longFields = Set.of("date", "startDate", "endDate", "dateTimeValue",
			"investigation.startDate", "fileSize", "fileCount", "datafile.id", "datafileFormat.id", "dataset.id",
			"facility.id", "facilityCycle.id", "investigation.id", "instrument.id", "id", "sample.id",
			"sample.investigation.id", "sample.type.id", "technique.id", "type.id", "user.id");
	public static final Set<String> sortFields = Set.of("datafile.id", "datafileFormat.id", "dataset.id", "facility.id",
			"facilityCycle.id", "investigation.id", "instrument.id", "id", "sample.id", "sample.investigation.id",
			"technique.id", "type.id", "user.id", "date", "name", "stringValue", "dateTimeValue", "numericValue",
			"numericValueSI", "fileSize", "fileCount");
	public static final Set<String> textFields = Set.of("name", "visitId", "description", "dataset.name",
			"investigation.name", "instrument.name", "instrument.fullName", "datafileFormat.name", "sample.name",
			"sample.type.name", "technique.name", "technique.description", "technique.pid", "title", "summary",
			"facility.name", "user.fullName", "type.name", "doi");
	public static final Set<String> pathFields = Set.of("location");
	public static final Set<String> indexedEntities = Set.of("Datafile", "Dataset", "Investigation",
			"DatafileParameter", "DatasetParameter", "DatasetTechnique", "InstrumentScientist",
			"InvestigationFacilityCycle", "InvestigationInstrument", "InvestigationParameter", "InvestigationUser",
			"Sample", "SampleParameter");
	public static final Map<String, ParentRelationship[]> relationships = Map.ofEntries(
			Map.entry("Instrument", new ParentRelationship[] {
					new ParentRelationship("InvestigationInstrument", "instrument.id", true,
							Map.of("instrument.name", "instrument.name", "instrument.fullName", "instrument.fullName")) }),
			Map.entry("User", new ParentRelationship[] {
					new ParentRelationship("InvestigationUser", "user.id", true,
							Map.of("user.name", "user.name", "user.fullName", "user.fullName")),
					new ParentRelationship("InstrumentScientist", "user.id", true,
							Map.of("user.name", "user.name", "user.fullName", "user.fullName")) }),
			Map.entry("Sample", new ParentRelationship[] {
					new ParentRelationship("Dataset", "sample.id", false,
							Map.of("sample.name", "sample.name", "sample.investigation.id", "sample.investigation.id")),
					new ParentRelationship("Datafile", "sample.id", false,
							Map.of("sample.name", "sample.name", "sample.investigation.id", "sample.investigation.id")) }),
			Map.entry("SampleType", new ParentRelationship[] {
					new ParentRelationship("Sample", "type.id", true, Map.of("type.name", "type.name")),
					new ParentRelationship("Dataset", "sample.type.id", false,
							Map.of("sample.type.name", "sample.type.name")),
					new ParentRelationship("Datafile", "sample.type.id", false,
							Map.of("sample.type.name", "sample.type.name")) }),
			Map.entry("InvestigationType", new ParentRelationship[] {
					new ParentRelationship("Investigation", "type.id", true, Map.of("type.name", "type.name")) }),
			Map.entry("DatasetType", new ParentRelationship[] {
					new ParentRelationship("Dataset", "type.id", true, Map.of("type.name", "type.name")) }),
			Map.entry("DatafileFormat", new ParentRelationship[] {
					new ParentRelationship("Datafile", "datafileFormat.id", false,
							Map.of("datafileFormat.name", "datafileFormat.name")) }),
			Map.entry("Facility", new ParentRelationship[] {
					new ParentRelationship("Investigation", "facility.id", true,
							Map.of("facility.name", "facility.name")) }),
			Map.entry("ParameterType", new ParentRelationship[] {
					new ParentRelationship("DatafileParameter", "type.id", true,
							Map.of("type.name", "type.name", "type.units", "type.units")),
					new ParentRelationship("DatasetParameter", "type.id", true,
							Map.of("type.name", "type.name", "type.units", "type.units")),
					new ParentRelationship("InvestigationParameter", "type.id", true,
							Map.of("type.name", "type.name", "type.units", "type.units")),
					new ParentRelationship("SampleParameter", "type.id", true,
							Map.of("type.name", "type.name", "type.units", "type.units")) }),
			Map.entry("Technique", new ParentRelationship[] {
					new ParentRelationship("DatasetTechnique", "technique.id", true,
							Map.of("technique.name", "technique.name", "technique.description", "technique.description",
									"technique.pid", "technique.pid")) }),
			Map.entry("Investigation", new ParentRelationship[] {
					new ParentRelationship("Dataset", "investigation.id", true,
							Map.of("visitId", "visitId", "investigation.name", "name", "investigation.title", "title",
									"investigation.startDate", "startDate")),
					new ParentRelationship("Datafile", "investigation.id", true,
							Map.of("visitId", "visitId", "investigation.name", "name")) }),
			Map.entry("Dataset", new ParentRelationship[] {
					new ParentRelationship("Datafile", "dataset.id", true, Map.of("dataset.name", "name")) }));

	public static final StandardQueryParser genericParser = buildParser();
	public static final StandardQueryParser datafileParser = buildParser("name", "description", "location",
			"location.fileName", "datafileFormat.name", "visitId", "sample.name", "sample.type.name", "doi");
	public static final StandardQueryParser datasetParser = buildParser("name", "description", "sample.name",
			"sample.type.name", "type.name", "visitId", "doi");
	public static final StandardQueryParser investigationParser = buildParser("name", "visitId", "title", "summary",
			"facility.name", "type.name", "doi");
	public static final StandardQueryParser sampleParser = buildParser("sample.name", "sample.type.name");

	private static StandardQueryParser buildParser(String... defaultFields) {
		HashMap<String, Analyzer> analyzerMap = new HashMap<>();
		for (String pathField : pathFields) {
			analyzerMap.put(pathField, new IcatSeparatorAnalyzer("/"));
			analyzerMap.put(pathField + ".exact", new KeywordAnalyzer());
			analyzerMap.put(pathField + ".fileName", new IcatSeparatorAnalyzer("."));
		}
		PerFieldAnalyzerWrapper analyzerWrapper = new PerFieldAnalyzerWrapper(new IcatSynonymAnalyzer(), analyzerMap);
		StandardQueryParser parser = new StandardQueryParser(analyzerWrapper);

		StandardQueryConfigHandler qpConf = (StandardQueryConfigHandler) parser.getQueryConfigHandler();
		qpConf.set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, true);
		if (defaultFields.length > 0) {
			qpConf.set(ConfigurationKeys.MULTI_FIELDS, defaultFields);
		}

		return parser;
	}
}
