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
		public boolean cascadeDelete;
		public Map<String, String> fieldMapping;

		/**
		 * @param parentName    Name of the parent entity.
		 * @param joiningField  Field that joins the child to its parent.
		 * @param cascadeDelete If the child is deleted, whether the parent onto which
		 * 						it is nested should be deleted wholesale or just have
		 * 						its fields pruned.
		 * @param fields        Fields that should be updated by this relationship where
		 * 					    the field is the same on parent and child.
		 */
		public ParentRelationship(String parentName, String joiningField, boolean cascadeDelete, String... fields) {
			this.parentName = parentName;
			this.joiningField = joiningField;
			this.cascadeDelete = cascadeDelete;
			fieldMapping = new HashMap<>();
			for (String field : fields) {
				fieldMapping.put(field, field);
			}
		}

		/**
		 * @param parentField Name on the parent, such as "dataset.name"
		 * @param childField  Name on the child, such as "name"
		 */
		public void mapField(String parentField, String childField) {
			fieldMapping.put(parentField, childField);
		}
	}

	private static Analyzer analyzer = new IcatSynonymAnalyzer();;

	public static final Set<String> doubleFields = new HashSet<>();
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

		relationships.put("Instrument", new ParentRelationship[] {
				new ParentRelationship("InvestigationInstrument", "instrument.id", true, "instrument.name",
						"instrument.fullName") });
		relationships.put("User", new ParentRelationship[] {
				new ParentRelationship("InvestigationUser", "user.id", true, "user.name", "user.fullName"),
				new ParentRelationship("InstrumentScientist", "user.id", true, "user.name", "user.fullName") });
		relationships.put("Sample", new ParentRelationship[] {
				new ParentRelationship("Dataset", "sample.id", false, "sample.name", "sample.investigation.id"),
				new ParentRelationship("Datafile", "sample.id", false, "sample.name", "sample.investigation.id") });
		relationships.put("SampleType", new ParentRelationship[] {
				new ParentRelationship("Sample", "type.id", true, "type.name"),
				new ParentRelationship("Dataset", "sample.type.id", false, "sample.type.name"),
				new ParentRelationship("Datafile", "sample.type.id", false, "sample.type.name") });
		relationships.put("InvestigationType",
				new ParentRelationship[] { new ParentRelationship("Investigation", "type.id", true, "type.name") });
		relationships.put("DatasetType",
				new ParentRelationship[] { new ParentRelationship("Dataset", "type.id", true, "type.name") });
		relationships.put("DatafileFormat",
				new ParentRelationship[] {
						new ParentRelationship("Datafile", "datafileFormat.id", false, "datafileFormat.name") });
		relationships.put("Facility",
				new ParentRelationship[] { new ParentRelationship("Investigation", "facility.id", true, "facility.name") });
		relationships.put("ParameterType",
				new ParentRelationship[] {
						new ParentRelationship("DatafileParameter", "type.id", true, "type.name", "type.units"),
						new ParentRelationship("DatasetParameter", "type.id", true,"type.name", "type.units"),
						new ParentRelationship("InvestigationParameter", "type.id", true, "type.name", "type.units"),
						new ParentRelationship("SampleParameter", "type.id", true, "type.name", "type.units") });
		relationships.put("Technique",
				new ParentRelationship[] { new ParentRelationship("DatasetTechnique", "technique.id", true,"technique.name",
						"technique.description", "technique.pid") });

		ParentRelationship investigationDatasetRelationship = new ParentRelationship("Dataset", "investigation.id",
				true, "visitId");
		investigationDatasetRelationship.mapField("investigation.name", "name");
		investigationDatasetRelationship.mapField("investigation.title", "title");
		investigationDatasetRelationship.mapField("investigation.startDate", "startDate");
		ParentRelationship investigationDatafileRelationship = new ParentRelationship("Datafile", "investigation.id",
				true,"visitId");
		investigationDatafileRelationship.mapField("investigation.name", "name");
		relationships.put("Investigation", new ParentRelationship[] {investigationDatasetRelationship, investigationDatafileRelationship });

		ParentRelationship datasetDatafileRelationship = new ParentRelationship("Datafile", "dataset.id", true);
		datasetDatafileRelationship.mapField("dataset.name", "name");
		relationships.put("Dataset", new ParentRelationship[] { datasetDatafileRelationship });
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
