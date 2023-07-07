package com.avro.util;

import org.apache.avro.Schema;
import org.apache.commons.collections.CollectionUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class AvroToCsvConverterNew {

    public static void main(String[] args) throws IOException {
        String schemaDir = "\\src\\main\\resources\\avro\\";
        String avroSchemaFile = schemaDir + "EventRecord.avsc";
        AvroToCsvConverterNew converter = new AvroToCsvConverterNew();
        converter.convertAvroSchemaToMetaDataCSV(avroSchemaFile);

    }

    public void convertAvroSchemaToMetaDataCSV(String avroFilePath) throws IOException {

        String basePath = "\\src\\main\\resources\\avro\\";
        String entitySchemaFile = basePath + "Entity.csv";
        String fieldSchemaFile = basePath + "Fields.csv";

        Schema avroSchema = AvroSchemaBuilderUtility.readAvroSchemaFromSchemaFile(avroFilePath);

        generateMetadata(avroSchema, entitySchemaFile, fieldSchemaFile);

    }

    public void generateMetadata(Schema avroSchema, String entityFilePath, String fieldsFilePath) throws IOException {

        StringBuilder entityBuilder = createEntityHeader();
        StringBuilder fieldBuilder = createFieldsHeader();

        int entityId = 1;

        if (null != avroSchema) {
            entityBuilder.append(buildEntityMetaData(avroSchema, null, entityId));

            List<Schema.Field> fields = avroSchema.getFields();

            if (CollectionUtils.isNotEmpty(fields)) {
                for (Schema.Field locaField : fields) {
                    int fieldId = 1;
                    fieldBuilder.append(buildFieldsMetaData(null, locaField, entityId, fieldId));
                    fieldId++;
                    if (Schema.Type.RECORD == locaField.schema().getType()) {
                        entityBuilder.append(buildEntityMetaData(locaField.schema(), null, entityId));
                        entityId++;
                    }
                }
            }
        }

        writeToFile(entityBuilder.toString(), entityFilePath);

        writeToFile(fieldBuilder.toString(), fieldsFilePath);

    }

    public StringBuilder buildEntityMetaData(Schema schema, Schema.Field field, Integer entityId) {

        StringBuilder entityDataBuilder = new StringBuilder();
        if (null != schema) {
            entityDataBuilder.append(entityId).append(",");
            entityDataBuilder.append(schema.getName()).append(",");
            entityDataBuilder.append(schema.getObjectProps().get("alias")).append(",");
            entityDataBuilder.append(schema.getDoc()).append(","); //description
            entityDataBuilder.append("PK").append(",");//primary_key
            entityDataBuilder.append(schema.getNamespace()).append(","); //object_type
            entityDataBuilder.append(schema.getName()).append(","); // object_name
            entityDataBuilder.append(schema.getNamespace()).append(schema.getName()).append(","); // package_name
            entityDataBuilder.append("\n");
        } else if (null != field) {
            entityDataBuilder.append(entityId + 1).append(",");
            entityDataBuilder.append(field.name()).append(",");
            entityDataBuilder.append(field.getObjectProp("alias")).append(",");
            entityDataBuilder.append(field.doc()).append(","); //description
            entityDataBuilder.append("PK").append(",");//primary_key
            entityDataBuilder.append(field.schema().getNamespace()).append(","); //object_type
            entityDataBuilder.append(field.name()).append(","); // object_name
            entityDataBuilder.append("\n");
        }
        return entityDataBuilder;
    }

    public StringBuilder buildFieldsMetaData(Schema schema, Schema.Field field, int entityId, int fieldId) {

        StringBuilder fieldDataBuilder = new StringBuilder();
        fieldDataBuilder.append(entityId).append(","); // entity_id
        fieldDataBuilder.append(fieldId + 1).append(","); //"field_no,");
        fieldDataBuilder.append(field.name()).append(","); //field_name
        fieldDataBuilder.append(field.getObjectProps().containsValue("null") ? "Y" : "N").append(","); //is_nullable
        fieldDataBuilder.append("array".equalsIgnoreCase(field.schema().getType().getName()) ? "Y" : "N").append(","); //("is_repeated,");
        fieldDataBuilder.append("enum".equalsIgnoreCase(field.schema().getType().getName()) ? "Y" : "N").append(",");//"is_enum");
        fieldDataBuilder.append("==").append(",");// "code_type"
        fieldDataBuilder.append(null != field.getObjectProp("size") ? field.getObjectProp("size") : "==").append(",");//data_length
        fieldDataBuilder.append(null != field.getObjectProp("precision") ? field.getObjectProp("precision") : "==").append(",");//data_precision
        fieldDataBuilder.append(null != field.getObjectProp("scale") ? field.getObjectProp("scale") : "==").append(",");//data_scale
        fieldDataBuilder.append(null != field.getObjectProp("default") ? field.getObjectProp("default") : "==").append(",");//defaut_value
        fieldDataBuilder.append(field.schema().getType().getName()).append(","); //"data_type,");
        fieldDataBuilder.append("\n");

        return fieldDataBuilder;
    }


    public static StringBuilder createEntityHeader() {

        StringBuilder entityHeader = new StringBuilder();
        entityHeader.append("entity_id,");
        entityHeader.append("entity-name,");
        entityHeader.append("alias,");
        entityHeader.append("description,");
        entityHeader.append("primary_key,");
        entityHeader.append("object_type,");
        entityHeader.append("object_name,");
        entityHeader.append("package_name,");
        entityHeader.append("\n");

        return entityHeader;
    }

    public static StringBuilder createFieldsHeader() {
        StringBuilder fieldsHeader = new StringBuilder();

        fieldsHeader.append("entity_id,");
        fieldsHeader.append("field_no,");
        fieldsHeader.append("field_name,");
        fieldsHeader.append("is_nullable,");
        fieldsHeader.append("is_repeated,");
        fieldsHeader.append("is_enum,");
        fieldsHeader.append("code_type,");
        fieldsHeader.append("size,");
        fieldsHeader.append("precision,");
        fieldsHeader.append("scale,");
        fieldsHeader.append("default,");
        fieldsHeader.append("data_type,");
//      fieldsHeader.append("");
//      fieldsHeader.append("");
        fieldsHeader.append("\n");


        return fieldsHeader;
    }

    public static void writeToFile(String data, String filePath) throws IOException {

        FileOutputStream out = new FileOutputStream(filePath);
        out.write(data.getBytes());
        out.close();


    }

}
