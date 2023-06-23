package com.avro.util;

import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroMeta;
import org.apache.avro.reflect.AvroSchema;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class AvroToCsvConverter {

    public static void main(String[] args) throws IOException {
       String avroSchemaFile = "D:\\subbu\\workspaces\\intellij-workspace\\DataFormatUtilityProject\\src\\main\\resources\\avro\\EventRecord.avsc";
        createEntity(avroSchemaFile);
    }

    public static void createEntity(String avroFilePath) throws IOException {
        String basePath = "D:\\subbu\\workspaces\\intellij-workspace\\DataFormatUtilityProject\\src\\main\\resources\\avro\\";
        String entitySchemaFile = basePath + "Entity.csv";
        String fieldSchemaFile = basePath+ "Fields.csv";
        StringBuilder entityBuilder = createEntityHeader();
        StringBuilder fieldBuilder = createFieldsHeader();

        Schema avroSchema = AvroSchemaBuilderUtility.readAvroSchemaFromSchemaFile(avroFilePath);

         int entityId = 1;
        if(null!=avroSchema){

            for(Schema.Field field : avroSchema.getFields()){
                if("record".equalsIgnoreCase(String.valueOf(field.getObjectProp("type")))){

                    entityBuilder.append(entityId + ",").append(",");
                    entityBuilder.append(field.getObjectProp("name")).append(",");


                    entityBuilder.append(field.getObjectProp("alias")).append(",");
                    entityBuilder.append(field.getObjectProp("doc")).append(","); //description
                    entityBuilder.append("PK").append(",");//primary_key
                    entityBuilder.append("AVRO").append(","); //object_type
                    entityBuilder.append(field.getObjectProp("name")).append(","); // object_name
                    entityBuilder.append("\n");

                } else {

                    fieldBuilder.append(entityId).append(","); // entity_id
                    fieldBuilder.append(entityId).append(","); //"field_no,");
                    fieldBuilder.append(field.getObjectProp("name")).append(","); //field_name
                    fieldBuilder.append(field.getObjectProp("type")).append(","); //"data_type,");
                    fieldBuilder.append(field.getObjectProps().containsValue("null")? "Y": "N").append(","); //is_nullable
                    fieldBuilder.append("array".equalsIgnoreCase(String.valueOf(field.getObjectProp("type"))) ? "Y": "N").append(","); //("is_repeated,");
                    fieldBuilder.append("enum".equalsIgnoreCase(String.valueOf(field.getObjectProp("type"))) ? "Y" : "N" ) ;//"is_enum");
                    fieldBuilder.append("\n");
                }

            }

        }

        FileOutputStream entityOut =  new FileOutputStream(entitySchemaFile);
        entityOut.write(entityBuilder.toString().getBytes());

        FileOutputStream fieldsOut =  new FileOutputStream(fieldSchemaFile);
        entityOut.write(fieldBuilder.toString().getBytes());


    }

    public static StringBuilder createEntityHeader(){

    StringBuilder entityHeader = new StringBuilder();
    entityHeader.append("entity_id,");
    entityHeader.append("entity-name,");
    entityHeader.append("alias,");
    entityHeader.append("description");
    entityHeader.append("primary_key");
    entityHeader.append("object_type");
    entityHeader.append("object_name");
    entityHeader.append("\n");

    return entityHeader;
  }

  public static  StringBuilder createFieldsHeader(){
        StringBuilder fieldsHeader = new StringBuilder();

      fieldsHeader.append("entity_id,");
      fieldsHeader.append("field_no,");
      fieldsHeader.append("field_name,");
      fieldsHeader.append("data_type,");
      fieldsHeader.append("is_nullable,");
      fieldsHeader.append("is_repeated,");
      fieldsHeader.append("is_enum");
//      fieldsHeader.append("");
//      fieldsHeader.append("");
//      fieldsHeader.append("");
//      fieldsHeader.append("");
//      fieldsHeader.append("");
//      fieldsHeader.append("");
//      fieldsHeader.append("");
      fieldsHeader.append("\n");

      return fieldsHeader;
  }



}
