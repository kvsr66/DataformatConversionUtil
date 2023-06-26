package com.avro.util;

import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroMeta;
import org.apache.avro.reflect.AvroSchema;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class AvroToCsvConverter {

    public static void main(String[] args) throws IOException {
       String schemaDir = "D:\\subbu\\workspaces\\intellij-workspace\\DataFormatUtilityProject\\src\\main\\resources\\avro\\";
        String avroSchemaFile = schemaDir + "multiplercords.avsc";
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
         int fielid = 0;
        if(null!=avroSchema){

            entityBuilder.append(entityId).append(",");
            entityBuilder.append(avroSchema.getName()).append(",");
            entityBuilder.append(avroSchema.getAliases()).append(",");
            entityBuilder.append(avroSchema.getDoc()).append(","); //description
            entityBuilder.append("PK").append(",");//primary_key
            entityBuilder.append(avroSchema.getNamespace()).append(","); //object_type
            entityBuilder.append(avroSchema.getName()).append(","); // object_name
            entityBuilder.append("\n");


            for(Schema.Field field : avroSchema.getFields()){
                if("record".equalsIgnoreCase(String.valueOf(field.getObjectProp("type")))){

                    entityBuilder.append(entityId+1).append(",");
                    entityBuilder.append(field.name()).append(",");
                    entityBuilder.append(field.aliases()).append(",");
                    entityBuilder.append(field.doc()).append(","); //description
                    entityBuilder.append("PK").append(",");//primary_key
                    entityBuilder.append(field.schema().getNamespace()).append(","); //object_type
                    entityBuilder.append(field.name()).append(","); // object_name
                    entityBuilder.append("\n");

                } else {
                    fielid = fielid+1;
                    fieldBuilder.append(entityId).append(","); // entity_id
                    fieldBuilder.append(fielid).append(","); //"field_no,");
                    fieldBuilder.append(field.name()).append(","); //field_name
                    fieldBuilder.append(field.getObjectProps().containsValue("null")? "Y": "N").append(","); //is_nullable
                    fieldBuilder.append("array".equalsIgnoreCase(String.valueOf(field.getObjectProp("type"))) ? "Y": "N").append(","); //("is_repeated,");
                    fieldBuilder.append("enum".equalsIgnoreCase(String.valueOf(field.getObjectProp("type"))) ? "Y" : "N" ) .append(",");//"is_enum");
                    fieldBuilder.append("==").append(",");// "code_type"
                    fieldBuilder.append(null!=field.getObjectProp("size")? field.getObjectProp("size") : "==").append(",");//data_length
                    fieldBuilder.append(null!=field.getObjectProp("precision")? field.getObjectProp("precision") : "==").append(",");//data_precision
                    fieldBuilder.append(null!=field.getObjectProp("scale")? field.getObjectProp("scale") : "==").append(",");//data_scale
                    fieldBuilder.append(null!=field.getObjectProp("default")? field.getObjectProp("default") : "==").append(",");//defaut_value
                    fieldBuilder.append(field.schema().getType().getName()).append(","); //"data_type,");
                    fieldBuilder.append("\n");


                }

            }

        }

        FileOutputStream entityOut =  new FileOutputStream(entitySchemaFile);
        entityOut.write(entityBuilder.toString().getBytes());
        entityOut.close();

        FileOutputStream fieldsOut =  new FileOutputStream(fieldSchemaFile);
        fieldsOut.write(fieldBuilder.toString().getBytes());
        fieldsOut.close();


    }

    public static StringBuilder createEntityHeader(){

    StringBuilder entityHeader = new StringBuilder();
    entityHeader.append("entity_id,");
    entityHeader.append("entity-name,");
    entityHeader.append("alias,");
    entityHeader.append("description,");
    entityHeader.append("primary_key,");
    entityHeader.append("object_type,");
    entityHeader.append("object_name,");
    entityHeader.append("\n");

    return entityHeader;
  }

  public static  StringBuilder createFieldsHeader(){
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



}
