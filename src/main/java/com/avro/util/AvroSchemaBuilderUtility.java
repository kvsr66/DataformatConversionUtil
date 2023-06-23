package com.avro.util;


import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;

public class AvroSchemaBuilderUtility {

    public static Schema readAvroSchemaFromSchemaFile(String avroSchemafile) throws IOException {
        Schema schema = null;

        schema = new Schema.Parser().parse(new File(avroSchemafile));

        return schema;


    }
}
