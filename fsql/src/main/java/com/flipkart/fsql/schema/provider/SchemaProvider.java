package com.flipkart.fsql.schema.provider;

import org.apache.calcite.schema.SchemaPlus;

import java.io.IOException;

public interface SchemaProvider {
    SchemaPlus registerSchema() throws Exception;
}
