package com.flipkart.fsql.planner;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public interface IQueryPlanner {
    public RelRoot getLogicalPlan(String sql) throws SqlParseException, RelConversionException, ValidationException;

}
