package com.flipkart.fsql.planner;

import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.util.ArrayList;

import java.util.List;

public class CalciteQueryPlanner implements IQueryPlanner {

    final Planner planner;

    public CalciteQueryPlanner(SchemaPlus rootSchema ) {
        this.planner = buildPlanner(rootSchema);
    }

    private Planner buildPlanner(SchemaPlus rootSchema) {
        final List<RelTraitDef> traitDefs = new ArrayList<>();

        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);
        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder().setLex(Lex.JAVA).build())
                .defaultSchema(rootSchema)
                .traitDefs(traitDefs)
                .context(Contexts.EMPTY_CONTEXT)
                .costFactory(null)
                .build();
        return Frameworks.getPlanner(frameworkConfig);
    }

    @Override
    public synchronized RelRoot getLogicalPlan(String sql) throws SqlParseException, RelConversionException, ValidationException {
        SqlNode sqlNode = planner.parse(sql);
        SqlNode validatedSql = planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(validatedSql);
        planner.close();
        planner.reset();
        return relRoot;
    }

    private static Table createTableFromRelSchema(RelDataType relationalSchema) {
        return new AbstractTable() {
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return relationalSchema;
            }
        };
    }
}
