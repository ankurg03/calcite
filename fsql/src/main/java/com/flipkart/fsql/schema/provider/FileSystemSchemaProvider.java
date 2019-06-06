package com.flipkart.fsql.schema.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.fsql.JsonUtility;
import com.flipkart.fsql.planner.CalciteQueryPlanner;
import com.flipkart.fsql.schema.model.SQLSchema;
import com.flipkart.fsql.schema.model.SqlFieldSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FileSystemSchemaProvider implements SchemaProvider {

    private final String resourceDir;
    private final Connection connection;
    private final RelSchemaConverter relSchemaConverter;

    public FileSystemSchemaProvider( Connection connection, String resourceDirForSchema) {
        this.resourceDir = resourceDirForSchema;
        this.connection = connection;
        this.relSchemaConverter = new RelSchemaConverter();
    }

    @Override
    public SchemaPlus registerSchema() throws Exception {

        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        for (File file : getResourceDirFiles()) {
            String schemaName = file.getName();
            JsonNode jsonSchemaNode = JsonUtility.mapper.readTree(file);
            SQLSchema sqlSchema = JsonUtility.mapper.treeToValue(jsonSchemaNode, SQLSchema.class);
            List<String> fieldNames = new ArrayList<>();
            List<SqlFieldSchema> fieldTypes = new ArrayList<>();
            fieldNames.addAll(
                    sqlSchema.getFields().stream().map(SQLSchema.SqlField::getFieldName).collect(Collectors.toList()));
            fieldTypes.addAll(
                    sqlSchema.getFields().stream().map(SQLSchema.SqlField::getFieldSchema).collect(Collectors.toList()));

            SQLSchema newSchema = new SQLSchema(fieldNames, fieldTypes);

            RelDataType relationalSchema = relSchemaConverter.convertToRelSchema(newSchema);
            System.out.println(relationalSchema);
            rootSchema.add(schemaName, createTableFromRelSchema(relationalSchema));
        }
        return rootSchema;
    }

    private static Table createTableFromRelSchema(RelDataType relationalSchema) {
        return new AbstractTable() {
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return relationalSchema;
            }
        };
    }

    private File[] getResourceDirFiles () throws Exception {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL url = loader.getResource(resourceDir);
        String path = url.toURI().getPath();
        return new File(path).listFiles();
    }

    public static void main (String[] args) throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        FileSystemSchemaProvider fs = new FileSystemSchemaProvider(connection, "schemas");
        SchemaPlus sp = fs.registerSchema();
        CalciteQueryPlanner calciteQueryPlanner = new CalciteQueryPlanner(sp);
        RelRoot relNode = calciteQueryPlanner.getLogicalPlan("select ns.phoneNumbers, nm.name from  NestedExampleSchema as ns, FlatSchemaExample as nm where " +
                "nm.name = ns.name");
        System.out.println(RelOptUtil.toString(relNode.project()));

        RelRoot relNode2 = calciteQueryPlanner.getLogicalPlan("select ns.name, sum(ns.id) from  NestedExampleSchema as ns group by (ns.name)");
        System.out.println(RelOptUtil.toString(relNode2.project()));
/**
 * Output
 * LogicalProject(phoneNumbers=[$5], name=[$8])
 *   LogicalFilter(condition=[=($8, $1)])
 *     LogicalJoin(condition=[true], joinType=[inner])
 *       EnumerableTableScan(table=[[NestedExampleSchema]])
 *       EnumerableTableScan(table=[[FlatSchemaExample]])
 *
 * LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])
 *   LogicalProject(name=[$1], id=[$0])
 *     EnumerableTableScan(table=[[NestedExampleSchema]])
 */


    }
}
