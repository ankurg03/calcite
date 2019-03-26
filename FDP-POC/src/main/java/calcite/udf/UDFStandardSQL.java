package calcite.udf;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.csv.CsvSchemaFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;



public class UDFStandardSQL {
    public static void main(String[] args) throws SQLException {
        final Properties properties = new Properties();
        try (Connection connection =
                     DriverManager.getConnection("jdbc:calcite:", properties)) {
            final CalciteConnection calciteConnection = connection.unwrap(
                    CalciteConnection.class);

            URL resource = UDFStandardSQL.class.getResource("");
            System.out.println(resource);


            final Schema schema =
                    CsvSchemaFactory.INSTANCE
                            .create(calciteConnection.getRootSchema(), null,
                                    ImmutableMap.of("directory",
                                            "FDP-POC/target/classes/sales", "flavor", "scannable"));
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            SchemaPlus post = rootSchema.add("TEST", schema);

            testBasics(calciteConnection);

            testScalarUDF(calciteConnection, post);


            testTableUDF(calciteConnection, post);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void testScalarUDF(CalciteConnection calciteConnection, SchemaPlus post) throws SQLException {
        //test for user define function
        post.add("SQUARE_TEST",
                ScalarFunctionImpl.create(SquareUDF.class, "eval"));

        final String sql_UDF = "select TEST.SQUARE_TEST(select TEST.DEPTS.DEPTNO from TEST.DEPTS where NAME = ?)";

        final PreparedStatement statement =
                calciteConnection.prepareStatement(sql_UDF);
        statement.setString(1, "Sales");


        ResultSet resultSet2 = statement.executeQuery();

        while (resultSet2.next()) {
            System.out.println(resultSet2.getString(1));
        }
    }

    private static void testBasics(CalciteConnection calciteConnection) throws SQLException {
        final String sql = "select * from \"TEST\".\"DEPTS\" where \"NAME\" = ?";
        final PreparedStatement statement2 =
                calciteConnection.prepareStatement(sql);
        statement2.setString(1, "Sales");
        ResultSet resultSet = statement2.executeQuery();
        System.out.println(resultSet.getMetaData().getColumnLabel(1) + " " +
                "" + resultSet.getMetaData().getColumnLabel(2));
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
        }

    }

    public static class SquareUDF {
        public long eval(int x) {
            return x * x;
        }
    }


    private static void testTableUDF(CalciteConnection calciteConnection, SchemaPlus post) throws SQLException {
        Method FIBONACCI_TABLE_METHOD =
                Types.lookupMethod(TableFunctionTest.class, "fibonacciTableWithLimit", long.class);
        TableFunction table = TableFunctionImpl.create(FIBONACCI_TABLE_METHOD);
        post.add("FIB", table);

        final String sql_UDF = "select TEST.fibonacciTableWithLimit(select TEST.DEPTS.DEPTNO from TEST.DEPTS where NAME = ?)";
        final String sql_UDF_D = "select * from table(TEST.FIB(10))";

        final PreparedStatement statement =
                calciteConnection.prepareStatement(sql_UDF_D);
//        statement.setString(1, "Marketing");

        ResultSet resultSet2 = statement.executeQuery();

        while (resultSet2.next()) {
            System.out.println(resultSet2.getString(1));
        }
    }


    public static class TableFunctionTest {

        public static ScannableTable fibonacciTableWithLimit(final long limit) {
            return new ScannableTable() {
                public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                    return typeFactory.builder().add("N", SqlTypeName.BIGINT).build();
                }

                public Statistic getStatistic() {
                    return Statistics.UNKNOWN;
                }

                public Schema.TableType getJdbcTableType() {
                    return Schema.TableType.TABLE;
                }

                public boolean isRolledUp(String column) {
                    return false;
                }

                public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
                                                            SqlNode parent, CalciteConnectionConfig config) {
                    return true;
                }

                public Enumerable<Object[]> scan(DataContext root) {
                    return new AbstractEnumerable<Object[]>() {
                        public Enumerator<Object[]> enumerator() {
                            return new Enumerator<Object[]>() {
                                private long prev = 1;
                                private long current = 0;

                                public Object[] current() {
                                    return new Object[]{current};
                                }

                                public boolean moveNext() {
                                    final long next = current + prev;
                                    if (limit >= 0 && next > limit) {
                                        return false;
                                    }
                                    prev = current;
                                    current = next;
                                    return true;
                                }

                                public void reset() {
                                    prev = 0;
                                    current = 1;
                                }

                                public void close() {
                                }
                            };
                        }
                    };
                }
            };
        }
    }
}
