package remote.jdbc;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.server.AvaticaProtobufHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class MyJdbcServer {

    public static void main(String[] args) {
        try {
            HttpServer start = Main.start(new String[]{/*META.FACTORY*/AvaticaMetaFactoryImplementation.class.getName()}, 9091,
                    AvaticaProtobufHandler::new);
            System.out.println("Started at port "+ start.getPort());
            start.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class AvaticaMetaFactoryImplementation implements Meta.Factory {

        static String model = "{\n"+
                "  \"version\": \"1.0\",\n"+
                "  \"defaultSchema\": \"ENHANCED\",\n"+
                "  \"schemas\": [\n"+
                "     {\n" +
                "       \"name\": \"STREAMS\",\n" +
                "       \"tables\": [ {\n" +
                "         \"type\": \"custom\",\n" +
                "         \"name\": \"ORDERS\",\n" +
                "         \"stream\": {\n" +
                "           \"stream\": true\n" +
                "         },\n" +
                "         \"factory\": \"remote.jdbc.OrderTableFactory\"\n" +
                "       } ]\n" +
                "     }   ]\n" +
                "}";

        private static JdbcMeta instance = null;
        private static JdbcMeta getInstance() {
            if (instance == null) {
                Properties info = new Properties();
                info.setProperty("lex", "JAVA");
                info.setProperty("model", "inline:" + model);
                try {
                    instance = new JdbcMeta("jdbc:calcite:",
                            info);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            return instance;
        }
        @Override public Meta create(List<String> args) {
            return getInstance();
        }
    }
}
