package demo.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class SqlJob {

    public static void main(String[] args) {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        env.executeSql("CREATE TABLE datagen (\n" +
                "    f_sequence INT,\n" +
                "    f_random INT,\n" +
                "    f_random_str STRING,\n" +
                "    ts AS localtimestamp,\n" +
                "    WATERMARK FOR ts AS ts\n" +
                "  ) WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second'='5',\n" +
                "    'fields.f_sequence.kind'='sequence',\n" +
                "    'fields.f_sequence.start'='1',\n" +
                "    'fields.f_sequence.end'='500',\n" +
                "    'fields.f_random.min'='1',\n" +
                "    'fields.f_random.max'='500',\n" +
                "    'fields.f_random_str.length'='10'\n" +
                "  )");

        env.executeSql(" CREATE TABLE print_table (\n" +
                "    f_sequence INT,\n" +
                "    f_random INT,\n" +
                "    f_random_str STRING\n" +
                "    ) WITH (\n" +
                "    'connector' = 'print'\n" +
                "  )");

        env.executeSql("INSERT INTO print_table select f_sequence,f_random,f_random_str from datagen");


    }
}
