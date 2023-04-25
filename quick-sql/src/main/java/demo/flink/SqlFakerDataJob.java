package demo.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class SqlFakerDataJob {

    public static void main(String[] args) {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        env.executeSql("CREATE TEMPORARY TABLE heros (\n" +
                "`name` STRING,\n" +
                "`power` STRING,\n" +
                "`age` INT\n" +
                ") WITH (\n" +
                "'connector' = 'faker', \n" +
                "'fields.name.expression' = '#{superhero.name}',\n" +
                "'fields.power.expression' = '#{superhero.power}',\n" +
                "'fields.power.null-rate' = '0.05',\n" +
                "'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}'\n" +
                ")");

        env.executeSql("CREATE TABLE print_table (\n" +
                "`name` STRING,\n" +
                "`power` STRING,\n" +
                "`age` INT\n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ")");

        env.executeSql("INSERT INTO print_table select * from heros");


    }
}
