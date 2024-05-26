package com.haishui;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

/**
 * @author haishui
 */
public class PaimonApiSourceDemo {
    private static final String WAREHOUSE_PATH = "D:\\warehouse";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // get table from catalog
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", WAREHOUSE_PATH);
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        Table table = catalog.getTable(Identifier.create("db_test", "t_test"));

        DataStream<Row> paimonSource = new FlinkSourceBuilder(table)
                .env(env)
                .sourceBounded(true)
                .buildForRow();

        paimonSource.print();

        env.execute();
    }
}
