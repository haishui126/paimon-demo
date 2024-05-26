package com.haishui;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;

/**
 * @author haishui
 */
public class PaimonApiSinkDemo {
    private static final String WAREHOUSE_PATH = "D:\\warehouse";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStream<Row> input = env
                .fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Alice", 100))
                .returns(Types.ROW_NAMED(new String[]{"name", "age"}, Types.STRING, Types.INT));

        // get table from catalog
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", WAREHOUSE_PATH);
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);

        // create database/table if not exists
        catalog.createDatabase("db_test", true);
        Identifier identifier = Identifier.create("db_test", "t_test");
        Schema schema = Schema
                .newBuilder()
                .column("name", org.apache.paimon.types.DataTypes.STRING())
                .column("age", org.apache.paimon.types.DataTypes.INT())
                .build();
        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);

        LogicalType logicalType = LogicalTypeConversion.toLogicalType(schema.rowType());
        DataType inputType = TypeConversions.fromLogicalToDataType(logicalType);
        new FlinkSinkBuilder(table)
                .forRow(input, inputType)
                .inputBounded(true)
                .build();

        env.execute();
    }
}
