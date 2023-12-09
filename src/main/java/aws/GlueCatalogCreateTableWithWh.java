package aws;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlueCatalogCreateTableWithWh {


    public static void main(String[] args) {

        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
       properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://vmware-euc-cloud/data-dir/iceberg-wh/");

        GlueCatalog catalog=new GlueCatalog();
        catalog.initialize("glue",properties);
        List<Namespace> namespces=catalog.listNamespaces();
        for (Namespace namespace:namespces){
            System.out.println(namespace);
        }

        Namespace artemis = Namespace.of("artemis");
        List<TableIdentifier> tables=catalog.listTables(artemis);

        for (TableIdentifier table:tables){
            System.out.println(table.name());
        }

        TableIdentifier name = TableIdentifier.of("artemis", "iceberg_table_with_loc");

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "event", Types.StringType.get()),
                Types.NestedField.required(3, "event_type", Types.StringType.get())

        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("event_type")
                .build();

        Map<String, String> tblProps = new HashMap<>();

        tblProps.put("write.spark.accept-any-schema","true");



        Table table = catalog.createTable(name,schema,spec,tblProps);

        TableIdentifier tableIdentifier=TableIdentifier.of("artemis","iceberg_table_with_db");

       // Table table=catalog.loadTable(tableIdentifier);

        System.out.println("Table Props");

        table.properties().forEach((x,y)-> System.out.println(x+" = "+y));

        System.out.println(table.location());

      //  catalog.dropTable(tableIdentifier);


        System.out.println("Tables After ");
        catalog.listTables(artemis).forEach(x-> System.out.println(x));




    }
}
