package aws;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlueCatalogExample {

    static class MetaData {
        int id;

        String name;

        String dataType;

        public MetaData(int id, String name, String dataType) {
            this.id = id;
            this.name = name;
            this.dataType = dataType;
        }

        @Override
        public String toString() {
            return "MetaData{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", dataType='" + dataType + '\'' +
                    '}';
        }
    }
    public static void main(String[] args) {

        Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
      //  properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://vmware-euc-cloud/data-dir/iceberg-wh/");

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

        TableIdentifier ctas_iceberg_parquet = TableIdentifier.of(artemis, "ctas_iceberg_parquet");


        Table table=catalog.loadTable(ctas_iceberg_parquet);
        System.out.println(table.location());
        System.out.println(table.properties());
        System.out.println(table.schemas());
        System.out.println(table.schema());
        System.out.println(table.locationProvider().toString());
        System.out.println(table.io().properties());
        table.newScan().planFiles().forEach(x-> System.out.println(x));
        table.schemas().forEach((x,y)-> System.out.println(x + " "+y));
        System.out.println(table.snapshots());


        Schema schema=table.schema();

        System.out.println(schema.asStruct());


        Region region = Region.US_WEST_2;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        Map<String, String> props =
                ImmutableMap.of(
                        "s3.delete.batch-size",
                        "5");

        FileIO io=new S3FileIO();
        io.initialize(new HashMap<>());


        System.out.println(io.properties());

        StaticTableOperations staticTableOperations=new StaticTableOperations(
                "s3://vmware-euc-cloud/data-dir/temp/transforms/UTD_ICE/metadata/00000-e86be9ae-82ed-48d0-b533-e50fc9932b38.metadata.json",io
        );



        TableMetadata metadata = staticTableOperations.current();

        System.out.println(metadata.schema());

        Schema schema_new=table.schema();

        System.out.println(schema_new.asStruct());
      //  System.out.println(schema.caseInsensitiveFindField("customer_ids"));

      //  System.out.println(schema.findField("customer_id"));

        List<MetaData> metaDataList=new ArrayList<>();
        schema_new.columns().forEach(x->{
            metaDataList.add(new MetaData(x.fieldId(),x.name(),x.type().toString()));
        });

        metaDataList.forEach(System.out::println);

        // schema.columns().add(Types.NestedField.optional(7,"hotel_rating",Types.IntegerType.get()));

        System.out.println(schema_new.columns());


        CloseableIterable<Record> result = IcebergGenerics.read(table).build();


        for (Record r: result) {
           r.struct().fields().forEach(
                   x-> System.out.println(r.getField(x.name()))
           );
        }




    }
}
