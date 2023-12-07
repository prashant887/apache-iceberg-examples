package aws;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.SnapshotUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class GetSnapshotId {
    public static void main(String[] args) throws ParseException {

        Catalog catalog = new GlueCatalog();

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("warehouse", "s3://vmware-euc-cloud/data-dir/temp/transforms");
        catalog.initialize("artemis", properties);

        Date date = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse("2023/01/01 00:00:00");
        long millis = date.getTime();

        TableIdentifier name = TableIdentifier.of("artemis", "iceberg_employees_partition");
        Table table = catalog.loadTable(name);
        Snapshot oldestSnapshotIdAfter2022 = SnapshotUtil.oldestAncestorAfter(table, millis);

        FileIO io=new S3FileIO();
        io.initialize(new HashMap<>());

        System.out.println(oldestSnapshotIdAfter2022.snapshotId());
        System.out.println(oldestSnapshotIdAfter2022.operation());
        System.out.println(oldestSnapshotIdAfter2022.parentId());
        System.out.println(oldestSnapshotIdAfter2022.manifestListLocation());
        System.out.println();
        oldestSnapshotIdAfter2022.addedDataFiles(io).forEach(x-> System.out.println(x));



    }
}
