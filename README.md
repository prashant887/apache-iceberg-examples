While Creating table if WH directory is used , WH Param is used tables are created as 
<WH_DIR>/<DBNAME>/<TABLENAME>
catalog.createTable(name,schema,spec,tblProps);
s3://vmware-euc-cloud/data-dir/temp/transforms/artemis.db/iceberg_table_with_db


if Location is used , table is created in Location Dir
catalog.createTable(name,schema,spec,location,tblProps);
s3://vmware-euc-cloud/data-dir/temp/transforms/iceberg_table_with_loc