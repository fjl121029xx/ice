package com.hll.ice;

import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MergSmallFileApp {

    public static void main(String[] args) throws ParseException {


//        Configuration conf = new Configuration();
//        String warehousePath = args[0];
//        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

//
        // Using a Hive catalog
        SparkConf sparkconf = new SparkConf().setAppName("MergSmallFileApp");

        sparkconf.set("write.metadata.delete-after-commit.enabled", "true");

        SparkSession spark = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate();
        Catalog catalog = new HiveCatalog(spark.sparkContext().hadoopConfiguration());
        TableIdentifier name = TableIdentifier.of(args[0], args[1]);
        Table table = catalog.loadTable(name);
        System.out.println(table.location());

        Actions.forTable(table).removeOrphanFiles().execute();
        Actions.forTable(table).rewriteManifests().execute();
        Actions.forTable(table).expireSnapshots().execute();
        // 1 day
        long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 1);
        table.expireSnapshots()
                .expireOlderThan(tsToExpire)
                .commit();

//        Actions.forTable(table).rewriteDataFiles()
//                .filter(Expressions.and(Expressions.equal("group_id", Integer.parseInt(args[3])), Expressions.lessThanOrEqual("report_date", args[4] + "T23:59:59.0000+08:00"), Expressions.greaterThanOrEqual("report_date", args[5] + "T00:00:00.0000+08:00")))
//                .targetSizeInBytes(Long.parseLong(args[2])) // 10KB
//                .execute();
    }
}
