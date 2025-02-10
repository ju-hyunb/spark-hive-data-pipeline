package com.example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class UploadToHive {
    public static void main(String[] args) {
        // SparkSession 생성
        SparkSession spark = SparkSession.builder()
                .appName("Spark Upload")
                .master("local[*]")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .config("spark.sql.shuffle.partitions", "50")
                .config("spark.driver.memory", "12g")
                .config("spark.executor.memory", "8g")
                .config("spark.memory.fraction", "0.8")
                .config("spark.memory.storageFraction", "0.5")
                .enableHiveSupport()
                .getOrCreate();

        String tableName = "ecommerce_data";
        String tableLocation = "./ecommerce_data";
        DataUploader uploader = new DataUploader(spark);

        //External Table 생성
        uploader.createExternalTable(tableName, tableLocation);

        //CSV 파일 로드
        List<String> csvPaths = Arrays.asList("./data/2019-Oct.csv", "./data/2019-Nov.csv");
        Dataset<Row> df = uploader.readAndMergeCSV(csvPaths);

        //UTC -> KST 변환
        df = uploader.convertUtcToKst(df);
        df.show(5, false);


        //세션 ID 생성
        df = uploader.createSessionIdByEventDate(df, 5);

        //데이터 저장
        uploader.saveToHive(df, tableName, tableLocation);

        //Spark 종료
        spark.stop();
        System.out.println("SparkSession 종료.");
    }
}
