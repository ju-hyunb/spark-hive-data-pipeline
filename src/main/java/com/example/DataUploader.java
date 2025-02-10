package com.example;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.when;

public class DataUploader {
    private final SparkSession spark;

    public DataUploader(SparkSession spark) {
        this.spark = spark;
    }

    public void createExternalTable(String tableName, String location) {
        spark.sql("DROP TABLE IF EXISTS " + tableName);
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + " ("
                + "user_id STRING, event_time TIMESTAMP, event_time_kst TIMESTAMP, session_id STRING"
                + ") PARTITIONED BY (event_date DATE) STORED AS PARQUET LOCATION '" + location + "'");
        System.out.println("External Table `" + tableName + "` 생성 완료.");
    }

    public Dataset<Row> readAndMergeCSV(List<String> paths) {
        Dataset<Row> mergedDF = spark.read().option("header", "true").csv(paths.get(0));
        for (int i = 1; i < paths.size(); i++) {
            mergedDF = mergedDF.union(spark.read().option("header", "true").csv(paths.get(i)));
        }
        return mergedDF;
    }

    public Dataset<Row> convertUtcToKst(Dataset<Row> df) {
        return df
            .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) // ✅ 문자열을 TIMESTAMP로 변환
            .withColumn("event_time_kst", expr("event_time + interval 9 hours")) // ✅ KST로 변환
            .withColumn("event_date", to_date(col("event_time_kst"))); // ✅ 날짜 컬럼 추가
    }   

    public Dataset<Row> createSessionIdByEventDate(Dataset<Row> df, int cond_minute) {
        // Window 함수 정의
        WindowSpec windowSpec = Window.partitionBy("user_id").orderBy("event_time_kst");
    
        // 이전 이벤트 시간 컬럼 생성
        df = df.withColumn("prev_event_time", lag("event_time_kst", 1).over(windowSpec));
    
        // 시간 차이 계산
        df = df.withColumn("time_diff", unix_timestamp(col("event_time_kst"))
                .minus(unix_timestamp(col("prev_event_time"))));
    
        // 새로운 세션 플래그 생성 (5분 이상 차이나면 새로운 세션)
        df = df.withColumn("new_session_flag", when(col("prev_event_time").isNull()
                .or(col("time_diff").geq(cond_minute * 60)), 1).otherwise(0));
    
        // 세션 ID 생성 (user_id + 누적 합계)
        df = df.withColumn("session_id", concat_ws("_", col("user_id").cast("string"),
                sum("new_session_flag").over(windowSpec)));
    
        return df;
    }

    public void saveToHive(Dataset<Row> df, String tableName, String location) {
        df.write().mode("append").partitionBy("event_date").format("parquet").save(location);
        System.out.println("`" + tableName + "` 테이블에 데이터 저장 완료.");
        spark.sql("MSCK REPAIR TABLE " + tableName);
    }
}
