package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class WAUProcessor {
    private final SparkSession spark;

    public WAUProcessor(SparkSession spark) {
        this.spark = spark;
    }

    //user_id 기준 주간 활성 사용자(WAU) 계산
    public Dataset<Row> calculateWeeklyWAUByUser(String tableName) {
        String query = "SELECT " +
                "  TO_DATE(DATE_FORMAT(event_date, 'yyyy-ww-1')) AS week_start_date, " +
                "  COUNT(DISTINCT user_id) AS wau_by_user " +
                "FROM " + tableName + " " +
                "GROUP BY week_start_date " +
                "ORDER BY week_start_date";
        return spark.sql(query);
    }

    //session_id 기준 주간 활성 사용자(WAU) 계산
    public Dataset<Row> calculateWeeklyWAUBySession(String tableName) {
        String query = "SELECT " +
                "  TO_DATE(DATE_FORMAT(event_date, 'yyyy-ww-1')) AS week_start_date, " +
                "  COUNT(DISTINCT session_id) AS wau_by_session " +
                "FROM " + tableName + " " +
                "GROUP BY week_start_date " +
                "ORDER BY week_start_date";

        return spark.sql(query);
    }

    //주간 WAU 결과를 CSV로 저장
    public void saveToCSV(Dataset<Row> df, String outputPath) {
        df.coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);
        System.out.println("WAU 결과 저장 완료: " + outputPath);
    }

    //WAU 계산 수행 및 결과 저장
    public void processWAU(String tableName, String outputDir) {
        System.out.println("주간 WAU 계산 시작...");

        // 주별 user_id 기준 WAU 계산
        Dataset<Row> weeklyWAUByUser = calculateWeeklyWAUByUser(tableName);
        saveToCSV(weeklyWAUByUser, outputDir + "/weekly_wau_by_user.csv");

        // 주별 session_id 기준 WAU 계산
        Dataset<Row> weeklyWAUBySession = calculateWeeklyWAUBySession(tableName);
        saveToCSV(weeklyWAUBySession, outputDir + "/weekly_wau_by_session.csv");

        System.out.println("주간 WAU 계산 완료.");
    }

    /**
     * Main 함수 (이 파일 단독 실행 가능)
     */
    @SuppressWarnings("CallToPrintStackTrace")
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("WAU Processor")
                .master("local[*]")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .config("spark.sql.shuffle.partitions", "50")
                .config("spark.driver.memory", "12g")
                .config("spark.executor.memory", "8g")
                .enableHiveSupport()
                .getOrCreate();

        try {
            String tableName = "ecommerce_data";
            String outputDir = "./output";

            WAUProcessor wauProcessor = new WAUProcessor(spark);
            wauProcessor.processWAU(tableName, outputDir);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
            System.out.println("SparkSession 종료.");
        }
    }
}
