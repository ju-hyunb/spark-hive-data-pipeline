## Spark-Hive Data Pipeline (Java)

### 1. 자바 언어 선택 이유
주 언어는 **Python**이지만, 최근 프로젝트에서 **Java**를 사용해본 경험이 있어 선택했습니다.  

---

### 2. 📂 `src/` 파일 구조
 **프로젝트의 주요 Java 파일과 역할 설명**


#### **각 Java 파일 설명**
1. **`DataUploader.java`**  
   - Hive 테이블을 생성하고 데이터를 업로드하는 역할을 담당  
   - `event_time`을 `event_time_kst`로 변환  
   - `session_id`를 생성하여 Hive에 저장  

2. **`UploadToHive.java`**  
   - **SparkSession**을 생성하고 Hive와 연동  
   - Hive에서 데이터를 불러와서 Spark SQL로 분석 가능하게 함  

3. **`WAUProcessor.java`**  
   - **WAU(Weekly Active Users) 계산**  
   - `user_id` 기준 및 `session_id` 기준으로 주별 활성 사용자(WAU) 수를 집계  
   - 결과를 CSV 파일로 저장  

---

### 3. 과제 완료 여부
현재 **코드만 작성하여 업로드**하였으며, 실제 실행 및 검증은 완료되지 않았습니다.

-  **코드 작성 완료**
-  **실제 실행 및 검증 미완료**
