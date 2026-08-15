# 시설공통자재 가격정보 파일 임포트 기록

작성일: 2026-06-11

## 원천 파일

사용자 제공 파일:

```text
C:\Users\COMTREE\Desktop\UI-ADOSAA-003R.시설공통자재 가격정보 내역.xlsx
```

서버 보관 파일:

```text
/opt/busan/import_facility_material_price_20260611.xlsx
```

확인 결과:

```text
보고서ID: UI-ADOSAA-003R
시트명: UI-ADOSAA-003R.시설공통자재 가격정보 내역
출력일자: 2026-06-11
실데이터 행: 400,070
컬럼 수: 13
게시연도 범위: 2007~2026
```

주요 컬럼:

```text
물품분류번호
물품분류명
물품식별번호
규격명/한글품목명
단위
가격
인도조건명
공급관할지역명
조달청담당부서명
게시일자
삭제여부
부가가치세포함여부
포탈가격업무구분명
```

## 성격

이 파일은 현재 유효 가격만 담은 파일이 아니라, 과거 가격 이력과 삭제 품목이 함께 들어 있는 스냅샷이다.

검사 결과:

```text
삭제여부 Y: 372,585
삭제여부 N: 27,485
파일 내부 중복 행: 21,572
```

따라서 원본 행을 그대로 업체추천/품목안내에 사용하면 안 되고, 최신 게시일자와 삭제여부를 기준으로 현재 유효 가격정보를 별도로 산출해야 한다.

## 구축 테이블

서버 DB:

```text
/opt/busan/chatbot_company.db
```

생성 테이블:

```text
facility_material_price_master
facility_material_price_current
facility_material_item_dictionary
facility_material_price_change_history
facility_material_price_import_log
```

테이블 역할:

```text
facility_material_price_master
  원천 파일의 가격 이력 행을 중복 제거 후 보존한다.

facility_material_price_current
  물품/단위/인도조건/공급지역/분야별 최신 게시일자 행 중 삭제여부=N인 가격만 보존한다.

facility_material_item_dictionary
  물품분류번호, 물품식별번호, 품명, 규격명, 분야를 검색 가능한 품목 사전으로 만든다.

facility_material_price_change_history
  다음 파일 임포트 시 신규, 변경, 최신 파일 누락 상태를 기록한다.

facility_material_price_import_log
  파일 단위 임포트 결과를 기록한다.
```

## 증분관리 기준

원천행 키:

```text
source_row_key =
  포탈가격업무구분명
  + 물품분류번호
  + 물품분류명
  + 물품식별번호
  + 규격명/한글품목명
  + 단위
  + 인도조건명
  + 공급관할지역명
  + 게시일자
```

행 변경 감지:

```text
row_hash = 원천행 전체 정규화 JSON의 SHA-256
```

현재 가격 그룹 키:

```text
current_group_key =
  포탈가격업무구분명
  + 물품분류번호
  + 물품식별번호
  + 단위
  + 인도조건명
  + 공급관할지역명
```

처리 규칙:

```text
신규 source_row_key:
  insert

기존 source_row_key + row_hash 변경:
  update + change_history 기록

기존 source_row_key + row_hash 동일:
  unchanged

이전 DB에는 있으나 최신 파일에 없는 source_row_key:
  missing_in_latest_import=1

current 테이블:
  current_group_key별 최신 게시일자 행 중 삭제여부=N인 것만 유지

dictionary 테이블:
  과거/현재 원천행을 합쳐 품목명, 규격명, 물품분류, 물품식별 검색 사전 생성
```

## 2026-06-11 최초 임포트 결과

실행 스크립트:

```text
/opt/busan/import_facility_material_price_file.py
```

로컬 원본:

```text
C:\Users\COMTREE\Desktop\busanproduct-advisor\scripts\import_facility_material_price_file.py
```

서버 실행 결과:

```text
row_count = 400,070
inserted = 378,498
updated = 0
unchanged = 21,572
current_active_count = 7,814
dictionary_count = 21,708
```

서버 DB 검증:

```text
facility_material_price_master = 378,498
facility_material_price_current = 7,814
facility_material_item_dictionary = 21,708
facility_material_price_change_history = 378,498
facility_material_price_import_log = 1
```

최신 유효 가격정보 분야별 분포:

```text
시설자재(기계설비분야): 3,691
시설자재(건축분야): 2,278
시설자재(전기분야): 1,543
시설자재(토목분야): 302
합계: 7,814
```

최신 유효 가격 게시일 범위:

```text
2024-10-16 ~ 2026-04-20
```

품목사전:

```text
facility_material_item_dictionary 전체: 21,708
active_price_count > 0 항목: 7,026
```

## 재임포트 검증

같은 파일을 로컬 임시 DB에 최초 임포트한 뒤 `--force`로 재임포트했다.

결과:

```text
inserted = 0
updated = 0
unchanged = 400,070
current_active_count = 7,814
dictionary_count = 21,708
```

따라서 동일 파일 재처리 시 신규/변경으로 잘못 잡히지 않고, 증분관리 구조가 작동한다.

## 운영 방식

사용자가 새 `UI-ADOSAA-003R` 파일을 내려받으면 서버에 업로드한 뒤 다음 명령으로 임포트한다.

```bash
cd /opt/busan
/opt/busan/venv/bin/python3 /opt/busan/import_facility_material_price_file.py \
  /opt/busan/import_facility_material_price_YYYYMMDD.xlsx \
  --db /opt/busan/chatbot_company.db
```

같은 파일을 강제로 재처리할 때만 `--force`를 사용한다.

```bash
/opt/busan/venv/bin/python3 /opt/busan/import_facility_material_price_file.py \
  /opt/busan/import_facility_material_price_YYYYMMDD.xlsx \
  --db /opt/busan/chatbot_company.db \
  --force
```

## 업체추천 활용 원칙

이 데이터는 업체 자격 데이터가 아니다. 따라서 업체 추천 점수에 직접 반영하지 않는다.

활용 위치:

```text
1. 사용자가 입력한 자재명/규격명을 물품분류번호, 물품식별번호, 표준 품명으로 정규화
2. 시설공통자재 가격정보 존재 여부 안내
3. 공사용자재/시설자재 성격의 품목인지 판단 보조
4. 가격 자체는 참고자료로만 표시
```

프런트 표시는 다음 수준이 적절하다.

```text
조달청 시설공통자재 가격정보 있음
최신 게시일: YYYY-MM-DD
참고가격: 금액
단위/인도조건/공급지역
물품분류번호/물품식별번호
주의: 조달청 시설공사 원가계산 참고 가격이며 실제 계약 가능 여부 또는 업체 자격을 의미하지 않음
```

