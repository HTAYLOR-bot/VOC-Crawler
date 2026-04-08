# Google Shopping US 타이어 리뷰 크롤러

Streamlit + Playwright 기반으로 **Google Shopping(미국)** 에서 브랜드명/상품명으로 상품을 찾고, 사용자 리뷰를 끝까지 수집해 CSV로 저장하는 프로젝트입니다.

## 지원 기능
- Google Shopping 미국 검색 (`brand + product`)
- 상품명 대소문자 무시 매칭
- `ion evo as` 검색 시 `ion evo as suv` 제외 (정확 키워드 우선)
- 리뷰 정렬: 관련성순 → 최근 리뷰순 시도
- `리뷰 더보기 / More reviews` 버튼 반복 클릭 + 스크롤로 전체 리뷰 수집
- 상대 날짜(예: `6 days ago`, `1 week ago`)를 크롤링 실행일 기준 절대 날짜로 변환
- 날짜 범위 필터 (`from`, `to`)
  - from/to 모두 비우면 전체
  - from만 입력하면 from ~ 오늘
  - to만 입력하면 과거 ~ to
- 크롤링 중 상태 로그/미리보기 표시
- `Resume(인증 후 재개)`, `일시중단`, `중단` 버튼 지원
- CSV 다운로드

## 컬럼 구성
1. No.
2. 브랜드명
3. 상품명
4. 리뷰 계정명
5. 별점
6. 날짜
7. 리뷰 내용
8. 출처 웹사이트

## 실행

### Windows (권장)
아래 배치 파일만 실행하면 환경 구성 후 UI를 바로 띄웁니다.

```bat
run_crawler.bat
```

### 수동 실행
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
streamlit run app.py
```

## 주의
- Google 화면 구조/셀렉터는 변경될 수 있어 보수 조정이 필요할 수 있습니다.
- 실제 운영 전에는 테스트 키워드로 충분히 검증하세요.
- 대상 사이트의 이용약관 및 관련 법규를 준수하세요.
