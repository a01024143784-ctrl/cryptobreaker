CryptoBreaker V1.6.2 SAFE DETAIL SPEED

적용 내용:
- 원본 코인 클릭 구조(openCoinDetail 호출부/리스트 렌더링) 변경 없음
- 상세창 API 연속 클릭 시 이전 요청 자동 취소
- 상세 API 3.5초 타임아웃 적용
- 같은 코인 3초 내 재요청 캐시 처리
- 상세 자동 갱신 10초 -> 15초로 완화
- full=1 무거운 호출 미사용 유지

테스트 순서:
1. run_29_3A8_BODY_PADDING_SIDEBAR_FIX.bat 실행
2. 코인 목록 클릭 확인
3. 상세창 즉시 표시 확인
4. 여러 코인을 빠르게 눌러도 마지막 클릭 코인만 표시되는지 확인
