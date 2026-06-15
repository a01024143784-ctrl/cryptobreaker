CryptoBreaker V1.7.0 SOCKET STEP1

적용 내용:
- MEXC WebSocket/SSE 실시간 출력 1단계 안정화
- WS 렌더 최소 간격 0.25초 -> 0.15초
- 프론트 렌더 최소 간격 280ms -> 180ms
- WS 정상 연결 시 REST 백업 호출 부담 완화
- 화면 카운트다운 문구를 MEXC 소켓 상태로 표시

테스트 순서:
1) run_29_3A8_BODY_PADDING_SIDEBAR_FIX.bat 또는 기존 실행 bat 실행
2) CMD에 [WS] MEXC 선물 웹소켓 연결됨 문구 확인
3) 화면에 ⚡ MEXC 소켓 연결됨 / ⚡ MEXC 소켓 실시간 표시 확인
4) 코인 클릭, 상세창, TradingView 정상 확인
