29-4I-4B-8 SOCKET SPEED OPTIMIZATION

적용 내용
1) 웹소켓 메시지마다 새 스레드를 만들던 구조를 제거
2) ws_dirty 이벤트 + 단일 워커 방식으로 0.25초 단위 병합 처리
3) SSE 실시간 출력은 유지하되 브라우저 렌더링을 requestAnimationFrame으로 묶음
4) 웹소켓 정상 연결 시 REST API 호출을 줄이고, 끊겼을 때만 백업 호출
5) /api/dashboard에 ws_connected, ws_last_recv 상태값 추가
6) 홈/시장 페이지에서 DOM 갱신 중복을 줄여 체감 렉 감소

목표
- 실시간 출력 반응속도 개선
- CPU 사용량 감소
- 방송 중 화면 끊김/버벅임 감소
- REST/API 과호출 감소

실행
python app.py
http://localhost:5000
