28-2D FIX

수정 내용:
- 서버 연결 실패 문구가 렌더링 오류에도 뜨던 문제 완화
- /api/dashboard 응답 실패와 화면 렌더 오류를 분리 처리
- /api/dominance, /api/oi-flow, /api/fear-greed 보조 API 추가
- 텔레그램 봇 안내 패널 숨김 처리
- 기존 COMMAND CENTER / MARKET OVERVIEW / OI FLOW / SURGE-DROP CARD PRO 유지

실행:
1) 압축 풀기
2) python app.py
3) http://127.0.0.1:5000 접속
