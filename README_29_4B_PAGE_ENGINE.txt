29-4B PAGE ENGINE 제작 완료

적용 내용:
1. 사이드바 메뉴 data-page 기반으로 정리
2. page-home / page-dashboard / page-game / page-mission / page-ranking / page-referral / page-wallet / page-mining / page-settings 연결
3. 출석보상 attendance 키를 기존 page-mission과 호환 처리
4. 마지막으로 열었던 페이지 localStorage 저장
5. URL 해시(#game, #wallet 등) 지원
6. 없는 페이지 클릭 시 홈으로 안전 복귀
7. 게임/출석/랭킹/지갑/채굴장 진입 시 loadCBK 자동 호출

다음 단계:
29-4C 게임센터 페이지 분리/고급화
