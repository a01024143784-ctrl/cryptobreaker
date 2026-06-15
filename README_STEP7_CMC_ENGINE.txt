CryptoBreaker V1.5 STEP7 CMC ENGINE

변경 내용:
- 도미넌스 수집을 CoinMarketCap 전용으로 변경
- 기존 CoinGecko fallback 제거
- CMD의 'CoinGecko(키없음)' 경고 제거
- 출력 기준을 '✅ CoinMarketCap'으로 정리
- .env.example에서 COINGECKO_API_KEY 항목 제거

필수 설정:
.env 파일에 아래 값을 넣어야 도미넌스가 정상 수집됩니다.
CMC_API_KEY=여기에_CoinMarketCap_API키

실행:
python app.py
