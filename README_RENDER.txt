CryptoBreaker Render Ready FIX3

Start Command:
gunicorn app:app --worker-class gthread --threads 8 --timeout 120 --keep-alive 5 --bind 0.0.0.0:$PORT

FIX:
- Gunicorn에서도 백그라운드 데이터 수집 스레드 자동 시작
- /api/cbk/* 호환 라우트 추가
- assets/backgrounds, assets/brand 추가
- requirements.txt gunicorn 포함
