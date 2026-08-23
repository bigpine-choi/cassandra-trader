# Cassandra Trader

업비트 KRW 현물시장을 대상으로 하는 Long/Cash 기반 알고리즘 자동매매 시스템입니다.

현재 버전은 API 키 없이 업비트 공개 시장 데이터를 수집해 SQLite에 보존하고, canonical 1분봉과 완결 4시간봉을 조회하는 읽기 전용 개발 단계입니다. 실제 주문은 실행하지 않습니다.

## 프로젝트 목표

- 업비트 KRW 현물시장 내 투자 유니버스 구성
- 완결된 4시간봉 기반 알파·레짐·포트폴리오 의사결정
- 시장 위험 변화에 따른 암호자산 노출 조절
- 페이퍼 트레이딩부터 소액 실거래까지 일관된 검증 경로 구축
- 클라우드 서버에서 24시간 안정적으로 운영 가능한 시스템 구축

## 현재 구현 상태

- [x] WSL2 Ubuntu 및 Python 3.12 개발환경
- [x] `uv` 기반 패키지·가상환경 관리
- [x] Ruff 코드 검사 및 Pytest 테스트
- [x] 공식 Upbit Python SDK 연동
- [x] 업비트 KRW 거래 페어 조회
- [x] 업비트 응답과 내부 도메인 모델 분리
- [x] REST 1분봉 raw 응답과 immutable canonical revision의 SQLite 저장
- [x] 단일 마켓 구간 백필과 `as_of` canonical 조회
- [x] 명시적 복수 KRW 마켓의 순차 백필, 종목별 coverage와 JSONL 결과
- [x] bounded backfill 실행 증거와 point-in-time 1분 slot 상태 판정
- [x] point-in-time resolved minute slot 기반 UTC 4시간봉 파생
- [x] 공개 WebSocket ticker·1분봉 수신, 재연결과 REST reconciliation
- [ ] point-in-time 투자 유니버스
- [ ] 백테스트 및 페이퍼 트레이딩
- [ ] 포트폴리오·리스크 엔진
- [ ] 주문·체결·대사 시스템
- [ ] 실거래 운영

## 핵심 설계 원칙

- 레버리지와 공매도를 사용하지 않습니다.
- 정상 투자 의사결정에는 완전히 종료된 4시간봉만 사용합니다.
- 위험 감시는 24시간 상시 수행하는 구조를 지향합니다.
- 실거래 전에 백테스트, 페이퍼 트레이딩, 소액 검증을 실시합니다.
- API 키와 인증정보를 Git에 저장하지 않습니다.
- 실거래 주문 경로에 LLM을 사용하지 않습니다.

## 설치

프로젝트 의존성을 설치하고 가상환경을 동기화합니다.

```bash
uv sync
```

## 실행

기본 실행:

```bash
uv run cassandra-trader
```

업비트 KRW 거래 페어 조회:

```bash
uv run cassandra-trader markets --limit 10
```

이 명령은 인증이 필요 없는 읽기 전용 공개 API만 사용하며 주문을 생성하지 않습니다.

여러 KRW 마켓의 동일한 UTC `[start, end)` 구간을 순차 백필:

```bash
uv run cassandra-trader backfill-dataset \
  KRW-BTC KRW-ETH KRW-XRP \
  --start 2026-08-22T08:00:00Z \
  --end 2026-08-22T16:00:00Z \
  --database candles.db
```

입력 마켓은 trim·대문자화 후 중복 제거·사전순 처리됩니다. 각 마켓의 백필 통계와 `[start, end)` canonical coverage를 JSON Lines로 출력하고, 마지막 줄에 전체 summary를 출력합니다. 일부 마켓이 실패해도 다음 마켓을 계속 처리하며 최종 종료 코드는 0이 아닙니다. `absent_slots`는 관측되지 않은 분 수만 나타내며 무거래 또는 장애를 판정하거나 분봉을 합성하지 않습니다.

분 단위 상태는 `as_of`까지 이용 가능한 증거를 canonical 우선으로 결합합니다. canonical candle이 있으면 `TRADED`, candle은 없지만 해당 분 전체를 덮는 성공한 bounded backfill 증거가 있으면 `NO_TRADE`, 둘 다 없으면 `UNRESOLVED`입니다. 실패한 backfill은 `NO_TRADE` 증거가 아니며 어떤 상태에서도 가격을 합성하거나 forward-fill하지 않습니다.

4시간봉은 정확히 240개의 point-in-time minute slot에서 파생합니다. 하나라도 `UNRESOLVED`이면 부분 OHLCV 없이 `INCOMPLETE`, 모두 `NO_TRADE`이면 관측이 완료된 `NO_TRADE`를 반환합니다. 그 밖에는 `COMPLETE`이며 실제 `TRADED` candle만 OHLCV에 사용합니다. `NO_TRADE` 분의 가격·거래량을 합성하거나 이전 가격으로 forward-fill하지 않습니다. 기존 `actual_count`는 실제 `TRADED` candle 수, `missing_minute_starts`는 `UNRESOLVED` 분 시작 목록을 뜻합니다.

단일 마켓 백필과 완결 4시간봉 조회:

```bash
uv run cassandra-trader backfill-candles \
  KRW-BTC \
  --start 2026-08-22T08:00:00Z \
  --end 2026-08-22T16:00:00Z \
  --database candles.db

uv run cassandra-trader four-hour-bar \
  KRW-BTC \
  --start 2026-08-22T08:00:00Z \
  --as-of 2026-08-22T16:10:00Z \
  --database candles.db
```

## 코드 품질 검사

```bash
uv run ruff format --check src tests
uv run ruff check .
uv run pytest -q
git diff --check
```

## 설계 문서

전체 아키텍처, 전략 기준선과 연구 증거 규약은 다음 문서에서 관리합니다.

- [Cassandra Blueprint v1.0](docs/CASSANDRA_BLUEPRINT_v1.0.md)
- [Cassandra Research Protocol v1.0](docs/CASSANDRA_RESEARCH_PROTOCOL_v1.0.md)

## 보안

- `.env`와 `secrets/`는 Git 추적에서 제외합니다.
- API 키를 코드, 문서, 로그, 테스트 데이터에 입력하지 않습니다.
- 향후 운영 키에는 조회·주문 권한만 부여하고 출금 권한은 부여하지 않습니다.

## 주의사항

현재 버전은 시장 데이터 조회 기능만 구현된 개발 버전입니다. 실제 주문, 자산 운용 또는 수익 창출 기능은 아직 구현되지 않았습니다.
