# Cassandra Trader

업비트 KRW 현물시장을 대상으로 하는 Long/Cash 기반 알고리즘 자동매매 시스템입니다.

현재 버전은 API 키 없이 업비트 공개 시장 데이터를 수집해 SQLite에 보존하고, point-in-time 마켓 목록·catalog eligibility·30일 유동성 순위·liquidity top-N shortlist·resumable bootstrap·incremental refresh·canonical 1분봉·완결 4시간봉과 bounded 공개 체결·호가 recorder를 제공하는 읽기 전용 개발 단계입니다. 실제 주문은 실행하지 않습니다.

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
- [x] append-only complete snapshot 기반 point-in-time 마켓 유니버스
- [x] point-in-time snapshot freshness와 warning 기반 catalog eligibility gate
- [x] 완전한 minute evidence 기반 trailing 30-day KRW quote-volume metric과 rank
- [x] 완결된 point-in-time liquidity rank 기반 versioned top-N shortlist
- [x] fresh non-warning analysis universe 전체의 resumable 30일 minute dataset bootstrap
- [x] current catalog capture와 successful evidence complement 기반 incremental run-once refresh
- [x] 고정된 point-in-time liquidity top-N의 bounded 공개 trade·orderbook append-only recorder
- [x] 읽기 전용 연구 데이터 준비상태 진단과 공통 cutoff의 탐색용 4시간봉 데이터셋 내보내기
- [ ] authoritative 상장 이력·상장 후 90일을 결합한 최종 투자 후보 정책
- [ ] 백테스트 및 페이퍼 트레이딩
- [ ] 포트폴리오·리스크 엔진
- [ ] 주문·체결·대사 시스템
- [ ] 실거래 운영

## 다음 개발 로드맵

2026-09-10 가격 동인 연구 계획을 개발 로드맵에 편입했습니다. 아래 항목은 구현 예정이며,
현재 읽기 전용 수집 기능과 구분합니다. 기존 데이터 품질·상장 이력 과제를 먼저 충족한 뒤
완결 4시간봉과 향후 7일 주 예측수평선을 기준으로 진행합니다.

1. **RD1 연구 데이터 계약:** 당시 유니버스·정보 가용시각·revision을 보존한 snapshot,
   실행 가능한 label, dataset manifest와 시간순 검증 골격을 만듭니다.
2. **RD2 기본 알파:** 가격·거래량·시장 상태의 단순 기준선부터 검증하고,
   Elastic Net·LightGBM/LambdaRank의 증분 가치를 비교합니다.
3. **RD3 미시구조:** 체결·호가 조회·재생 API를 추가하고 비용·유동성·하방위험을 검증합니다.
4. **RD4 외부 동인 Challenger:** 거시·파생시장·네트워크·공급·뉴스·국내외 괴리를
   시점 계약 확보 후 변수군별로 시험합니다. 기본 모델의 진행을 막는 필수 입력은 아닙니다.
5. **RD5 배분·운영 연결:** 검증된 기대수익·위험·비용을 총노출과 C′에 연결한 뒤
   기존 paper → shadow → micro live 게이트를 따릅니다.

가격 설명, 미래 예측, 비용 후 투자성과를 각각 검증합니다. 외부 파생시장 데이터 연구는
선물·숏 실거래 전환을 의미하지 않으며, LLM은 실거래 주문 경로에 사용하지 않습니다.
단계별 의존성과 완료 기준은 [Blueprint 15장](docs/CASSANDRA_BLUEPRINT_v1.0.md#15-구현-로드맵과-완료-기준),
가설과 반증 기준은 [Research Protocol 2.4절](docs/CASSANDRA_RESEARCH_PROTOCOL_v1.0.md#24-가격-동인-연구-가설)을 따릅니다.

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

현재 KRW 마켓 catalog를 append-only snapshot으로 저장하고 과거 시점 기준으로 조회:

```bash
uv run cassandra-trader capture-universe --database candles.db

uv run cassandra-trader universe-as-of \
  --as-of 2026-08-23T12:00:00Z \
  --database candles.db

uv run cassandra-trader eligible-markets \
  --as-of 2026-08-23T12:00:00Z \
  --maximum-snapshot-age-seconds 3600 \
  --database candles.db

uv run cassandra-trader market-liquidity \
  --quote-currency KRW \
  --window-end 2026-08-23T12:00:00Z \
  --as-of 2026-08-23T12:10:00Z \
  --maximum-snapshot-age-seconds 3600 \
  --database candles.db

uv run cassandra-trader liquidity-top-n \
  --quote-currency KRW \
  --window-end 2026-08-23T12:00:00Z \
  --as-of 2026-08-23T12:10:00Z \
  --maximum-snapshot-age-seconds 3600 \
  --top-n 100 \
  --database candles.db

uv run cassandra-trader bootstrap-liquidity-dataset \
  --quote-currency KRW \
  --universe-as-of 2026-08-23T12:10:00Z \
  --window-end 2026-08-23T12:00:00Z \
  --maximum-snapshot-age-seconds 3600 \
  --database candles.db

uv run cassandra-trader refresh-liquidity-dataset \
  --quote-currency KRW \
  --maximum-snapshot-age-seconds 3600 \
  --database candles.db

uv run cassandra-trader record-microstructure \
  --quote-currency KRW \
  --window-end 2026-08-24T12:00:00Z \
  --as-of 2026-08-24T12:10:00Z \
  --maximum-snapshot-age-seconds 3600 \
  --top-n 100 \
  --duration-seconds 60 \
  --orderbook-units 15 \
  --database candles.db
```

point-in-time universe query는 `available_at <= as_of`인 complete 성공 snapshot만 사용합니다. catalog snapshot 계층은 당시 membership과 warning metadata를 보존하고, 별도 catalog eligibility 계층은 호출자가 명시한 maximum snapshot age와 warning 여부만 판정합니다. snapshot이 없거나 `as_of - observed_at`이 maximum age를 초과하면 전체 결과를 `UNRESOLVED`로 fail-closed하고, fresh snapshot의 warning market만 `EXCLUDED`합니다. `ELIGIBLE`은 최종 매수 대상이 아니라 후속 유동성 심사 전 후보입니다.

Universe는 용도별로 세 계층을 구분합니다. catalog universe는 Upbit KRW 전체 market이며 warning market도 snapshot·수집 대상에서 제거하지 않고 사실 데이터와 metadata를 보존합니다. analysis universe는 fresh catalog의 non-warning market 전체이며, 향후 feature·시장 폭·모멘텀·변동성·횡단면 분석은 liquidity top-N이 아니라 이 전체 집합을 사용합니다. execution candidate universe만 point-in-time 30일 유동성 top-N shortlist를 사용합니다.

trailing liquidity는 fresh catalog eligibility를 통과한 non-warning KRW market마다 정확한 UTC `[window_end - 30 days, window_end)`의 43,200개 minute slot을 `as_of` 기준으로 요약합니다. 실제 `TRADED` canonical candle의 `quote_volume`만 합산하고 `NO_TRADE`는 합계에 기여하지 않습니다. 한 slot이라도 `UNRESOLVED`이면 해당 metric의 volume을 공개하지 않으며, 후보 하나라도 unresolved이면 전체 rank를 만들지 않습니다. synthetic candle, forward-fill과 partial-volume ranking은 하지 않습니다.

`liquidity-top-n`은 완결된 upstream liquidity ordinal rank에서 호출자가 명시한 N까지만 `SELECTED`로 만드는 versioned read-only policy입니다. 초기 운영 목표는 `top_n=100`이며 domain이나 application에 하드코딩하지 않고 CLI와 향후 실행 설정에서 명시적으로 전달합니다. 동률도 기존 market-code tie-break rank를 그대로 사용하며 N 밖 종목을 추가하지 않습니다. upstream liquidity가 하나라도 unresolved이면 모든 존재 후보를 `UNRESOLVED`로 두고 partial selection을 만들지 않습니다. top 100도 최종 투자 유니버스나 매수 신호가 아닙니다. 향후 execution candidate 중 실제 보유 종목 수는 고정하지 않고 비용 차감 엣지·불확실성·유동성·포트폴리오 분산효과로 결정할 예정이지만 signal, portfolio, execution은 아직 구현되지 않았습니다.

`bootstrap-liquidity-dataset`은 명시한 fresh point-in-time catalog의 non-warning analysis universe 전체에 정확한 UTC 30일·43,200분 범위를 순차 백필합니다. 실행 시작의 `resume_as_of`로 기존 `MinuteSlotSummary`를 확인해 모든 slot이 이미 resolved된 market은 API 호출 없이 `SKIPPED_COMPLETE`로 건너뜁니다. incomplete market은 작은 gap만 최적화하지 않고 동일한 전체 범위를 다시 조회하며 기존 canonical revision과 successful evidence를 checkpoint로 재사용합니다. 한 market의 최종 실패는 `FAILED`로 남기고 다음 market을 계속 처리합니다. 보수적 rate limit과 timeout·connection·HTTP 429·5xx 한정 retry를 적용하고 synthetic candle이나 forward-fill은 만들지 않습니다.

`refresh-liquidity-dataset`은 run-once cycle마다 현재 KRW catalog를 새 snapshot으로 보존하고, source 관측 시점까지 완전히 종료된 마지막 UTC minute를 기준으로 rolling 30일 window를 정합니다. fresh non-warning market에서 기존 successful evidence가 덮지 않는 새 tail과 내부 gap만 시간순 bounded backfill하며, 전체 window가 canonical과 evidence로 이미 resolved됐으면 API 호출 없이 건너뜁니다. rolling window 밖의 과거 canonical과 evidence는 삭제하지 않습니다. bootstrap은 최초 전체 범위 구성과 안전 재개용이고 refresh는 이후 증분 갱신용입니다.

`record-microstructure`는 시작 전 `evaluate_liquidity_top_n_as_of()`를 한 번만 평가해 fresh·resolved execution shortlist를 세션 전체에 고정하고, 선택된 market만의 공개 realtime trade와 orderbook snapshot·realtime frame을 지정한 기간 동안 기록합니다. analysis universe 전체를 대상으로 확장하지 않으며, API key·계좌·주문·Private WebSocket을 사용하지 않습니다. raw frame과 exact Decimal 정규화 event를 동일 transaction에 append하고 재연결마다 새 ticket으로 같은 고정 selection을 재구독하지만, capture 시작 이전 history나 disconnect 구간을 REST·synthetic event로 복원하지 않고 orderbook 완전성을 추측하지 않습니다. 아직 24/7 recorder, retention, compression, cost model 또는 signal·portfolio·execution 연결은 아니며, 실제 live smoke 후 events/sec와 SQLite MB/min을 측정해 장기 보존 정책을 정해야 합니다.

현재 `/v1/market/all` 응답을 과거 상장 상태로 소급하지 않습니다. 최초 성공 snapshot 이전은 `UNRESOLVED`이며 current listing이나 candle history로 listing age를 합성하지 않습니다. 상장 후 90일 정책, authoritative 과거 상장·폐지 이력 backfill과 정식 학습·검증 파이프라인은 아직 구현되지 않았습니다. 탐색용 연구 데이터셋 추출은 아래의 읽기 전용 명령으로 제공합니다. refresh 자동 scheduler도 없으므로 명령을 cron/systemd 없이 명시적으로 실행해야 하며 signal, portfolio, execution에는 연결되지 않습니다.

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

## 연구 데이터 준비상태와 탐색용 데이터셋

기존 SQLite DB에서 명시한 종목·기간을 읽습니다. API 호출, DB 생성·마이그레이션,
추가 수집은 수행하지 않습니다. 실제 DB에 저장된 기간과 맞게 날짜를 지정해야 합니다.

```bash
uv run cassandra-trader research-readiness KRW-BTC KRW-ETH \
  --start 2026-08-24T00:00:00Z --end 2026-08-25T00:00:00Z \
  --as-of 2026-08-25T00:10:00Z \
  --maximum-snapshot-age-seconds 3600 --database candles.db

uv run cassandra-trader export-research-dataset KRW-BTC KRW-ETH \
  --start 2026-08-24T00:00:00Z --end 2026-08-25T00:00:00Z \
  --as-of 2026-08-25T00:10:00Z \
  --maximum-snapshot-age-seconds 3600 --database candles.db \
  --output artifacts/research/20260825-v1
```

- `research-readiness`: 종목별 거래·무거래·미확인 분 수, 누락 구간, 관측 범위,
  마지막 가용시각, 4시간봉 완결 여부와 cutoff 시점 catalog 상태를 JSON으로 출력합니다.
- `export-research-dataset`: `bars.jsonl`, `lineage.jsonl`, `readiness.json`,
  `manifest.json`을 새 디렉터리에 저장합니다. 기존 출력은 덮어쓰지 않습니다.
- 시작·종료는 UTC 4시간 경계, 범위는 `[start, end)`, `as_of >= end`여야 합니다.
- 종료 코드: **0** 요청 종목의 구간 완결 및 catalog 적격, **2** 데이터 미완결 또는
  catalog 미적격, **1** DB·파일·실행 오류. 잘못된 인자도 argparse 규칙에 따라 2입니다.
  데이터나 catalog가 미완결이어도 진단용 export는 보존됩니다.
- `data_complete`는 분 단위 증거가 모두 있다는 뜻이며, 무거래도 포함합니다.
  가격 관측·학습 표본의 충분함 또는 매매 가능성을 의미하지 않습니다.
- 명시한 종목만 진단합니다. 전체 analysis universe나 유동성 top-N의 준비 완료를
  선언하거나 일부 종목만으로 유동성 순위를 생성하지 않습니다.
- 모든 출력은 **`NON_PIT_EXPLORATORY`**, **`confirmatory_ready=false`**입니다.
  공통 cutoff에서 알려진 과거 봉을 추출할 뿐 각 과거 의사결정 시점의 정보·상장 이력을
  복원하지 않습니다. 완전한 데이터여도 정식 OOS·lockbox·모델 승격 근거로 쓰지 않습니다.

구체적인 파일 계약과 제한은 [Research Dataset v1](docs/RESEARCH_DATASET_V1.md)을 참고합니다.

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

현재 버전은 시장 데이터 수집·조회와 탐색용 연구 데이터 추출 단계입니다. 실제 주문, 자산 운용 또는 수익 창출 기능은 아직 구현되지 않았습니다.
