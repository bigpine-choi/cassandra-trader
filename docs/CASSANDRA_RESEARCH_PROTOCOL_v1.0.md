# Cassandra Research Protocol v1.0

> 업비트 KRW 현물 Long/Cash 전략을 채택하기 전에 필요한 연구·검증 증거의 기준선

| 항목 | 내용 |
|---|---|
| 문서 상태 | 검토 초안 |
| 기준일 | 2026-08-22 |
| 대상 | Cassandra v1.0 연구, 백테스트 및 실거래 승격 |
| 상위 설계 문서 | [Cassandra Project Blueprint v1.0](CASSANDRA_BLUEPRINT_v1.0.md) |
| 시장 범위 | Upbit KRW 현물 · Long/Cash · 무레버리지 · 무공매도 |
| 핵심 원칙 | 좋은 과거 수익률보다 시점 무결성, 반증 가능성, 비용 현실성, 재현성을 먼저 입증 |

이 문서에서 `TBD`는 미결정 사항이다. `TBD`인 숫자나 규칙은 결과를 본 뒤 유리하게
선택하지 않으며, 13장의 확정 절차에 따라 마지막 관련 OOS 평가와 lockbox 열람 전에
버전이 있는 설정 또는 의사결정 기록으로 고정한다.

---

## 1. 목적과 적용 범위

### 1.1 Blueprint와 Research Protocol의 역할

| 문서 | 답하는 질문 | 주요 내용 |
|---|---|---|
| Blueprint | 무엇을 만들고 어떻게 운영할 것인가? | 시장 범위, 목표 아키텍처, 데이터·의사결정 주기, 위험·집행 원칙, 구현 로드맵 |
| Research Protocol | 어떤 증거가 있어야 연구 결과를 채택하고 다음 단계로 승격할 수 있는가? | 가설, 데이터 시점 계약, 비교군, 검증 분할, 비용, 지표, 과최적화 통제, 승격·강등 기준 |

Blueprint의 거래 범위와 안전 불변조건은 연구 결과로 우회할 수 없다. 연구 결과가
아키텍처나 운영 정책의 변경을 요구하면 먼저 별도의 검토와 Blueprint/ADR 변경을 거친다.
이 Protocol은 성과가 좋아 보이는 후보를 자동으로 실거래에 넣는 절차가 아니다.

### 1.2 연구 결과와 실거래 승격의 분리

- 연구 결과는 특정 데이터·코드·설정 아래의 반증 가능한 관측이며 수익성 보장이 아니다.
- 통계적으로 유의해 보이는 결과만으로 paper, shadow 또는 live 승격을 승인하지 않는다.
- 데이터 무결성, 실행 현실성, 위험 통제, 복구·대사, 운영 관측성을 별도의 증거로 요구한다.
- 연구자는 후보와 근거를 제시하고, Champion 승격은 shadow 증거를 포함한 사람의 승인으로만
  이루어진다.
- 자동 재학습은 Challenger 산출까지만 허용하며 운영 Champion의 자동 교체는 금지한다.

### 1.3 적용 범위와 현재 구현 경계

본 Protocol의 전략 범위는 Upbit KRW 현물의 Long/Cash다. 레버리지, 공매도, 파생상품,
강화학습 기반 주문 의사결정, LLM 기반 실거래 주문 판단은 초기 연구 범위에서 제외한다.
LLM은 문서·코드·연구 보조에 사용할 수 있지만 주문 경로의 입력이나 승인 주체가 될 수 없다.

현재 저장소에 구현된 것은 Python 3.12와 `uv` 기반 패키지, 공식 Upbit SDK를 통한 인증 없는
KRW 마켓 카탈로그 조회, SDK 응답과 내부 `TradingPair` 모델의 분리, CLI, 단위 테스트 4개,
GitHub Actions의 기본 품질 검사다. canonical 1분봉, 파생 4시간봉, PIT 유니버스, 특징,
모델, 백테스트, 계좌·원장, 주문, paper/shadow/live는 아직 구현되지 않았다. 따라서 이후
장들의 연구·운영 구성요소는 현재 기능 설명이 아니라 구현 후 충족해야 할 계약이다.
클라우드/VPS의 24시간 이벤트 기반 실행도 현재 기능이 아니라 검증·승격을 거쳐야 할 목표
운영 계약이다.

---

## 2. 연구 가설과 예측 목표

### 2.1 예측 단위와 출력

모든 정상 예측은 canonical 1분봉으로부터 생성되어 완전히 닫히고 품질 검사를 통과한
4시간봉마다 갱신한다. 논리적 UTC 봉 경계인 `bar_end`와 실제 final·품질 확인 뒤 정한 공통
정보 cutoff인 `as_of`를 분리한다. 4시간은 정보 갱신·정상 의사결정 주기이지 보유기간이
아니다. 모든 종목은 같은 전역 UTC `bar_end`와 그에 대응하는 `as_of`에서 평가한다.

| 목표 | 수평선 | 출력 | 주 평가 |
|---|---:|---|---|
| 시장 대비 초과수익 | 1일·3일·7일·14일 | 종목별 조건부 초과수익 분포와 기대값 | Rank IC, ICIR, 비용 차감 포트폴리오 기여 |
| 교차단면 순위 | 1일·3일·7일·14일 | 동일 `as_of` 적격 종목의 순위·점수 | Rank IC, Precision@K, 순위 안정성 |
| 양의 초과수익 확률 | 1일·3일·7일·14일 | 보정된 확률과 불확실성 | proper score, calibration, baseline 대비 skill |
| 하방위험 | 각 수평선과 포트폴리오 위험 창 | 손실 꼬리, 하방확률, CVaR 관련 추정 | calibration, tail ranking, 실현 초과 빈도 |

Blueprint에 따라 7일을 주 예측 수평선으로 두되 1일·3일·14일 결과를 숨기거나 사후에
선택하지 않는다. “약 7일”은 포트폴리오의 목표 평균 보유기간과 회전율 기준이지 개별
포지션의 고정·최소·최대 보유기간이 아니다. 위험이 발생하거나 신호가 소멸하면 7일 전에도
매도할 수 있고, 신호가 지속되면 7일보다 오래 보유할 수 있다.

초과수익의 시장 프록시, label 시작·종료 가격, 실행 지연은 `TBD-R01`, `TBD-R02`로
확정한다. 어느 정의도 같은 봉의 종가를 정보와 체결가격으로 동시에 사용해서는 안 된다.

### 2.2 사전 등록 가설과 반증 조건

각 실험은 아래 가설 중 하나 이상의 `hypothesis_id`를 갖고, 방향, 주 수평선, 비교 기준,
평가 지표, 비용 시나리오, 표본, 후보군과 반증 규칙을 실행 전에 등록한다.

| ID | 가설 | 필요한 OOS 증거 | 반증 또는 채택 실패 조건 |
|---|---|---|---|
| H-RANK | PIT 정보로 미래 초과수익의 교차단면 순위를 예측할 수 있다. | 전역 시간축 rolling-origin에서 양의 Rank IC 분포, 기준선 대비 증분, lockbox 재현 | 사전 지정 집계 Rank IC의 block-bootstrap 신뢰구간 상단이 0 이하이면 방향 가설을 반증한다. 0을 걸치면 미결정이며 채택 근거가 아니다. |
| H-PROB | 양의 초과수익 확률을 단순 발생률보다 유용하게 추정할 수 있다. | 발생률/단순모델 대비 proper-score skill과 OOS calibration | baseline 대비 score 개선의 신뢰구간 상단이 0 이하이거나 calibration이 사전 기준을 충족하지 못하면 반증 또는 채택 실패다. calibration 기준은 `TBD-R03`이다. |
| H-DOWN | 하방위험 모델이 단순 역사·EWMA 기준보다 미래 꼬리손실을 잘 구분한다. | exceedance calibration, tail ranking, 레짐·유동성별 안정성 | 기준선 대비 하방위험 skill의 신뢰구간 상단이 0 이하이거나 위험을 지속적으로 과소 추정하면 반증 또는 위험모델 채택 실패다. |
| H-NET | 예측을 Long/Cash 포트폴리오로 변환하면 현실적 비용 후에도 사전 지정한 Calmar 적격 위험 기준선보다 경제적 증분이 있고 cash 절대 hurdle을 넘는다. | 모든 현금 비용과 simulated/executed 노출 경로를 반영한 chronological OOS, outer-fold Net Calmar 중앙값의 paired 차이, cash 대비 순수익 경제효과와 lockbox 비교 | paired Net Calmar 차이의 신뢰구간 상단이 0 이하이면 방향 가설을 반증한다. cash hurdle 또는 `TBD-V08`의 경제효과를 넘지 못하거나 성과가 거래 불가능·비유동 종목, 미래정보 또는 비용 누락에 의존하면 채택에 실패한다. |
| H-ROBUST | 관측된 효과가 한 fold·레짐·유동성 꼬리·소수 종목에만 의존하지 않는다. | fold·레짐·유동성·상장연령 하위표본과 영향도 분석 | 사전 정의한 안정성 조건을 통과하지 못하면 전체 평균이 양수여도 승격하지 않는다. 안정성 조건은 `TBD-V09`다. |

proper-score skill은 `baseline loss - candidate loss`로 정의해 클수록 좋게 맞춘다. 하방위험의
과소추정과 calibration 실패 규칙은 `TBD-R03`에 수치·방향을 함께 고정한다. H-NET 비교 기준은
첫 outer 결과 전에 동결하거나, 모든 적격 기준선과 paired 비교하고 사전 지정한 다중비교
통제를 적용한다.

공통 판정은 paired skill의 신뢰구간 상단이 0 이하이면 반증, 하단이 사전 지정 최소
경제효과를 넘으면 잠정 지지, 그 사이는 미결정이다. 방향만 검정하는 H-RANK와 calibration
가설에는 그 목적에 맞춘 사전 규칙을 쓴다. 7일 primary horizon·primary metric 외의
1일·3일·14일과 보조 지표는 탐색 결과로 분리하거나 `TBD-O01`의 multiplicity 통제를 적용한다.

### 2.3 결과 판정 언어

- **반증**: 사전 반증 조건을 충족했다. 같은 가설을 다시 시험하려면 변경 이유와 새 가설을
  등록하고 이전 실패를 보존한다.
- **미결정**: 신뢰구간이 양·음 방향을 모두 허용하거나 관측 길이가 부족하다. 성공으로 세지
  않는다.
- **잠정 지지**: 개발 및 chronological OOS에서 사전 조건을 충족했다. lockbox와 다음 승격
  단계의 증거 없이는 실거래 채택을 뜻하지 않는다.
- **승격 가능**: 12장의 현재 단계 게이트를 모두 충족했다. 실제 승격은 별도 사람 승인을
  요구한다.

---

## 3. 데이터 및 시점 계약

### 3.1 필수 시각과 버전 필드

| 필드 | 정의 | 금지되는 대체 |
|---|---|---|
| `event_time` | 거래, 호가, 봉 구성 이벤트 또는 시장상태가 실제로 발생한 거래소 시각 | 수신시각으로 덮어쓰기 |
| `source_available_at` | 출처가 레코드나 revision을 공개한 것으로 직접 관측·입증되는 시각 | 수신·적재시각 또는 추정값으로 대체 |
| `received_at` | Cassandra 수집기가 해당 원응답 또는 revision을 처음 수신한 시각 | `source_available_at` 또는 재처리 시각으로 덮어쓰기 |
| `ingested_at` | 원응답이 append-only 저장소에 영속화된 시각 | `received_at`으로 대체 |
| `available_at` | live-captured 데이터가 Cassandra 검증 경로에서 실제로 이용 가능해진 시각. Blueprint의 `available_time`과 같은 개념이다. | `event_time` 또는 모델 추정시각을 실제값처럼 기록 |
| `modeled_available_at` | reconstructed PIT에 versioned availability model과 보수적 지연을 적용한 의사결정 가능시각 | actual PIT의 `available_at`으로 가장 |
| `provenance` | 출처·endpoint·요청/연결·snapshot·수집 방식과 availability 근거 | 출처 없는 최종값만 보존 |
| `bar_end` | canonical 1분봉·4시간봉의 논리적 UTC 구간 종료시각 | 실제 가용·결정시각으로 간주 |
| `as_of` | 한 의사결정에서 특징·유니버스·예측이 볼 수 있는 정보의 공통 cutoff | 실행 시각의 최신 final 데이터로 과거를 재구성 |
| `decision_at` | 해당 `as_of` snapshot의 품질 확인 뒤 모델·포트폴리오 계산을 시작한 실제 시각 | `bar_end`로 소급 기록 |
| `revision` | 같은 논리 레코드의 단조 증가 버전과 변경 사유 | 기존 값을 제자리 덮어쓰기 |
| `label_start`·`label_end` | 실행 가능한 label 가격구간의 시작·종료시각 | 단순 행 수 또는 봉 개수로 대체 |
| `label_available_at` | actual PIT label 결과가 학습에 처음 이용 가능해진 실제 시각 | `label_end`와 항상 같다고 가정 |
| `modeled_label_available_at` | reconstructed PIT label에 availability model과 계산 지연을 적용한 학습 가능시각 | actual `label_available_at`으로 가장 |

원천, 요청/연결 ID, 스키마 버전, 품질 상태와 raw hash도 함께 보존한다. 특징에는 실제
lookback의 `feature_dependency_start`와 lineage를 둔다. actual PIT 입력은
`input.available_at <= as_of`, reconstructed PIT 입력은
`input.modeled_available_at <= as_of`를 만족해야 하며 둘 다 `as_of <= decision_at`이어야 한다.
actual PIT 학습 label은 `label_available_at <= training_cutoff`, reconstructed PIT 학습
label은 `modeled_label_available_at <= training_cutoff`일 때만 사용할 수 있다. 봉이 늦게
final되면 `decision_at`을 늦추거나 해당 정상 결정을 건너뛰며 `bar_end`로 시간을 되돌리지
않는다.

역사 데이터 증거 등급은 다음과 같이 고정한다.

| 등급 | 허용 용도 | 제한 |
|---|---|---|
| **actual PIT live-captured** | 예측·백테스트·shadow 검증과 실제 latency·revision·TCA 추정 | 실제 `received_at`, `ingested_at`, `available_at`과 provenance를 보존한다. `source_available_at`은 출처가 제공할 때 실제값으로, 없으면 `UNAVAILABLE`로 기록한다. |
| **reconstructed PIT** | explicit availability model과 보수적 지연을 적용한 역사 OHLCV 백테스트 | model version·가정·`modeled_available_at`·`modeled_label_available_at`을 기록한다. source value의 불변성을 검증하거나 당시 revision을 재구성할 수 있어야 하며, 과거 latency·revision·TCA의 실측 증거로 사용하지 않는다. |
| **non-PIT exploratory** | 특징·가설 탐색과 데이터 가능성 조사 | confirmatory OOS, lockbox 또는 승격 증거로 사용하지 않는다. |

과거 REST backfill을 오늘 수신했다면 `received_at`과 `ingested_at`은 오늘이며 이를 과거 시각으로
소급하지 않는다. 출처의 과거 공개시각을 직접 입증할 수 없으면 actual
`source_available_at`을 채우지 않는다. 다만 versioned explicit availability model과 보수적
지연을 적용한 역사 OHLCV는 reconstructed PIT로 백테스트에 사용할 수 있다. 과거 latency,
revision 도착분포와 TCA에 관한 주장은 actual PIT live-captured 자료로 제한한다. 구체적인
availability model과 민감도는 `TBD-D04`로 고정한다. 보수적 지연만으로 revision PIT가
확보되는 것은 아니다. 불변성 또는 당시 revision을 입증하지 못한 값과 그 파생물은
non-PIT exploratory로 강등한다.

### 3.2 revision 생명주기

실시간 WebSocket 경로에서 canonical 시장 데이터의 정상 상태 전이는 다음과 같다. 역사 REST
원천은 WebSocket provisional을 가장하지 않고 source 상태를 보존한 reconciled 후보에서
시작한다.

1. **WebSocket provisional**: 실시간 원이벤트를 append-only로 저장하고 멱등 키로 중복을
   식별한다. 아직 REST 기준 확정값으로 간주하지 않는다.
2. **REST reconciled**: 누락 의심·재접속·정기 복구 시 REST 결과와 비교해 동일성, gap,
   revision 원인을 기록한다.
3. **final**: `TBD-D01`의 late-event 창과 무결성 검사를 통과한 revision을 연구용 final로
   표시한다.

final 이후 정정도 삭제나 덮어쓰기가 아니라 새 `revision`으로 추가한다. 과거 의사결정
재생은 “현재 가장 최신 final”이 아니라 해당 `as_of`까지 실제로 이용 가능했던 revision을
조회한다. WebSocket 이벤트만을 시장이나 계좌의 최종 진실로 간주하지 않는다.

### 3.3 canonical 1분봉과 완결 4시간봉

- ticker와 1분봉은 24시간 이벤트 기반 감시 데이터다.
- canonical 1분봉이 기본 시계열이며, raw REST/WebSocket을 동일한 버전 스키마로
  정규화한다.
- 완결 4시간봉은 완결·품질 판정된 canonical 1분봉을 UTC 경계로 집계한 파생 데이터다.
- REST 240분봉을 유일한 원천으로 받아 저장하는 방식은 연구 기준 경로가 아니다.
- 체결·호가는 전 종목을 무조건 상시 저장하지 않고 보유 종목, 상위 매수 후보와 현재 주문
  대상을 중심으로 수집한다.
- 미완결 4시간봉은 정상 알파·레짐·포트폴리오 결정에 사용할 수 없다.
- 위험·주문·체결·잔고 감시는 4시간 결정을 기다리지 않고 실시간 이벤트 경로에서 계속된다.

### 3.4 `NO_TRADE`와 `MISSING`

| 상태 | 의미 | 허용 처리 |
|---|---|---|
| `NO_TRADE` | 시장이 열려 있었고 수집 경로도 정상이며, 해당 구간에 체결이 없었다는 근거가 있다. | 명시적 품질 상태로 보존한다. OHLC carry와 거래량 0 구성 규칙은 `TBD-D02`로 확정한다. |
| `MISSING` | 이벤트가 없었던 것인지 수집·전송·저장 실패인지 확정할 수 없다. | gap으로 보존하고 REST 복구를 시도한다. 조용히 0거래 또는 forward-fill로 바꾸지 않는다. |

`MISSING`이 남은 1분봉을 포함하는 4시간봉의 생성·사용·격리 정책은 `TBD-D03`으로 정한다.
그 결정 전에는 정상 학습과 의사결정에서 제외하는 것이 기본 안전 동작이다.

### 3.5 point-in-time 유니버스

각 종목에 대해 상장, 상장폐지, 거래 유의, 거래정지, 재개와 시장 코드 변경을 효력시각과
증거 등급에 맞는 `available_at` 또는 `modeled_available_at`이 있는 이벤트로 저장한다.
유니버스는 일 1회 PIT snapshot으로 갱신하고, 각 4시간 `as_of`는 그때 유효한 최신 snapshot만
사용한다. snapshot은 그때까지 알려진 상태와 거래대금·유동성만으로 재구성한다.

- catalog universe는 Upbit KRW 전체 market이며 warning market의 사실 데이터와 metadata도
  snapshot과 수집 범위에 보존한다.
- analysis universe는 fresh catalog의 non-warning market 전체다. feature, 시장 폭, 모멘텀,
  변동성, 횡단면 분석은 liquidity top-N이 아니라 이 전체 집합을 사용한다.
- 실제 신호 평가와 주문 가능 후보를 위한 execution candidate universe만 point-in-time 30일
  유동성 top-N shortlist를 사용한다. 초기 운영 목표 `top_n=100`은 코드 상수가 아니라 실행
  설정과 manifest에 명시한다.
- execution candidate 중 최대 6개 보유를 목표로 하지만 signal·portfolio·execution과 상장 후
  90일·authoritative listing history 정책은 아직 구현되지 않았다.
- 최종 생존 종목 목록으로 과거를 필터링하지 않는다.
- 상장폐지·유의·정지 종목과 실패한 데이터 구간을 역사에서 제거하지 않는다.
- BTC·ETH를 포함한 fresh non-warning market 전체를 analysis universe에 유지한다. 향후 별도
  execution 제외 정책이 필요하면 analysis universe를 축소하지 않고 versioned policy로 분리한다.
- 오늘 수정된 종목 메타데이터를 과거 snapshot에 소급 적용하지 않는다.
- cross-coin, breadth, BTC/ETH 상태도 같은 `as_of` 유니버스와 revision만 사용한다.
- 외부 지수나 데이터가 당시 이용 가능했음을 입증할 수 없으면 학습 입력이 아니라 사후
  진단으로만 사용한다.

30일 lookback과 상장 90일 기준은 서로 다른 정책 축이다. execution shortlist의 초기
`top_n=100`은 각 실행에서 명시하고 domain이나 application에 하드코딩하지 않는다. 설정 변경은
manifest와 versioned policy/config 근거로 추적한다. 실제 구성은 동률, 결측·불완전 데이터
규칙으로 결정론적으로 정하고 이 설정 주변의 민감도는 `TBD-U01`로 기록한다. 후보가 부족하면
shortlist를 억지로 채우지 않는다.

### 3.6 재현성 데이터 manifest

모든 연구 실행은 raw/canonical/derived snapshot ID, 스키마 버전, 시각 범위, row count,
품질 상태별 count, source hash, revision cutoff, PIT 유니버스 hash, 증거 등급, provenance와
availability model version을 manifest로 남긴다.
파생 artifact와 연구 결과는 구성 입력 가운데 가장 약한 증거 등급을 상속한다.
동일 manifest와 코드·설정으로 canonical 1분봉, 완결 4시간봉, 특징과 label을 다시 만들었을
때 bitwise 또는 사전 정의한 수치 허용오차 안에서 동일해야 한다.

---

## 4. 특징군

특징은 경제적 가설이 있는 군으로 등록하며, 개별 변형을 시험한 횟수도 실험 시행 수에
포함한다. 모든 rolling statistic은 현재 `as_of`에서 이용 가능한 관측만 사용하고, 전처리
파라미터는 각 training fold에서만 적합한다.

| 특징군 | 후보 정보 | 주요 누출·과적합 통제 |
|---|---|---|
| 다중 기간 가격 | 1분·4시간 기반 모멘텀, 단기 반전, 상대강도, drawdown, 경로 형태 | 미완결 봉 금지, 수평선별 독립 가설, 가격 revision 고정 |
| 거래량·거래대금 | 수준, 변화율, surprise, 매수·매도 불균형 후보 | `NO_TRADE`/`MISSING` 분리, 당시 유니버스 내 횡단면 정규화 |
| 변동성 | realized/EWMA 변동성, range, 변동성 변화, jump proxy | 미래 창 사용 금지, 레짐별 calibration |
| 하방위험 | downside deviation, drawdown 속도, tail loss, downside semivariance | tail 정의와 창을 사전 등록, 희소 사건의 불확실성 보고 |
| 유동성 | 거래대금, 회전율, Amihud류 proxy, spread·depth·impact 관측 | candle proxy를 실제 실행비용으로 간주하지 않음, 선택적 호가·체결로 검증 |
| 시장 breadth | 상승 비율, 신고가·신저가, 분산, 횡단면 dispersion | 당시 적격·관측 가능 종목만 사용, 생존편향 차단 |
| BTC/ETH 상태 | 추세, 변동성, tail, 시장 대비 상대상태 | 거래대상 여부와 무관하게 동일 시점 정보만 사용 |
| cross-coin | 상대 모멘텀, 동조화, 선도·지연 후보, 상관·cluster | 모든 종목을 같은 전역 시간축에 정렬, 자산 간 train/test 혼합 금지 |

뉴스, 온체인, 거시·해외시장 데이터는 초기 핵심 특징이 아니다. source revision, 게시·수신
시각, 심볼 매핑, 결측과 이용권한을 포함한 PIT 계약을 먼저 통과한 뒤 **후속 Challenger**로만
평가한다. 사후 정제된 뉴스나 온체인 값을 과거 `as_of`에 소급해 넣지 않는다.

특징 채택에는 단독 성과가 아니라 ablation, fold 안정성, 레짐·유동성 하위표본, 비용 후
증분, drift 관측 가능성, 계산 지연과 lineage를 요구한다. 특징 수가 늘면 후보 시행 수와
multiple-testing 부담도 함께 늘어난다.

---

## 5. 모델 경쟁 구조

### 5.1 후보 계층

| 계층 | 후보 | 역할 |
|---|---|---|
| 무투자 기준선 | KRW cash | 위험을 감수하지 않은 기준. 모델의 명목수익만으로 채택하지 않게 한다. |
| 시장 기준선 | PIT 내부 시장 프록시 | 시장 베타와 종목선택의 기여를 분리한다. 정확한 프록시는 `TBD-R01`이다. |
| 포트폴리오 기준선 | PIT liquid universe 동일가중 | 복잡한 점수·최적화 없이 투자 가능한 대안과 비교한다. |
| 신호 기준선 | 단순 다중기간/7일 모멘텀 | 알려진 단순 신호 대비 복잡도의 증분을 측정한다. |
| 지정 v1 Alpha Champion | Elastic Net + LightGBM/LambdaRank | Blueprint가 확정한 v1 구성. 본 Protocol의 증거 기준을 통과해야만 승격할 수 있다. |
| 선형 비교군 | Ridge, Lasso | 해석 가능하고 규제가 강한 기준 후보 |
| 트리 Challenger | XGBoost | 비선형·상호작용 대안. 동일한 비용·trial budget 아래 평가한다. |
| 딥러닝 Challenger | TimesNet, TimeMixer, iTransformer | 일반 시계열 아키텍처의 이전 가능성을 시험하는 후보. 사전 Champion이 아니다. |
| 후속 Challenger | time-series foundation model, 온라인 적응 모델 | 데이터·연산·재현성·drift 계약이 준비된 뒤에만 평가한다. |

Blueprint가 확정한 v1 Alpha Champion과 Challenger의 권한을 본 Protocol이 바꾸지 않는다.
지정 v1 Champion이 증거 기준을 통과하지 못하면 다른 모델을 대신 승격하지 않고 **v1 승격
실패**로 기록한다. 다른 모델을 Champion으로 교체하려면 먼저 Blueprint 또는 ADR을 변경해야
한다. Challenger는 비교·shadow 평가 대상이며 이름이나 다른 데이터셋의 논문 성능만으로
Champion 권한을 얻지 않는다. 같은 권한 규칙은 Blueprint의 해석 가능한 연속 점수 기반 v1
Regime Champion에도 적용한다.

### 5.2 공정한 경쟁 조건

- 모든 후보는 같은 data manifest, PIT universe, development outer folds, label과 비용
  시나리오를 사용한다.
- v1 승격용 마지막 lockbox에는 development outer에서 지정 v1 Champion 구성 안에서
  선택·동결한 단 하나의 최종 pipeline과 사전 지정 baseline만 넣는다. Challenger 평가는
  별도 사전 등록하고, 그 결과로 v1 Champion을 대체하거나 추가 선택하지 않는다.
- 전처리, 특징 선택, 하이퍼파라미터, ensemble, calibration은 inner loop에서만 선택한다.
- 모델별 탐색공간, random seed 수, wall-clock/연산 예산과 총 시행 수를 사전 등록한다.
- 예측 정확도와 비용 차감 포트폴리오 성과를 모두 보고하며, 한쪽만 골라 발표하지 않는다.
- 복잡도에 따른 학습시간, 추론지연, 메모리, 실패율, 재현성, calibration 비용도 기록한다.
- foundation model은 pretraining corpus의 잠재 중복, 모델·토크나이저 버전, license와 artifact
  hash를 감사한다.
- 온라인 적응 모델도 운영 Champion을 직접 갱신하지 않는다. 적응 결과는 새 Challenger
  artifact로 동결한 뒤 동일한 승격 절차를 거친다.

강화학습과 LLM 주문 의사결정은 초기 모델 경쟁에서 제외한다. LLM 출력은 예측·비중·주문
파라미터 또는 위험 veto의 운영 입력이 될 수 없다.

---

## 6. 검증 프로토콜

### 6.1 동일한 전역 시간축

종목별 행을 무작위 분할하지 않는다. 같은 `as_of`의 일부 종목이 train에 있고 다른 종목이
validation/test에 있는 분할도 금지한다. breadth와 cross-coin 특징 때문에 한 종목의 같은
시점 정보가 다른 종목을 통해 누출될 수 있기 때문이다.

분할 단위는 전 종목의 동일한 UTC 4시간 `as_of` 묶음이다. 상장 전·정지·결측 종목은 행을
억지로 채우지 않고 그 시점의 PIT 상태로 표시한다.

### 6.2 Primary: chronological rolling-origin walk-forward

주 성능 추정은 과거로 학습하고 그 뒤의 연속 구간을 평가하는 rolling-origin outer fold다.
expanding/rolling training window, 최소 학습 길이, OOS 길이와 step은 `TBD-V01`로 첫 outer
결과 열람 전에 고정한다. outer OOS 구간은 원칙적으로 겹치지 않게 구성한다. 겹침이 필요한
경우 각 `as_of`에 평가할 단 하나의 frozen-origin 예측과 가중 규칙을 미리 정해 동일 시점을
두 번 집계하지 않는다. primary wealth path의 OOS `as_of`는 첫 평가 이후 끊김 없이 이어져야
하며, 비연속 OOS 설계는 primary Net Calmar 증거로 사용하지 않는다.

각 outer fold에서는 다음 순서를 지킨다.

1. training 시점에 이용 가능한 revision과 PIT 유니버스만 선택한다.
2. label interval이 outer validation/test와 겹치는 training 관측을 purge한다.
3. 필요한 경계에는 사전 정의한 embargo를 적용한다.
4. training 내부의 시간순 inner folds에서 전처리, 특징, 모델, 하이퍼파라미터, calibration,
   비용모델, no-trade band와 포트폴리오 파라미터를 선택한다.
5. 선택을 동결한 뒤 outer OOS 구간을 한 번 평가한다.
6. 다음 origin으로 이동하며 그 시점까지의 정보만 사용해 다시 적합한다.

outer 결과를 본 뒤 후보군이나 선택 규칙을 바꾸면 새 실험 계보로 등록하고 이미 본 outer
결과를 순수 OOS라고 부르지 않는다.

### 6.3 label interval purging과 embargo

각 표본은 행 번호가 아니라 `feature_dependency_start`, `as_of`, `decision_at`, `label_start`,
`label_end`, `label_available_at`을 갖는다. training label 구간이 validation/test label·정보
구간과 겹치면 해당 training 표본을 purge한다. 1일·3일·7일·14일 중 사용한 최대 수평선과
예상 실행 지연을 모두 고려한다.

embargo는 purge와 구분하며, 비시간순 조합 분할에서 미래측 training의 feature lookback과
잔여 serial dependence가 test 경계를 오염시키지 않도록 적용한다. 임의 비율이나 OOS 성과로
고르지 않고 최대 label 수명, 특징 정보수명, 실행 지연과 누출 진단으로 `TBD-V02`를 정해
여러 길이 민감도를 보고한다. 적용 뒤 실제 표본 수를 fold별로 공개한다.

### 6.4 Secondary: CPCV, CSCV와 PBO

보조 검증은 primary chronological 결과를 대체하지 않는다.

- **CPCV**: label-interval purging과 embargo를 적용한 조합 분할로 여러 backtest path의
  성과분포와 경로 민감도를 만든다. 일부 조합은 역사적 운용 순서를 그대로 모사하지 않으므로
  최종 배포 성능으로 해석하지 않고 primary OOS와 합쳐 하나의 수익 경로로 만들지 않는다.
- **CSCV**: 동일 관측축에서 사전 등록된 여러 후보의 성과행렬을 대칭적으로 나누어, IS에서
  선택한 승자가 OOS 순위에서 붕괴하는 정도로 PBO를 진단한다. 행렬에는 같은 기간·비용 정의의
  고정 configuration과 purging·embargo를 거친 OOS 예측만 넣고 성공 후보만 선별하지 않는다.
  성과 payoff가 CSCV IS/OOS 경계를 가로지르면 `TBD-V02`에 따라 경계 관측을 제외하고 수를
  기록한다.
- CPCV와 CSCV는 같은 방법이 아니다. CSCV 자체를 label purging 기법이라고 부르지 않는다.
- PBO는 후보 **선택 절차**의 과적합 위험 진단이지 개별 전략이 미래에 수익을 낼 확률이
  아니다. 실패 후보와 시행을 누락하면 PBO도 신뢰할 수 없다.
- v1 승격용 PBO 후보행렬은 지정 v1 Champion 선택 경로 안의 사전 등록 configuration으로
  제한한다. Challenger의 별도 실험은 v1 Champion 선택 후보처럼 섞지 않는다.

조합 수, group 수, path 구성, PBO 후보행렬과 산출 가능 최소 조건은 `TBD-V03`으로 사전
동결한다. purge 이후 표본·경로가 부족하거나 고정 후보 수가 PBO 추정에 충분하지 않으면
수치를 억지로 만들지 않고 각각 `not estimable`로 기록한다. 이는 0 또는 통과를 뜻하지 않는다.
필수 secondary 증거가 `not estimable`이면 backtest candidate 승격을 통과하지 못한다.
CPCV와 CSCV/PBO는 개발 기간에서만 수행하며 마지막 lockbox 관측을 path, 후보행렬 또는 PBO에
넣지 않는다.

### 6.5 Nested model selection

모델 종류뿐 아니라 특징군, lookback, `TBD-U01`의 buffer·tie·결측 처리, liquidity 규칙, scaling,
calibration, no-trade band, 비용·포트폴리오 파라미터까지 결과를 보고 고른 항목은 모두
selection 대상이다. 이 선택은 outer training 내부의 nested 절차로 제한한다. outer OOS와
lockbox는 선택에 사용하지 않는다. 7일 primary horizon, primary label과 primary 판정 지표는
첫 outer 결과 전에 고정한다. 다른 horizon·label은 별도 가설·trial이며, 모델 내부 보조
target만 inner selection 대상으로 둘 수 있다.

### 6.6 마지막 chronological lockbox

전체 연구 기간의 마지막 연속 구간을 lockbox로 격리한다. 기간, 데이터 revision cutoff,
평가 코드와 승인된 접근자는 `TBD-V04`로 사전 기록한다.

- 후보군, 선택 규칙, 비용모델과 acceptance rule을 동결한 후 한 번만 연다.
- 열람 시각, 실행자, artifact hash와 결과를 access log에 남긴다.
- 결과를 본 뒤 모델·특징·비용·threshold를 바꾸면 해당 lockbox는 소진된 것이며, 변경 모델은
  새 미래 구간 또는 별도 미열람 lockbox가 필요하다.
- CPCV/CSCV, DSR 또는 높은 평균 성과는 lockbox를 대체하지 않는다.

### 6.7 의사결정 가격과 체결가격

완결 4시간봉의 종가는 그 봉을 완결한 정보의 일부일 수 있으나 같은 시각의 체결가격으로도
사용할 수 없다. bar의 final/availability 확인 뒤 `decision_at`을 기록하고, simulated order는
그 이후의 첫 적격 시장 이벤트와 명시적 지연을 거쳐야 한다.

limit order는 이후 호가·체결 경로가 조건을 만족할 때만 fill되며 partial/unfilled 상태를
허용한다. 과거 candle만 있어 fill을 입증할 수 없으면 보수적 fill rule과 불확실성을
명시하고, 미시구조 검증 전에는 정밀 체결 성과라고 주장하지 않는다.

### 6.8 재현성 검증

- 같은 raw+revision cutoff로 canonical 1분봉과 완결 4시간봉을 재생해 동일성을 확인한다.
- late, duplicate, out-of-order, gap, `NO_TRADE`, `MISSING` fixture를 포함한다.
- 데이터 행 순서, 병렬 처리, 재시작에도 특징·예측·포트폴리오 결과가 동일해야 한다.
- 모든 fold 경계에서 label interval overlap이 0인지 기계적으로 검사한다.
- 동일 `experiment_id`를 깨끗한 환경에서 재실행해 manifest와 결과 hash를 비교한다.

---

## 7. 거래비용과 실행 가정

### 7.1 비용 구성요소

| 구성 | 연구 반영 방식 |
|---|---|
| fee | 당시 적용 가능한 거래소 수수료와 주문 유형을 versioned input으로 사용한다. 현재 수수료를 과거 전체에 무근거 소급하지 않는다. |
| half-spread | decision 이후의 arrival mid와 실행 가능한 bid/ask를 기준으로 방향별 비용을 추정한다. 호가가 없으면 검증된 보수적 proxy를 쓴다. |
| depth impact | 주문금액, 가시 depth, 참여율, 유동성 상태와 urgency에 조건부인 비선형 비용으로 추정한다. |
| delay drift | `decision_at`부터 거래소 도착까지 시장이 움직인 signed 비용을 별도로 측정한다. |
| partial fill | 실제 또는 시뮬레이션 fill 수량만 포지션에 반영하고 잔량의 재주문·취소 비용을 추적한다. |
| opportunity cost | 미체결·위험 veto·capacity 제한으로 놓친 목표 포지션의 반사실적 성과를 별도 TCA 진단으로 기록한다. 실제 NAV P&L에서 현금 비용으로 다시 차감하지 않는다. |

### 7.2 가격 시점

| 가격 | 정의 | 용도 |
|---|---|---|
| `decision_price` | 완결된 정보로 목표를 계산할 때의 참조가격. fill로 간주하지 않는다. | 신호·목표 포트폴리오 기준 |
| `arrival_mid` | 주문이 실행 경로에 도착했을 때 최우선 bid/ask의 중간값 | 의사결정 지연과 시장 충격 분리 |
| `fill_price` | 수수료 전 실제 또는 보수적으로 시뮬레이션된 체결가격 | P&L, realized spread, TCA |

비용 회계는 두 관점 중 하나를 일관되게 사용한다. decision-price 기준 회계는 gross P&L에서
implementation shortfall와 fee를 차감하고, fill-to-fill 회계는 실제 fill 경로 P&L에서 fee를
차감한다. `fill_price - decision_price`에 이미 들어간 delay·spread·impact를 다시 차감하지
않는다. 정확한 부호와 분해식은 `TBD-C01`로 고정한다.

### 7.3 상태의존 비용과 스트레스

비용은 종목, 주문 방향·크기, spread, depth, 변동성, 거래량, 레짐, 시간대, urgency와
데이터 신선도에 따라 달라진다. 전 기간 단일 bps를 적용한 결과만으로 승격하지 않는다.
의사결정과 no-trade band에는 `decision_at`까지 이용 가능한 ex-ante 비용 예측만 사용한다.
arrival mid와 fill은 ex-post TCA와 다음 비용모델 version의 재보정에만 쓰며 같은 주문 판단에
되먹이지 않는다.

기준 비용과 함께 2배 비용 스트레스를 반드시 보고한다. 무엇을 2배로 하는지 모호하지 않게
fee·spread·impact·delay별 stress와 총비용 2배 시나리오를 `TBD-C02`에 사전 등록한다. 2배
스트레스 통과 자체가 미래 비용이나 수익성을 보장하지 않는다.

### 7.4 cost-aware no-trade band

현재 포트폴리오에서 목표 포트폴리오로 이동할 순 기대개선이 예상 비용, 추정오차와 위험
buffer를 넘지 못하면 거래하지 않는다. band는 회전율을 사후에 예쁘게 만드는 조정변수가
아니며 inner selection과 `TBD-C03`의 안정성·비용오차 근거로 고정한다. 위험 veto와 emergency
감축은 이 band를 우회할 수 있다.

### 7.5 TCA

paper부터 모든 가상·실주문에 decision, arrival, submit, acknowledgement, fill/cancel 시각과
가격·수량을 기록한다. backtest/paper의 `predicted`, replay/shadow의
`simulated/counterfactual`, live의 `realized` TCA를 구분한다. 예상비용과 관측 가능한
implementation shortfall의 bias, MAE/분위수 오차, coverage를 종목·레짐·유동성·주문크기별로
보고한다. 비용모델 재보정도 새 version과 experiment로 남기며 과거 결과를 조용히
재작성하지 않는다.

---

## 8. 포트폴리오 목적함수

### 8.1 v1 강건 C′ 목적함수

포트폴리오 primary 목적함수는 Blueprint의 강건 C′를 그대로 상속한다. C′는 선택된
위험자산 슬리브의 상대비중 $x$를 다음과 같이 구한다.

$$
\max_x \left[
\tilde{\mu}^{\top}x
-\lambda\,\mathrm{CVaR}^{regime}_{7d}(x)
-\eta\,\mathrm{Cost/Turnover}(x)
-\gamma\lVert x-x_B\rVert^2
\right]
$$

$x_i \ge 0$, $\sum_i x_i=1$이며 이 합은 위험자산 슬리브 내부의 상대비중이다.
$\tilde{\mu}$는 축소·불확실성 조정된 비용 차감 기대 엣지이고, $x_B$는 신뢰도와
역변동성을 결합한 anchor/fallback이다. $x_B$와 동일가중 fallback은 현재 적격 집합에서
비음수·합계 1로 정규화한다. $\lambda$, $\eta$, $\gamma$, shrinkage, CVaR와 비용 회계의
calibration은 `TBD-P01`로 정한다.

C′의 `Cost/Turnover(x)`는 현재 위험자산 슬리브 상대비중에서 $x$로 이동하는 마찰을 뜻한다.
각 비용 구성요소는 $\tilde{\mu}$ 또는 이 항 중 한 곳에만 배정한다. 실제 계좌 비용과 H-NET은
$\Delta w = EffectiveExposure_{new}x-w_{prev}$의 전환량을 7장 회계로 한 번만 반영한다.
항별 배타적 매핑과 calibration은 `TBD-P01`로 사전 고정한다.

다른 목적함수나 $x_B$ anchor penalty 제거, 현금과 위험자산을 한 최적화에 합치는 변경은
v1 primary가 아니라 Challenger/ADR 대상이다. 결과가 더 좋아 보여도 Blueprint 또는 ADR
변경 없이 v1 Champion 경로에 사용할 수 없다. C′는 포트폴리오 구성 계약이며 H-NET의
acceptance는 9장의 Net Calmar와 경제효과 계약을 따른다.

### 8.2 불변 제약

- 별도 `EffectiveExposure` 엔진이 레짐 목표와 종목별 한도·적격 종목 수·유동성 제약을
  반영해 `0 <= EffectiveExposure <= 1`을 결정한다.
- 선택된 적격 위험자산이 0개이거나 `EffectiveExposure = 0`이면 C′와 fallback을 실행하지 않고
  `w_cash = 1`로 둔다.
- 계좌 비중은 `w_i = EffectiveExposure * x_i`로 환산하며 `w_i <= 0.20`을 만족해야 한다.
  종목당 NAV 20%는 절대 안전 상한이다.
- `w_cash = 1 - sum(w_i)`: 현금은 C′ 내부에 넣지 않는다. 적격 종목이 부족하거나
  신호·유동성·확신이 약하면 실행 불가능한 잔여 NAV를 현금으로 둔다.
- 보유 종목 수와 위험자산 100% 투자를 강제하지 않는다. 예를 들어 적격 종목이 3개이고
  각 20% 상한이면 최대 코인 노출은 60%, 현금은 최소 40%다.
- 최소 주문, 주문금액/ADV 참여율, depth, 예상 impact와 capacity 제약을 적용한다.
- BTC beta와 상관 cluster별 집중도를 제한·보고한다. 정확한 한도는 `TBD-P02`다.
- 거래 유의·정지, stale/missing data, 위험 상태와 kill switch는 모델 점수보다 우선한다.

의사결정은 대사를 마친 실제 계좌 NAV를 사용하며 별도의 `max_authorized_nav` 또는 자본 확대
승인 변수를 두지 않는다. 다만 paper/live 모드, 명시적 live 활성화, 주문·종목·일간손실 한도,
kill switch와 주문·잔고 대사는 별도의 운영 안전장치로 유지한다.

### 8.3 불확실성과 현금

예측 분산, calibration 오차, 데이터 품질, 모델 간 불일치 또는 drift가 커지면
$\tilde{\mu}$를 축소하고 별도 `EffectiveExposure` 엔진이 총 노출을 현금 방향으로 낮춘다.
같은 불확실성을 기대 엣지, CVaR와 anchor penalty에서 중복 처벌하지 않도록 각 항의 역할을
ablation으로 확인한다. 현금 확대는 모델 실패를 숨기는 수익률 장식이 아니며 cash·시장·동일가중
기준선과 함께 노출·위험 조정 성과를 보고한다.

---

## 9. 평가 지표

### 9.1 예측 지표

| 영역 | 필수 지표 | 보고 단위 |
|---|---|---|
| 순위 | Spearman Rank IC, ICIR | 수평선별, 4시간 `as_of`별, fold·레짐·유동성별 |
| 상위 후보 | Precision@K, 상위 분위수 초과수익 | K는 진단용이며 고정 보유 종목 수가 아니다. `TBD-M01`로 사전 지정 |
| 확률 | Brier score 또는 사전 지정 proper score, calibration curve/intercept/slope | 발생률·단순모델 대비 skill과 표본 수 포함 |
| 하방위험 | tail ranking, exceedance rate, calibration loss, CVaR forecast error | 수평선·레짐·유동성별 |

ICIR은 관측 의존성을 무시한 단순 t-stat로 해석하지 않는다. 유효 표본수와 block-bootstrap
불확실성을 함께 보고한다.

### 9.2 비용 차감 포트폴리오 지표

Blueprint의 1차 모델 선택 지표인 **chronological walk-forward outer fold별 Net Calmar의
중앙값**을 primary portfolio metric으로 유지한다. 이 한 지표만으로 승격하지 않으며 아래
필수 지표, paired baseline 차이, 신뢰구간과 안정성 조건을 함께 통과해야 한다. primary metric
변경은 단순 실험 설정이 아니라 Blueprint 변경 검토 대상이다.

Net Calmar 계산 계약은 다음과 같다.

- 모든 outer OOS 예측을 시간순으로 연결한 하나의 after-cost wealth path를 만든다. 같은
  `as_of`를 중복 집계하거나 fold별 초기자본을 독립 합산하지 않는다.
- fold 경계에서 기존 포지션과 미체결 상태를 그대로 이월한다. 강제 청산·재진입 또는 무료
  리밸런싱을 만들지 않고 새 목표와의 차이에 정상 거래비용을 부과한다.
- 수수료, spread, impact, delay와 partial fill이 발생한 시점에 wealth와 포지션을 갱신한다.
- 전체 연속 OOS MDD의 high-water mark는 after-cost 연속 wealth 기준이며 fold 경계에서
  초기화하지 않는다.
- fold Net Calmar를 계산할 때는 포지션·미체결 상태와 실제 비용을 그대로 이월하되, fold 시작
  after-cost wealth를 1로 정규화하고 이를 fold-local high-water mark로 둔다. 해당 fold의
  elapsed-time CAGR을 이 local MDD 절댓값으로 나누며, 이전 fold의 drawdown을 중복 계산하지 않는다.
- non-cash fold의 local MDD가 0이면 임의 epsilon을 넣지 않고 해당 fold Calmar를
  `not estimable`로 기록한다. 무한값 clipping과 사후 제외도 금지한다. 유효한 finite paired
  fold 수가 `TBD-V08`의 사전 기준보다 적으면 primary Net Calmar와 H-NET을 `not estimable`로
  기록하며 승격 통과로 간주하지 않는다.
- KRW cash는 순수익 0, MDD 0의 `0/0`이므로 Calmar paired 비교군에서 제외한다. 대신 연속 OOS
  최종 wealth와 net CAGR이 비용 후 cash 경로를 넘어야 하는 절대 hurdle로 사용하며 경제적
  margin은 `TBD-V08`로 사전 고정한다.

- 순수익 CAGR과 누적 초과수익
- Net Sharpe, Net Calmar, Net Sortino
- MDD, drawdown duration, CVaR와 tail loss
- 시장 beta, BTC beta, 상관 cluster 집중도, 총·순 노출
- turnover와 거래 건수, 목표 대비 체결 비율
- 평균뿐 아니라 중앙값·분위수를 포함한 holding period 분포
- 주문금액·참여율별 capacity curve와 capacity 초과 시 성과
- 예상비용 대비 실현비용 bias, MAE, 분위수 오차와 coverage
- baseline 비용·2배 비용·구성요소 stress 결과

평균 보유기간이 약 7일이라는 목표는 holding-period 분포와 turnover로 점검한다. 7일보다
짧거나 긴 개별 거래를 실패로 처리하지 않는다.

### 9.3 하위표본과 불확실성

점추정치만 보고하지 않는다. 모든 주요 지표는 다음과 함께 제시한다.

- outer fold별 원자료와 집계
- 상승·하락·고변동 등 사전 정의 레짐별 결과
- 유동성 분위, 상장연령, 종목 규모와 데이터 품질별 결과
- 종목·기간 영향도와 특정 극단수익 제거 민감도
- baseline과의 paired 차이에 대한 block-bootstrap 신뢰구간

bootstrap은 같은 `as_of`의 전체 cross-section을 하나의 시간 단위로 묶어 block resample하여
자산 간 동시 상관을 보존한다. 후보 간 차이는 같은 block index를 paired 재표집한다. Rank
IC·proper score는 `as_of`별 통계량을, 포트폴리오는 시간순 순수익 block을 재표집해 각 경로의
CAGR·MDD·Calmar 등을 다시 계산한다. corrected data-driven block length와 여러 길이 민감도를
사용하며 confidence level, 반복 수, 민감도 grid와 multiplicity 규칙은 `TBD-M02`로 첫 primary
outer 결과 열람 전에 정한다. 구조변화가 큰 경우 bootstrap 신뢰구간만으로 안정성을 주장하지
않고 fold·레짐 표를 우선한다.

---

## 10. 과최적화 통제

### 10.1 Experiment registry

성공, 실패, 중단, 오류 실험을 모두 append-only registry에 남긴다. 결과를 본 실험은
보고서에 쓰지 않았더라도 시행 수에서 제거할 수 없다.

| 필드 | 필수 내용 |
|---|---|
| `experiment_id` | 불변 ID, 부모 실험과 `hypothesis_id` |
| 시간·책임 | 생성·시작·종료 시각, 실행자, 검토자, 상태와 중단 사유 |
| 데이터 | raw/canonical/derived/PIT manifest ID와 hash, revision cutoff |
| 코드·환경 | Git commit, dirty flag, dependency lock hash, 실행 image/environment hash |
| 설정 | 전체 config hash, seed, label·feature·universe·cost·portfolio version |
| 모델 | 후보 family, 탐색공간, hyperparameter budget, artifact hash |
| 분할 | inner/outer/CPCV/CSCV/lockbox 정의와 경계 hash |
| 시행 수 | 계획·완료·실패·결과 열람 시행 수, 후보 간 의존성 설명 |
| 결과 | 사전 primary/secondary 지표, 신뢰구간, artifact와 로그 위치 |
| 판정 | 반증·미결정·잠정 지지·승격 후보 및 판정 근거 |

### 10.2 하나의 시행으로 세는 변경

결과를 본 뒤 선택 가능한 모델, 특징, label, horizon, universe, 비용, split, seed, ensemble,
portfolio objective 또는 threshold 변형은 각각 registry에 남긴다. 단순 코드 오류로 출력 전에
중단된 실행은 오류로 구분하되 삭제하지 않는다. 최종 모델 선택에 영향을 준 전체 시행 수와
후보 상관구조를 보고한다.

### 10.3 PBO, DSR, PSR와 Minimum Track Record Length

| 진단 | 답하는 질문 | 대체하지 못하는 것 |
|---|---|---|
| PBO | CSCV에서 IS 승자 선택 절차가 OOS 하위 순위로 붕괴할 위험은 얼마인가? | 개별 전략의 미래 수익 확률, chronological lockbox |
| DSR | 여러 시행과 비정규 수익을 고려할 때 선택된 Sharpe가 얼마나 과장됐는가? | 비용 현실성, 시점 무결성, 레짐 안정성 |
| PSR | 표본길이·왜도·첨도 아래 Sharpe가 사전 benchmark를 넘을 불확실성은 어떠한가? | multiple testing 전체 보정, 미래 수익 보장 |
| Minimum Track Record Length | 사전 Sharpe benchmark를 지정 신뢰도로 구분하는 데 필요한 관측 길이는 얼마인가? | 단순 calendar soak를 수익성 증거로 바꾸는 것 |

각 진단의 benchmark와 신뢰수준은 `TBD-O01`로 사전 등록한다. 중첩 보유기간과 자기상관이
있으므로 IID 가정을 무비판적으로 적용하지 않고 HAC 또는 시간 block-bootstrap 민감도를
병기한다.

### 10.4 연구 변경 통제

- 핵심 가설과 primary metric은 첫 outer 평가 전에 등록한다.
- lockbox를 열기 전 후보군, trial budget과 acceptance rule을 동결한다.
- 사후 분석은 명확히 “exploratory”로 표시하고 확인 실험과 구분한다.
- 실패를 제거하거나 primary/secondary metric을 바꿔 성공처럼 보이게 하지 않는다.
- 동일 아이디어의 이름만 바꿔 시행 수를 초기화하지 않는다.

---

## 11. Champion/Challenger와 drift

### 11.1 감시 대상

| drift | 감시 내용 | 기본 대응 |
|---|---|---|
| feature drift | 분포, 결측/품질 상태, 범위, 상관구조, lineage·schema 변화 | 원인 분리, 필요 시 신규 위험 축소, Challenger 재평가 |
| prediction drift | 점수·순위·확률 분포, calibration, 불확실성, 모델 간 불일치 | calibration 진단, 노출 축소 후보, 자동 승격 금지 |
| Rank IC drift | rolling Rank IC/ICIR, horizon·fold·레짐별 붕괴 | 사전 경계 도달 시 연구 재개 또는 Champion 강등 검토 |
| 위험 drift | 변동성·상관·BTC beta·CVaR 예측과 실현 exceedance | 위험예산 축소, risk fallback 또는 halt |
| execution-cost drift | 예상 대비 realized spread, delay, impact, fill rate, opportunity cost | no-trade band/용량 재검토, 신규 위험 축소 |

window, 기준분포, 검정, 경보·강등 threshold와 최소 표본은 `TBD-DT01`이다. 여러 drift 지표를
반복 감시하는 데 따른 오경보와 데이터 품질 원인도 분리해 보고한다.

### 11.2 재학습과 승격

- 자동 파이프라인은 새 데이터로 Challenger를 학습하고 고정 artifact를 생성할 수 있다.
- 새 artifact는 data/code/config/model hash와 registry 기록을 가져야 한다.
- Challenger는 frozen Champion과 같은 live 입력에서 shadow 비교한다.
- 사람은 예측뿐 아니라 위험, 비용, drift, 장애·복구와 대사 증거를 검토한다.
- 승인 전에는 Champion routing이나 live 주문 경로를 바꾸지 않는다.
- Champion 자동 교체와 drift threshold 자동 완화는 금지한다.
- 성능·데이터 무결성·실행비용 또는 운영 안전성이 강등 조건에 닿으면 fallback, 현금 방향
  감축, 이전 Champion 복귀 또는 live 중단을 실행할 수 있다.

---

## 12. 승격 단계

승격은 다음 순서를 건너뛰지 않는다.

`research → backtest candidate → paper → shadow → small live → scaled live`

| 단계 | 필수 증거 | 허용 범위 | 중단·강등 조건 |
|---|---|---|---|
| research | 가설·반증 규칙, data/time contract, baseline, registry, 재현 가능한 prototype | 오프라인 탐색만 허용 | 누출, PIT 실패, 재현 불가, 결과 선택적 보고 |
| backtest candidate | nested rolling-origin, 비용·2배 stress, 하위표본·CI, CPCV 경로, CSCV/PBO, DSR/PSR, 마지막 lockbox | 고정 candidate artifact와 연구 보고서 | lockbox 소진 후 조정, 비용 후 증분 부재, capacity·tail 의존, 데이터 lineage 결함 |
| paper | 공통 전략 코드, recorded replay/fake exchange, 부분체결·미체결·재시작·멱등성·대사 시험 | 실제 주문 없이 주문 상태·원장 검증 | 중복 의도/주문, 미해결 상태, 재생 불일치, 위험 veto 실패 |
| shadow | 24시간 live data, 가상주문, 실제 latency·spread·depth 기반 counterfactual TCA, drift와 REST 복구 관측 | 거래소 주문 제출 금지; 실제 market impact·fill 증거라고 주장하지 않음 | 데이터 gap 미복구, stale-data 거래, 비용오차·drift 경계, 미해결 P1 |
| small live | 사람 승인, live switch, 제한된 실제 주문, kill switch·REST 대사·rollback 검증 | 실제 NAV로 비중을 계산하되 전용 계좌/모드와 노출·주문·손실 한도로 위험을 제한 | 손실·노출·비용·대사·운영 경계 위반, 비인가 artifact, 복구 실패 |
| scaled live | 같은 frozen Champion의 shadow→small-live 예측·주문·TCA 일치성, capacity·위험·복구의 충분한 독립 관측, 수동 승격 승인 | 실제 NAV에 자동 적응하되 동일 안전 한도 유지 | capacity 초과, cost/risk drift, 대사 불일치, Champion 강등 또는 운영 승인 철회 |

각 단계의 최소 관측량, 성공·중단·강등 조건과 승인자는 해당 단계 시작과 증거 수집 전에
`TBD-G01`로 고정한다. 특히 live 위험·손실·kill 조건은 live 활성화 전에 고정한다. 긴 soak는
데이터와 운영 안정성을 관측할 기회일 뿐이다. 단순히 일정 기간 장애 없이 실행됐다는
사실만으로 수익성을 인정하지 않으며, 짧은 MTRL을 calendar soak로 대신하지 않는다.

paper/shadow 이후 비용, 주문, label 또는 포트폴리오의 경제적 동작이 바뀌면 기존 lockbox
증거는 이전 candidate에만 유효하다. 변경 모델은 새 candidate와 미열람 미래 구간이 필요하다.
경제적 동작을 바꾸지 않는 운영 수정은 equivalence와 회귀시험 증거를 남긴다.

small/scaled live에서도 체결·잔고 변화는 WebSocket으로 실시간 추적하지만 시스템 기동,
WebSocket 재접속, 주문 종료 상태 도달과 정해진 주기에 REST 기준 상태와 대사한다.
WebSocket 이벤트만을 계좌의 최종 진실로 간주하지 않는다.

---

## 13. 미확정 사항과 의사결정 로그

### 13.1 이미 고정된 정책과 연구로 정할 값의 분리

다음은 본 Protocol에서 성과에 맞춰 튜닝하지 않는 설계 정책이다.

- Upbit KRW 현물 Long/Cash, 무레버리지, 무공매도
- canonical 1분봉에서 완결 4시간봉 생성
- 완결 4시간봉마다 정상 알파·레짐·포트폴리오 결정
- 24시간 이벤트 기반 시장·위험·주문·체결·잔고 감시
- 약 7일 목표 평균 보유기간은 고정·최소·최대 보유기간이 아님
- Upbit KRW 전체 catalog, fresh non-warning 전체 analysis universe, 명시적 `top_n=100` 초기 execution shortlist
- Blueprint가 지정한 Elastic Net + LightGBM/LambdaRank v1 Alpha Champion과 Challenger 권한
- 위험자산 슬리브 상대비중, $x_B$ anchor와 별도 `EffectiveExposure` 엔진을 갖는 강건 C′
- 전체 계좌 NAV 기준 종목당 최대 20%, 가변 종목 수와 현금 비중
- actual PIT live-captured와 explicit availability model 기반 reconstructed PIT의 등급 분리
- 실거래 주문 경로에서 LLM 제외, 자동 Champion 승격 금지

### 13.2 TBD 목록

| ID | 미확정 항목 | 확정 방법과 필요한 자료 | 고정 시점 |
|---|---|---|---|
| TBD-R01 | 시장 초과수익 프록시와 외부 benchmark 사용범위 | PIT 재구성 가능성, 구성종목·가중치, 이용가능시각과 beta 분해를 비교 | label 생성 전 |
| TBD-R02 | 1·3·7·14일 label의 시작/종료 가격, 실행 지연, delisting return | arrival/fill 가능성, corporate/market-status event와 민감도 분석 | 첫 학습 전 |
| TBD-R03 | 확률·하방 target, loss와 calibration acceptance | 발생률, 표본수, proper-score baseline, tail coverage 모의시험 | 모델 경쟁 전 |
| TBD-D01 | provisional→final late-event 창과 REST reconciliation 주기 | live 수신 지연·revision 분포와 재접속 관측 | canonical schema 승인 전 |
| TBD-D02 | `NO_TRADE` 1분봉 OHLC/volume 구성 | 거래소 candle 의미와 recorded replay 비교 | canonicalizer 구현 전 |
| TBD-D03 | 잔여 `MISSING`이 있는 4시간봉의 격리·허용 기준 | gap 길이·위치별 feature/label 영향과 복구 성공률 | bar builder 승인 전 |
| TBD-D04 | reconstructed PIT availability model, 보수적 지연과 민감도 | live-captured 지연분포, REST 문서, source metadata와 provenance | 첫 역사 OHLCV backtest 전 |
| TBD-U01 | analysis universe와 execution shortlist의 tie, 결측·불완전 데이터 처리 및 `top_n` 민감도 | 30일 lookback·초기 `top_n=100`·향후 상장 90일 기준 주변의 turnover, capacity, PIT 재현성 비교 | outer 평가 전; 기준 변경은 Blueprint/ADR 검토 |
| TBD-V01 | rolling/expanding window, 학습·OOS 길이와 step | 최소 레짐·label 수, 연속 OOS coverage, drift와 추정분산 trade-off, 모의 누출검사 | 첫 outer 실행 전 |
| TBD-V02 | purging interval과 embargo 길이 | 실제 label interval, 최대 horizon, 실행 지연, feature 정보수명과 민감도 | 첫 outer 실행 전 |
| TBD-V03 | CPCV group/path, CSCV 후보행렬·PBO 구성과 `not estimable` 최소 조건 | purge 후 표본·경로, 고정 후보 수, path coverage와 후보 의존성 | secondary 평가 전 |
| TBD-V04 | 마지막 chronological lockbox 기간·revision cutoff·접근자 | 충분한 독립 시장상태와 미열람 여부 확인 | 개발 OOS 열람 전 |
| TBD-V05 | 모델별 탐색공간·seed·연산·총 trial budget | 단순 기준선 중심 pilot의 분산·비용, 동등 자원 원칙 | 모델 탐색 전 |
| TBD-V08 | cash 절대 hurdle, Calmar 적격 baseline 대비 최소 경제효과, finite paired fold 최소 수 | 비용오차, capital/capacity, bootstrap CI, fold 수와 0 MDD 발생률, 실용적 효용 | 첫 primary outer 결과 열람 전 |
| TBD-V09 | fold·레짐·유동성 안정성 요구 | 하위표본 표본수와 영향도, 실패비용, 다중검정 통제 | 첫 primary outer 결과 열람 전 |
| TBD-C01 | implementation-shortfall 회계식 | 주문 event schema와 replay TCA 회계시험 | 첫 비용 포함 outer 실행 전 |
| TBD-C02 | 기준비용·2배 stress의 구성요소와 상태모형 | 당시 fee, 선택적 호가·체결, shadow TCA | 첫 비용 포함 outer 평가 전; shadow에서 재검증 |
| TBD-C03 | cost-aware no-trade band의 후보공간·선택규칙 | 비용 예측오차, turnover/shortfall trade-off를 inner OOS로 비교; fold별 값은 inner에서만 선택 | 첫 비용 포함 outer 실행 전; 최종 refit 값은 lockbox 전 동결 |
| TBD-P01 | C′의 $\lambda$, $\eta$, $\gamma$, shrinkage, CVaR·비용 추정과 calibration | 비용 항별 배타적 매핑, 계좌 전환량 회계, block stress, nested stability와 중복차감 ablation; C′ 구조 변경은 제외 | 첫 portfolio outer 실행 전; 최종 refit 값은 lockbox 전 동결 |
| TBD-P02 | 총노출, BTC beta, 상관 cluster, 유동성·capacity 제약 | stress 손실, cluster 안정성, depth/ADV 참여율과 small-live TCA | 제약이 영향을 주는 평가·단계 시작 전; 변경 시 새 candidate 평가 |
| TBD-M01 | Precision@K의 K와 상위 후보 진단 정의 | PIT 유니버스 크기·capacity와 사전 분석계획 | outer 평가 전 |
| TBD-M02 | CI 수준, corrected block length, 반복 수, 민감도와 multiplicity 규칙 | corrected automatic selection, Monte Carlo error와 의존성 진단 | 첫 primary outer 결과 열람 전 |
| TBD-O01 | PBO/DSR/PSR benchmark, 신뢰수준, 유효 시행 수와 multiplicity 정책 | 전체 registry, 후보 상관, 비정규·자기상관 진단 | 첫 primary outer 결과 열람 전 |
| TBD-DT01 | feature/prediction/IC/risk/cost drift window와 경계 | outer·shadow 기준분포, 오경보 비용, 최소 표본 | shadow 전 |
| TBD-G01 | 단계별 최소 증거량, 성공·중단·강등 경계와 승인자 | failure injection, MTRL, TCA·대사·위험 관측과 운영 runbook | 해당 단계 시작·증거 수집 전; live 위험조건은 live 활성화 전 |

새 TBD를 발견하면 ID를 추가하고 숨겨진 기본값으로 구현하지 않는다. 위 고정 시점 전에
결정하지 못한 항목은 해당 평가 또는 승격의 blocker다.

### 13.3 의사결정 로그 형식

| 필드 | 내용 |
|---|---|
| `decision_id` | 관련 TBD/가설과 연결된 불변 ID |
| 질문과 범위 | 무엇을 왜 결정하는지, 영향을 받는 단계 |
| 선택지 | 검토한 대안과 제외 이유 |
| 증거 | data/experiment/report ID와 hash, 불확실성·한계 |
| 결정 | 선택값, 적용 시작시각, version |
| 승인 | 연구·위험·운영 검토자와 승인시각 |
| 재검토 조건 | drift, 새 데이터, 정책 변경 등 재개 조건 |

---

## 14. 주요 학술 근거

아래 연구는 후보 가설과 검증 설계의 근거이지 Cassandra의 수익성을 입증하지 않는다.
표본, 거래소, 통화, 빈도, 유니버스, long-short 가능 여부와 비용 가정이 다르므로 Upbit KRW
현물 Long/Cash PIT 데이터에서 사전 등록한 비용 포함 검증을 새로 통과해야 한다.

### 14.1 암호자산 교차단면·모멘텀·시장미시구조

| 근거 | 출판 상태 | Protocol에 주는 제한적 함의 |
|---|---|---|
| Liu & Tsyvinski (2021), [Risks and Returns of Cryptocurrency](https://doi.org/10.1093/rfs/hhaa113), *Review of Financial Studies* | 동료심사 저널 | 암호자산 고유요인과 시계열 모멘텀을 후보 가설로 둘 근거다. Upbit KRW·4시간·미래 순수익의 증명은 아니다. |
| Liu, Tsyvinski & Wu (2022), [Common Risk Factors in Cryptocurrency](https://doi.org/10.1111/jofi.13119), *Journal of Finance* | 동료심사 저널 | 시장·규모·모멘텀과 교차단면 특징을 baseline/feature 후보로 둔다. 원 논문의 long-short 결과를 Long/Cash에 직접 이전하지 않는다. |
| Cakici et al. (2024), [Machine learning and the cross-section of cryptocurrency returns](https://doi.org/10.1016/j.irfa.2024.103244), *International Review of Financial Analysis* | 동료심사 저널 | 복잡도 증분이 제한되고 성과가 소형·비유동·극단수익 자산에 의존할 수 있어 단순 기준선, capacity와 tail stress가 필요하다. 논문 수익을 목표값으로 쓰지 않는다. |
| Cong et al. (2026), [Crypto Value, Factor Pricing, and Market Segmentation](https://doi.org/10.1287/mnsc.2024.05875), *Management Science* | 동료심사 저널, Articles in Advance | network/on-chain 정보와 시장 분절을 연구할 근거다. 실제 `available_at`·revision을 갖춘 후속 PIT Challenger로만 사용한다. |
| Tzouvanas, Kizys & Tsend-Ayush (2020), [Momentum trading in cryptocurrencies: Short-term returns and diversification benefits](https://doi.org/10.1016/j.econlet.2019.108728), *Economics Letters* | 동료심사 저널 | momentum의 horizon 의존성 때문에 1·3·7·14일을 분리 검증한다. 소표본·일봉 결과를 4시간 Upbit 실행으로 일반화하지 않는다. |
| Makarov & Schoar (2020), [Trading and Arbitrage in Cryptocurrency Markets](https://doi.org/10.1016/j.jfineco.2019.07.001), *Journal of Financial Economics* | 동료심사 저널 | 거래소 분절, order flow와 price impact는 arrival/fill, spread/depth와 상태의존 비용을 직접 검증해야 할 근거다. 알파 수익성 증거는 아니다. |
| Brauneis et al. (2021), [How to measure the liquidity of cryptocurrency markets?](https://doi.org/10.1016/j.jbankfin.2020.106041), *Journal of Banking & Finance* | 동료심사 저널 | candle 유동성 proxy 하나를 실행가능성으로 간주하지 않고 실제 spread·depth·impact와 대조한다. BTC/ETH 중심 결과의 전 종목 일반화를 금지한다. |
| Han, Kang & Ryu (2026-03-26 revision), [Momentum in the Cryptocurrency Market: A Comprehensive Analysis under Realistic Assumptions](https://doi.org/10.2139/ssrn.4675565), SSRN working paper | **Preprint/working paper, 비동료심사** | 비용·일중경로·fat tail을 고려한 반례와 민감도 참고자료다. 단독 채택 근거가 아니며 정식 게재 여부와 revision을 추적한다. |

### 14.2 금융 ML 검증·과최적화·불확실성

| 근거 | 출판 상태 | Protocol에 주는 제한적 함의 |
|---|---|---|
| Tashman (2000), [Out-of-sample tests of forecasting accuracy: an analysis and review](https://doi.org/10.1016/S0169-2070(00)00065-0), *International Journal of Forecasting* | 동료심사 저널 | rolling-origin OOS 설계의 근거다. 모든 종목 전역축은 cross-coin 누출을 막기 위한 Cassandra의 추가 계약이다. |
| Cerqueira, Torgo & Mozetič (2020), [Evaluating time series forecasting models](https://doi.org/10.1007/s10994-020-05910-7), *Machine Learning* | 동료심사 저널 | 비정상 시계열에서 시간순 성능추정 절차를 primary로 둘 경험적 근거다. 모든 금융 패널에 대한 보편정리는 아니다. |
| Cawley & Talbot (2010), [On Over-fitting in Model Selection and Subsequent Selection Bias in Performance Evaluation](https://www.jmlr.org/papers/v11/cawley10a.html), *JMLR* | 동료심사 저널 | 전처리·특징·모델 선택을 inner loop에 가두는 nested evaluation 근거다. outer 결과를 보고 후보를 바꾸면 절차도 오염된다. |
| López de Prado (2018), [Advances in Financial Machine Learning](https://www.wiley-vch.de/en?isbn=9781119482086&option=com_eshop&view=product), Wiley | **전문 단행본, 동료심사 저널 논문 아님** | label purging, embargo와 CPCV 구현의 방법론 참고다. CPCV는 chronological lockbox를 대체하지 않는다. |
| Bailey et al. (2017), [The Probability of Backtest Overfitting](https://doi.org/10.21314/JCF.2016.322), *Journal of Computational Finance* | 동료심사 저널 | CSCV/PBO의 원 근거다. 전체 후보 성과행렬과 실패 시행이 필요하며 개별 전략의 참 수익 확률로 해석하지 않는다. |
| Bailey & López de Prado (2012), [The Sharpe Ratio Efficient Frontier](https://doi.org/10.21314/JOR.2012.255), *Journal of Risk* | 동료심사 저널 | PSR와 Minimum Track Record Length의 근거다. IID가 깨지는 중첩 수익에는 의존성 보완이 필요하며 soak 기간 기준이 아니다. |
| Bailey & López de Prado (2014), [The Deflated Sharpe Ratio: Correcting for Selection Bias, Backtest Overfitting, and Non-Normality](https://doi.org/10.3905/jpm.2014.40.5.094), *Journal of Portfolio Management* | 동료심사 저널 | multiple testing, selection bias와 비정규성 보정을 위해 전체 시행 수와 후보 간 의존성을 기록할 근거다. 비용·독립 OOS를 대신하지 않는다. |
| Politis & Romano (1994), [The Stationary Bootstrap](https://doi.org/10.1080/01621459.1994.10476870), *JASA* | 동료심사 저널 | 약한 의존을 보존한 신뢰구간의 근거다. 전역 cross-section 시간 block으로 재표집하며 구조변화에는 별도 fold 분석이 필요하다. |
| Politis & White (2004), [Automatic Block-Length Selection for the Dependent Bootstrap](https://doi.org/10.1081/ETC-120028836) 및 Patton, Politis & White (2009), [Correction to “Automatic Block-Length Selection for the Dependent Bootstrap” by D. Politis and H. White](https://doi.org/10.1080/07474930802459016), *Econometric Reviews* | 동료심사 저널 | block length를 데이터 기반으로 정할 때 corrected algorithm과 길이 민감도를 쓰는 근거다. CI 수준·반복 수는 별도 사전 등록한다. |
| Harvey, Liu & Zhu (2016), [… and the Cross-Section of Expected Returns](https://doi.org/10.1093/rfs/hhv059), *Review of Financial Studies* | 동료심사 저널 | 많은 후보 시험의 false discovery 통제 근거다. 다른 연구의 단일 t-stat threshold를 Cassandra에 그대로 이식하지 않는다. |

### 14.3 시계열 모델 원 논문과 후속 Challenger

| 근거 | 출판 상태 | Protocol에서의 위치 |
|---|---|---|
| Wu et al. (2023), [TimesNet: Temporal 2D-Variation Modeling for General Time Series Analysis](https://openreview.net/forum?id=ju_Uqw384Oq), ICLR 2023 | 동료심사 학회 | task-general 딥러닝 Challenger. 사전 Champion이 아니다. |
| Wang et al. (2024), [TimeMixer: Decomposable Multiscale Mixing for Time Series Forecasting](https://openreview.net/forum?id=7oLshfEIC2), ICLR 2024 | 동료심사 학회 | multi-scale forecasting Challenger. 동일 PIT·비용 조건에서 단순모델과 경쟁한다. |
| Liu et al. (2024), [iTransformer: Inverted Transformers Are Effective for Time Series Forecasting](https://openreview.net/forum?id=JePfAI8fah), ICLR 2024 | 동료심사 학회 | multivariate forecasting Challenger. cross-sectional 투자 성과를 입증한 논문은 아니다. |
| Das et al. (2024), [A decoder-only foundation model for time-series forecasting](https://proceedings.mlr.press/v235/das24c.html), ICML 2024 | 동료심사 학회 | foundation model 후속 Challenger의 대표 근거다. pretraining 중복·version·재현성을 먼저 감사한다. |
| Ansari et al. (2024), [Chronos: Learning the Language of Time Series](https://openreview.net/forum?id=gerNCVqqtR), TMLR | 동료심사 저널 | zero-shot/fine-tuning 가능성을 시험할 후속 후보일 뿐, crypto transfer 성능은 별도 검증한다. |
| Pham et al. (2023), [Learning Fast and Slow for Online Time Series Forecasting](https://openreview.net/forum?id=q-PbpHD3EOk), ICLR 2023 | 동료심사 학회 | online adaptation 후속 Challenger의 근거다. 자동 Champion 갱신에는 사용하지 않는다. |

위 모델 논문은 일반 공개 시계열의 forecasting·분류 등에서 아키텍처를 평가한다. Upbit KRW
현물의 PIT 교차단면 순위, 양의 초과수익 확률, 하방위험 또는 상태의존 비용 후 포트폴리오
성과를 입증하지 않는다. 논문 benchmark 성능은 본 Protocol의 검증을 생략할 근거가 아니다.

### 14.4 근거 등급과 갱신

1. 정식 게재된 동료심사 논문과 학회 원문을 우선한다.
2. preprint/working paper는 상태와 revision date를 명시하고 반례·후보 생성에 활용하되 단독
   승격 근거로 삼지 않는다.
3. 전문 단행본·공식 문서는 구현 계약의 참고자료로 구분하고 동료심사 논문이라고 표기하지
   않는다.
4. 논문 결과와 Cassandra 결과의 표본·시장·빈도·비용 차이를 모든 연구 보고서에 기록한다.
5. 문헌 갱신으로 후보를 추가할 수 있지만, 이미 본 OOS/lockbox에 소급해 후보를 선택하지
   않는다.

---

이 Protocol의 통과는 손실 부재나 미래 수익을 보장하지 않는다. 통과가 의미하는 것은 특정
version의 데이터·코드·설정과 관측 기간에서, 사전 정의한 반증·비용·위험·운영 검사를 통과한
증거가 있고 그 한계를 재현 가능하게 기록했다는 것뿐이다.
