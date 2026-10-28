
---
description: "Phase 22 - 障害復旧試験・集計・SLO評価の一括実行"
agent: ai-mobility-engineer
---

# Phase 22：障害復旧評価オーケストレーター

## 目的

Phase 18〜21で実装した障害復旧測定、統計集計、
SLO評価を統合し、1つのCLIから実行できるようにする。

## 最初に調査するファイル

- README.md
- scripts/phase18-measure-recovery.py
- scripts/phase19-recovery-report.py
- scripts/phase20-slo-evaluation.py
- config/phase20-slo.json
- tests/test_phase18_measure_recovery.py
- tests/test_phase19_recovery_report.py
- tests/test_phase20_slo_evaluation.py
- .circleci/config.yml

調査後、変更方針を日本語で提示してから実装する。

## 実装要件

### 1. オフラインモード

- デフォルトでオフラインモードとする
- 既存のPhase 18測定JSONを入力として受け取る
- Phase 19の統計集計を実行する
- Phase 20のSLO評価を実行する
- 結果を指定ディレクトリへ出力する
- 元データや既存レポートを上書きしない
- 測定条件が不明な場合はINCONCLUSIVEを維持する

### 2. クラスタ検証モード

- 明示的なオプションでのみ有効化する
- 実行対象のKubernetes contextとnamespaceを確認する
- 通常Workerと検証用Workerの状態を事前確認する
- Redis Pendingが0件であることを確認する
- Phase 18の測定処理を呼び出す
- 複数回実行に対応する
- 測定後、Phase 19・20による評価を行う
- 障害発生時も結果と失敗理由を保存する

重要：
クラスタ検証モードは設計・実装・モックテストまで行う。
今回のAgent作業では実クラスタへの障害注入を行わない。

### 3. 安全対策

- クラスタ検証モードでは実行前に明示的確認を要求する
- Kubernetes contextとnamespaceの不一致では中止する
- 予期せぬWorkerの並列稼働を防止する
- 通常Workerを無断で停止しない
- 検証用Workerを無断で起動しない
- 既存JSON・レポートの上書きを禁止する
- 途中失敗を成功と報告しない
- git commit、git pushを自動実行しない

### 4. テスト

pytestで以下を検証する。

- オフラインモード正常系
- CLI引数の正常・異常系
- 入力ファイル不足
- 出力先の衝突
- INCONCLUSIVEの伝播
- PASS/FAILの伝播
- クラスタ検証モードの確認不足
- context/namespace不一致
- Redis Pendingが残っている場合
- Worker稼働状態不一致
- 測定処理の失敗
- 途中失敗時の結果記録

Kubernetes操作はモックで検証する。

### 5. 互換性

- Phase 18〜21の既存CLIを維持する
- 測定条件の比較可能性判定を維持する
- 既存のpytestを壊さない
- Python標準ライブラリを優先する
- 不要な大規模リファクタリングをしない

## 完了時の確認

以下を実行する。

python -m pytest -q tests --ignore=tests/integration
git diff --check

実装ファイル、テスト件数、設計判断、
既知の制約を日本語で報告する。

クラスタ操作、コミット、プッシュは行わない。
