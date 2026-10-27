
---
description: "Phase 21 - 測定条件の自動記録とSLO評価連携"
agent: ai-mobility-engineer
---

# Phase 21：測定条件の自動記録とSLO評価連携

## 役割

Python、Kubernetes、Redis Streams、MongoDB、
非機能試験を専門とするエンジニアとして作業する。

既存の実装を優先し、最小限の変更で機能を追加する。
結果の正当性と比較可能性を重視する。

## 最初に調査するファイル

- README.md
- scripts/phase18-measure-recovery.py
- scripts/phase19-recovery-report.py
- scripts/phase20-slo-evaluation.py
- config/phase20-slo.json
- tests/test_phase19_recovery_report.py
- tests/test_phase20_slo_evaluation.py
- k8s/phase17-worker-deployment.yaml
- .circleci/config.yml

最初に既存実装とデータ構造を確認し、
変更方針と対象ファイルを日本語で説明する。

## 目的

Phase 18 → Phase 19 → Phase 20を連携させ、
障害復旧時間の測定条件を記録・集計・評価できるようにする。

## 要件1：測定条件の自動記録

Phase 18の測定JSONに
measurement_conditionsを追加する。

以下の項目を対象とする。

- failure_scenario
- measurement_method
- test_pause_seconds
- job_recovery_includes_test_pause
- worker_readiness_probe

実行環境から取得できる値は、
Kubernetes DeploymentやPodの実際の設定を参照する。

test_pause_secondsは対象Podの環境変数等から取得し、
実際に使用された設定を確認する。

worker_readiness_probeはコンテナ定義を確認する。

failure_scenarioとmeasurement_methodは、
スクリプトの処理内容に基づいて明示的に記録する。

情報源が不明な項目を推測で補完しない。

取得不能・不明な条件は、その理由を記録し、
比較可能性の評価では不明として扱う。

## 要件2：Phase 19の集計

- 各測定JSONのmeasurement_conditionsを確認する
- 全測定で条件が一致することを検証する
- 条件不一致や欠損を隠して集計しない
- 比較不能な場合は理由を出力する
- サンプル数と成功・失敗件数を維持する
- 既存JSONとの後方互換性を維持する
- Phase 19の既存CLIと出力項目を削除しない

測定条件の一致が確認できない場合、
Phase 20へ比較可能と誤認させる情報を渡さない。

## 要件3：Phase 20との連携

Phase 19のsummary.jsonに記録された条件を、
Phase 20の設定と比較する。

比較条件が一致する場合のみ閾値評価する。

判定は以下とする。

PASS:
条件が一致し、必須指標が閾値以内。

FAIL:
条件が一致し、比較可能な必須指標が閾値超過。

INCONCLUSIVE:
測定条件不明、不一致、サンプル不足などにより
有効な合否判定ができない。

既存のPhase 20評価ルールを尊重する。

## 要件4：テスト

pytestで少なくとも以下を検証する。

1. 測定条件の正常取得
2. 環境変数の欠損
3. readinessProbeの有無
4. 不正な待機時間設定
5. 全データの測定条件一致
6. 測定条件の不一致
7. 測定条件の欠損
8. 旧形式JSONとの互換性
9. 一致条件でPASS
10. 一致条件でFAIL
11. 条件不明でINCONCLUSIVE
12. 集計・判定の回帰テスト

Kubernetesへの接続を必要とする処理は
モックを使用して単体テストする。

## 制約

- 実測済みのPhase 18 JSONを変更しない
- 既存のPhase 19/20レポートを上書きしない
- src/worker.pyを変更しない
- Kubernetesマニフェストを変更しない
- クラスタ操作や障害注入を実行しない
- git commit、git pushを実行しない
- 実測データや測定条件を捏造しない
- 大規模リファクタリングを避ける

出力テストではtmp_pathなどを使用する。

## 実装後の検証

以下を実行する。

python -m pytest -q tests --ignore=tests/integration
git diff --check

テスト失敗時は原因を調査し、
必要な修正と再テストを行う。

## 完了報告

日本語で次を報告する。

- 追加・変更ファイル一覧
- 測定条件の取得方法
- 比較可能性の判定方法
- 既存データへの影響
- テスト実行件数と結果
- 未解決の課題
- 人間がレビューすべき設計判断

実装が完了しても、
コミット・プッシュは人間の承認まで行わない。
