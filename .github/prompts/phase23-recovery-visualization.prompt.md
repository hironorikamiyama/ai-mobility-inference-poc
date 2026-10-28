
---
description: "Phase 23 - 障害復旧時間の可視化・傾向分析"
agent: ai-mobility-engineer
---

# Phase 23：障害復旧時間の可視化と傾向分析

## 役割

Python・Kubernetes・非機能試験に詳しいエンジニアとして、
既存PoCの測定データを分析・可視化する機能を実装する。

今回も既存コードを理解したうえで、
最小限の変更によって機能を追加する。

## 1. 最初に調査するファイル

- README.md
- scripts/phase18-measure-recovery.py
- scripts/phase19-recovery-report.py
- scripts/phase20-slo-evaluation.py
- scripts/phase22-recovery-orchestrator.py
- config/phase20-slo.json
- tests/test_phase19_recovery_report.py
- tests/test_phase20_slo_evaluation.py
- tests/test_phase22_recovery_orchestrator.py
- .circleci/config.yml

既存の入力・出力仕様と測定条件の扱いを調査し、
変更方針を日本語で説明してから実装する。

## 2. 実装目標

Phase 18〜22の測定・評価データを使用し、
復旧時間の推移、ばらつき、閾値との比較を可視化する。

新規スクリプトの候補：

scripts/phase23-recovery-visualization.py

新規テストの候補：

tests/test_phase23_recovery_visualization.py

既存ファイルの命名規則に合わせて実装する。

## 3. 入力仕様

基本入力：

- Phase 18の測定JSON群
- Phase 19の集計JSON
- Phase 20のSLO評価JSON
- Phase 22の実行結果JSON（存在する場合）

実際の既存スキーマを確認し、
利用可能な情報のみを使う。

推測によって測定日時、測定条件、評価結果を補完しない。

Phase 22の出力ディレクトリを指定して
レポートを生成できるCLIを用意する。

Phase 18の旧形式JSONと新形式JSONの両方を扱う。

## 4. 可視化機能

以下を含む静的HTMLレポートを生成する。

### 復旧時間の推移

- Pod再作成検出
- Pod Ready検出
- ジョブ復旧完了検出
- Ready後の処理完了

測定日時がある場合は日時順に表示する。

日時がない場合は順序不明として扱い、
ファイル名順を時系列と誤認させない。

### 閾値との比較

- 実測平均
- 設定された閾値
- PASS / FAIL / INCONCLUSIVE
- 評価根拠

既存のPhase 20評価結果を尊重する。

独自の判定ロジックを重複実装しない。

### 統計情報

- 測定件数
- 成功件数
- 失敗件数
- 平均
- 中央値
- 最小
- 最大
- 標準偏差

既存のPhase 19集計結果を優先して利用する。

## 5. 測定条件と比較可能性

measurement_conditionsを確認する。

測定条件が一致するデータのみ、
同一条件の比較対象として扱う。

条件不明・不一致の場合は、
比較不能の理由を明示する。

異なる条件のデータを混在させて、
改善傾向やSLO達成を断定しない。

今回の測定には30秒のテスト用待機が含まれる。

この待機時間を無条件に差し引いて、
実運用時の復旧時間として表示しない。

3件程度の測定ではp95などを
信頼できるSLO保証として扱わない。

## 6. 出力

以下を新規出力ディレクトリへ作成する。

- recovery-analysis.html
- recovery-analysis.json

HTMLには以下を含める。

- 測定概要
- 復旧時間のグラフ
- 統計情報
- SLO評価結果
- 測定条件
- 制約と注意事項

グラフは軽量なSVGなどを優先する。

HTMLは外部CDNに依存せず、
ローカルで閲覧できるようにする。

JSONや測定データに含まれる文字列は
適切にエスケープし、HTML注入を防止する。

## 7. Phase 22との連携

最初はPhase 23を独立CLIとして実装する。

Phase 22側の既存動作を変更せず、
Phase 22の生成済みディレクトリを読み込んで
分析できるようにする。

Phase 22に直接組み込む変更が必要な場合は、
理由と影響範囲を先に説明する。

## 8. pytest

次のケースを検証する。

1. 正常な測定JSONからHTMLを生成できる
2. 分析JSONを生成できる
3. 複数回測定の順序を適切に扱う
4. 測定日時の欠損
5. 測定条件の一致
6. 測定条件の不一致
7. 旧形式データ
8. 測定データ0件
9. 測定データ1件
10. 異常値・欠損値
11. PASS / FAIL / INCONCLUSIVE表示
12. 入力文字列のHTMLエスケープ
13. 出力先の上書き防止
14. Phase 19〜22との互換性

テストではtmp_pathなどを使用する。

## 9. 制約

- 実クラスタへの接続・障害注入を行わない
- src/worker.pyを変更しない
- Kubernetesマニフェストを変更しない
- 既存測定JSONを変更しない
- Phase 18〜22のレポートを上書きしない
- git commit / git pushを実行しない
- 不要な大規模リファクタリングをしない

## 10. 実装後の確認

以下を実行する。

python -m pytest -q tests --ignore=tests/integration
git diff --check

追加・変更ファイル、設計判断、テスト結果、
既知の制約を日本語で報告する。

Phase 23の実装とテストが完了しても、
GitHubへの反映は人間が最終確認してから行う。
