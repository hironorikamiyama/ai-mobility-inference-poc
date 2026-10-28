
---
description: "Phase 24 - 復旧試験・SLO評価・可視化の完全統合"
agent: ai-mobility-engineer
---

# Phase 24：障害復旧評価レポートの完全統合

## 目的

Phase 22オーケストレーターにPhase 23の可視化処理を統合する。

1回のCLI実行で、統計集計、SLO評価、
静的HTML/SVGレポートおよび分析JSONを生成する。

クラスタモードでは、既存の障害注入・復旧測定処理を
利用する。

## 最初に調査するファイル

- README.md
- scripts/phase18-measure-recovery.py
- scripts/phase19-recovery-report.py
- scripts/phase20-slo-evaluation.py
- scripts/phase22-recovery-orchestrator.py
- scripts/phase23-recovery-visualization.py
- tests/test_phase22_recovery_orchestrator.py
- tests/test_phase23_recovery_visualization.py
- config/phase20-slo.json
- .circleci/config.yml

既存のCLI仕様、出力ディレクトリの扱い、
エラー処理、既存テストを確認する。

調査後、実装方針と変更ファイルを日本語で提示してから
実装すること。

## 実装方針

Phase 22を呼び出し元として、
Phase 23の既存機能を再利用する。

統計計算やSLO判定を重複実装しない。

Phase 22の既存CLIと互換性を維持する。

可能なら新しいオプション
--include-visualization を追加する。

既存利用者の動作を変えないため、
可視化機能は明示的なオプションで有効にする。

## オフラインモード

以下の処理を一括実行する。

1. Phase 18測定JSONを読み込む
2. Phase 19統計レポートを生成する
3. Phase 20 SLO評価を実行する
4. Phase 23 HTML/SVG可視化を生成する
5. Phase 22統合結果に可視化成果物のパスを記録する

Kubernetesへの接続や操作は一切行わない。

## クラスタモード

既存のPhase 22安全対策を維持する。

- --confirm-cluster-run を必須とする
- context/namespaceを検証する
- 通常Worker停止・残存Podなしを確認する
- 検証用Workerが1/1 Readyであることを確認する
- Redis Pending 0件を確認する
- Workerのscale変更は自動実施しない
- 測定回数は既存の1～10回制限を維持する
- 測定途中の失敗状態を記録する

今回のAgent作業では実クラスタ操作を行わない。

## 出力構造

既存のPhase 22出力を維持し、
可視化を有効化した場合のみ以下を追加する。

output/
  phase18/
    phase18-*.json
  phase19/
    summary.json
    recovery-results.csv
    recovery-report.md
  phase20/
    slo-evaluation.json
    slo-evaluation.md
  phase23/
    recovery-analysis.html
    recovery-analysis.json
  phase22-result.json

オフラインモードでは、Phase 18測定データが
外部入力ディレクトリに存在する場合がある。

Phase 23へ入力する測定ディレクトリを
正しく指定すること。

## 重要：出力先の安全性

Phase 22は新規出力先のみ許可する。
Phase 23も既存出力先への上書きを拒否する。

両者を統合する際に、
Phase 22の既存出力と衝突しないよう設計する。

必要に応じて安全な一時ディレクトリや
未作成のphase23サブディレクトリを使用する。

- 入力JSONを変更しない
- 既存レポートを上書きしない
- symlink等のパス衝突保護を維持する
- エラー時の部分出力を記録する
- 一部成功を全体成功と誤認させない
- 既存出力先への再実行を拒否する

## ステータスの扱い

Phase 20の評価を尊重する。

- PASS
- FAIL
- INCONCLUSIVE

処理エラー時にはPhase 22全体をERRORとする。

可視化生成に失敗した場合、
評価がPASSでも統合処理全体はERRORとする。

エラー原因と生成済み成果物を記録すること。

途中失敗で部分評価が存在する場合は、
HTMLレポートでも部分評価である旨を明示する。

測定件数不足の場合、INCONCLUSIVEを維持する。

## テスト

pytestで以下を検証する。

1. オフライン一括実行成功
2. HTML/SVG生成成功
3. 分析JSON生成成功
4. 測定条件一致時のPASS
5. 閾値超過時のFAIL
6. サンプル不足時のINCONCLUSIVE
7. 可視化失敗時のERROR
8. 既存出力先の上書き拒否
9. Phase 18データの欠損
10. Phase 22既存CLIの後方互換性
11. クラスタモードの確認フラグ必須
12. context/namespace不一致
13. Worker状態不一致
14. 途中失敗・部分評価の表示
15. HTMLエスケープと外部CDN非依存

クラスタ操作はモックで検証する。

## 制約

- Python標準ライブラリを優先
- 既存Phase 18～23の不要な改修を避ける
- src/worker.pyを変更しない
- Kubernetesマニフェストを変更しない
- 既存の測定データを変更しない
- 実クラスタへのアクセス・障害注入を行わない
- git commitとgit pushを行わない

## 完了確認

以下を実行する。

python -m pytest -q tests --ignore=tests/integration
git diff --check

READMEにPhase 24の実行例、出力構造、
安全上の制約、検証履歴を追記する。

完了後、日本語で以下を報告する。

- 変更ファイル一覧
- 追加機能と設計判断
- 追加テストと全テスト件数
- 動作確認結果
- 未解決の問題・残るリスク

コミットとプッシュは人間の確認後に行う。
