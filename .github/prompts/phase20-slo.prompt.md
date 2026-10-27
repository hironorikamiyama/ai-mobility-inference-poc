
---
description: Phase 20 - Kubernetes障害復旧時間のSLO評価機能を実装
agent: ai-mobility-engineer
---

# Phase 20：SLO評価機能の実装

## 目的

Phase 18の障害復旧測定データと、
Phase 19の統計集計機能を利用して、
復旧時間の自動評価機能を実装する。

## 調査対象

- README.md
- scripts/phase18-measure-recovery.py
- scripts/phase19-recovery-report.py
- tests/test_phase19_recovery_report.py
- .circleci/config.yml

## 実装要件

### 1. 評価基準

SLO評価用のJSON設定ファイルを作成する。

評価基準は正式なSLOではなく、
PoC検証用の例示値として扱う。

### 2. 判定ロジック

以下の3種類を実装する。

- PASS：比較可能で、閾値を満たす
- FAIL：比較可能で、閾値を超過する
- INCONCLUSIVE：比較条件や測定データが不足する

必須指標で比較可能なFAILがあれば、
総合判定ではFAILを優先する。

### 3. 比較可能性

以下を確認する。

- 測定条件
- 評価対象の指標
- 測定データ件数
- テスト用待機時間の有無
- 閾値設定の妥当性

Phase 18の実測データには30秒の
テスト用待機が含まれている。

既存JSONに測定条件が不足する場合、
値を推測せずINCONCLUSIVEとする。

### 4. 出力

- JSON形式の評価レポート
- Markdown形式の評価レポート
- 判定結果と根拠
- 実測値・閾値・サンプル数

### 5. テスト

pytestで次を検証する。

- PASS
- FAIL
- INCONCLUSIVE
- 閾値との境界値
- 測定条件不一致
- 欠損データ
- 不正な設定値
- NaN・Infinity・負数
- 既存機能との互換性

## 制約

- Phase 19の既存出力を変更しない。
- Phase 18・19の生成済みJSONを上書きしない。
- Kubernetesクラスタを操作しない。
- git commit、git pushは実行しない。

## 作業手順

1. 既存コードを調査する。
2. 実装方針を提示する。
3. 必要なPythonコードを生成する。
4. pytestを生成する。
5. テストを実行する。
6. 失敗した場合は原因を調査して修正する。
7. 変更内容とテスト結果を報告する。
