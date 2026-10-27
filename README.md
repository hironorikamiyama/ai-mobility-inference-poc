
# AI Mobility Inference PoC

## Python / PyTorch / Redis Streams / MongoDB / Kubernetes / CircleCI

PythonとPyTorchを利用した非同期推論処理基盤のPoCです。

Redis Streamsによるジョブキュー、Python Workerによる推論処理、MongoDBへの結果保存を組み合わせ、非同期処理の正常系・障害復旧・性能分析・CI自動テストまでを段階的に検証しています。

本PoCでは、機械学習モデルの精度向上ではなく、推論処理を非同期システムとして運用する際に必要な信頼性・再現性・検証可能性を重視しています。

### 主な検証対象

- Redis Streamsによる非同期ジョブ処理
- PyTorch CPU環境での推論
- MongoDBへの結果保存
- Workerの複数レプリカ実行
- Pendingジョブの再回収
- 冪等性を考慮した結果保存
- 再試行上限超過時のDLQ隔離
- Kubernetesでのコンテナ実行
- Worker処理時間・E2Eレイテンシの分析
- CircleCIによる単体・統合・障害復旧テスト
- CI実行時間の最適化

---

# 1. プロジェクトの目的

## 1.1 背景

AI推論処理を実際のシステムへ組み込む場合、推論モデルが単体で動作するだけでは十分ではありません。

例えば、以下のような課題が発生します。

- 短時間に大量の推論要求が発生する。
- 推論処理とAPI応答を分離する必要がある。
- Workerが停止した場合に処理中のジョブが失われる可能性がある。
- DB障害によって推論結果を保存できない場合がある。
- 同一ジョブが再配信され、重複処理される可能性がある。
- システムのボトルネックが推論・Redis・MongoDBのどこにあるか分かりにくい。
- 変更によって既存の障害復旧処理が壊れる可能性がある。

本PoCでは、これらの問題を小規模な環境で再現し、Pythonコードと自動テストによって動作を検証します。

## 1.2 到達目標

1. 非同期推論処理の基本構成を理解する。
2. Kubernetes上でWorkerを実行できるようにする。
3. Redis StreamsのPendingとConsumer Groupを理解する。
4. 保存処理の冪等性を確保する。
5. 障害発生時の復旧経路を検証する。
6. E2EレイテンシとWorker内部処理時間を計測する。
7. CIによって正常系・異常系を自動検証する。
8. 検証結果と制約をREADMEに記録する。

---

# 2. 技術構成

## 2.1 使用技術

| 分類 | 技術 | 用途 |
| --- | --- | --- |
| 言語 | Python 3.14 | アプリケーション・テスト |
| 推論 | PyTorch CPU | 推論処理 |
| 数値計算 | NumPy | 数値処理 |
| メッセージキュー | Redis Streams | ジョブ投入・取得・Pending管理 |
| データベース | MongoDB | 推論結果保存 |
| コンテナ | Docker | Workerイメージ作成 |
| オーケストレーション | Kubernetes / kind | Worker実行・レプリカ管理 |
| テスト | pytest | 単体・統合テスト |
| CI | CircleCI | GitHub連携による自動検証 |
| マニフェスト検証 | Kubeconform | Kubernetesスキーマ検証 |
| バージョン管理 | Git / GitHub | 実装・検証履歴の管理 |

## 2.2 検証環境

主な開発環境はWindowsのWSL2上に構築したLinux環境です。

- WSL2 / Ubuntu
- Python仮想環境（`.venv`）
- Docker
- kind
- kubectl
- Redis
- MongoDB

CircleCIでは、PythonのプライマリコンテナとRedis・MongoDBのサービスコンテナを使用しています。

検証時の主なバージョンは以下です。

| 項目 | バージョン |
| --- | --- |
| CI Python | 3.14.8 |
| PyTorch | 2.14.1+cpu |
| pytest | 9.1.1 |
| CI NumPy（確認時） | 2.5.4 |
| Redis（CI） | 7系 |
| MongoDB（CI） | 7.0系 |
| Kubeconform | v0.6.3 |
| Kubernetes検証スキーマ | v1.37.0 |

これらは検証時の構成であり、依存ライブラリの最新バージョンを示すものではありません。

---

# 3. システムアーキテクチャ

## 3.1 全体構成

```text
             +------------------+
             |     Producer     |
             |   Python Script  |
             +---------+--------+
                       |
                       | XADD
                       v
             +------------------+
             |  Redis Streams   |
             |  inference_jobs  |
             +---------+--------+
                       |
                       | XREADGROUP
                       | XAUTOCLAIM
                       v
       +---------------+---------------+
       |                               |
       v                               v
+------------------+          +------------------+
|     Worker 1     |          |     Worker 2     |
| Python / PyTorch |          | Python / PyTorch |
+---------+--------+          +---------+--------+
          |                             |
          +--------------+--------------+
                         |
                         | 結果保存
                         v
                +------------------+
                |     MongoDB      |
                | inference_results|
                +---------+--------+
                          |
                          | 保存成功
                          v
                     Redis XACK
```

WorkerはRedis Streamsからジョブを取得し、PyTorchによる推論処理を行います。

推論結果をMongoDBへ保存した後、RedisのメッセージをACKします。

## 3.2 主要なRedis設定

| 項目 | 設定 |
| --- | --- |
| Stream | `inference_jobs` |
| Consumer Group | `inference_workers` |
| DLQ | `inference_jobs_dead` |
| ジョブ取得 | `XREADGROUP` |
| Pending再回収 | `XAUTOCLAIM` |
| ACK | `XACK` |

WorkerのConsumer名は環境変数 `CONSUMER_NAME` で指定できます。

## 3.3 MongoDB

データベース：

`ai_mobility`

コレクション：

`inference_results`

ジョブ識別には `job_id` を使用します。

MongoDBへの保存では `_id=job_id` を利用したupsert方式を採用しています。

これにより、同一ジョブが再処理されても同一IDのドキュメントが重複して作成されることを防ぎます。

---

# 4. PyTorch推論処理

## 4.1 推論モデル

本PoCでは、16×16のTensorを利用した小規模なCPU推論処理を使用します。

目的は、複雑な学習済みモデルを評価することではなく、非同期推論処理の入出力・保存・再試行を検証することです。

検証用の入力値として `0.5` を使用し、結果スコアが概ね `0.5` となることを確認しています。

```python
assert abs(document["score"] - 0.5) < 1e-5
assert document["device"] == "cpu"
```

この検証は、モデルの実用精度を保証するものではありません。

## 4.2 Workerの処理

Workerの基本的な処理順序は次のとおりです。

```text
Worker起動
    |
    v
Redis接続・Consumer Group準備
    |
    v
Pendingジョブの再回収確認
    |
    +-- 対象なし --> 新規ジョブ取得
    |
    v
入力値の検証
    |
    v
PyTorch推論
    |
    v
MongoDB保存
    |
    +-- 成功 --> ACK
    |
    +-- 失敗 --> ACKせずPending保持
```

---

# 5. 非同期処理の信頼性設計

## 5.1 Pending管理

Redis StreamsのConsumer Groupでは、取得済みでACKされていないメッセージがPendingとして管理されます。

Workerがメッセージ取得後に停止した場合、対象メッセージはPendingに残ります。

本PoCでは、`XAUTOCLAIM` を利用して一定時間以上アイドル状態のメッセージを別Workerが再回収します。

## 5.2 再回収条件

通常環境での再回収アイドル時間は30秒です。

```python
RECLAIM_IDLE_MS = int(
    os.getenv("RECLAIM_IDLE_MS", "30000")
)
```

環境変数を指定しなければ30,000msが適用されます。

Phase 14からは、統合テストに限って短縮したアイドル時間を指定できるようにしました。

ただし、テスト用の短縮設定と通常環境の30秒設定は、検証上区別して扱います。

## 5.3 MongoDB保存失敗時のACK

MongoDB保存失敗時にはRedis ACKを実行しません。

```text
Redisから取得
    |
    v
PyTorch推論
    |
    v
MongoDB保存失敗
    |
    v
ACKしない
    |
    v
Redis Pendingに残る
```

これにより、保存失敗したジョブを後から再処理する経路を確保します。

ただし、ACKが行われないことだけで、すべての障害に対してジョブ消失が起きないことを保証するものではありません。

## 5.4 冪等性

Redis Streamsの再配信により、同一ジョブが複数回処理される可能性があります。

MongoDBへの保存を `_id=job_id` によるupsertとすることで、同じジョブIDに対応する結果ドキュメントは1件に保たれます。

この設計は、Worker処理の厳密なExactly Once実行を保証するものではありません。

本PoCは、再配信を前提に保存時の重複を抑制する設計です。

---

# 6. 再試行とDLQ

## 6.1 再試行ポリシー

WorkerはRedis Pendingの配信回数を確認し、再試行を継続するか、DLQへ隔離するかを判定します。

判定には `should_dead_letter()` を使用します。

再試行上限を超えたジョブは、通常の推論処理を続行せず、DLQへ移動します。

## 6.2 DLQ隔離

DLQ Stream：

`inference_jobs_dead`

DLQへの記録と元メッセージのACKは、Redisトランザクションで処理します。

```python
with client.pipeline(transaction=True) as pipe:
    pipe.xadd(DEAD_LETTER_STREAM, record)
    pipe.xack(STREAM_NAME, GROUP_NAME, message_id)
    results = pipe.execute()
```

これにより、通常の障害復旧で処理できなかったジョブを、後から分析できるようにします。

## 6.3 DLQ検証の範囲

統合テストでは、Redisの `XCLAIM` を使用して再試行上限直前のPending状態を作成します。

その後、実際のWorkerに `XAUTOCLAIM` を実行させ、DLQ隔離へ進むことを確認します。

検証項目：

- DLQへの対象ジョブの記録
- 元メッセージのPending解消
- Redis ACK
- MongoDBに推論結果が存在しないこと
- Workerの `Retry exhausted:` ログ

Redisトランザクション中の強制障害注入までは実施していません。

---

# 7. Kubernetes実行環境

## 7.1 構成

本PoCでは、kindによるローカルKubernetes環境を使用しました。

使用した主な要素は以下です。

- Namespace
- Deployment
- Pod
- Redis
- MongoDB
- Worker
- PersistentVolumeClaim等のストレージ関連設定

検証Namespace：

`ai-mobility-poc`

ローカルkindクラスタ：

`go-k8s-poc`

## 7.2 Kubernetesマニフェスト

マニフェストは `k8s/` ディレクトリで管理しています。

CircleCIではKubeconformを使用して静的検証を行います。

```bash
kubeconform \
  -strict \
  -summary \
  -kubernetes-version 1.37.0 \
  k8s/
```

検証結果：

```text
Summary: 9 resources found in 6 files - Valid: 9, Invalid: 0, Errors: 0, Skipped: 0
```

検証対象9リソースすべてがスキーマ検証を通過しています。

## 7.3 検証上の制約

Kubeconformのスキーマ検証だけでは、次の項目は保証できません。

- Podの正常起動
- コンテナイメージの取得
- PVCの割り当て
- Redis・MongoDBへの接続
- Worker間の通信
- リソース不足によるスケジューリング問題

これらは実際のKubernetes環境で別途確認する必要があります。

---

# 8. 性能試験

## 8.1 性能検証の目的

Workerの処理能力と非同期推論基盤の遅延特性を把握するため、100ジョブを単位とした負荷試験を実施しました。

主な評価項目：

- E2Eレイテンシ
- p95 / p99
- Redis待機時間
- Worker内部処理時間
- Tensor生成時間
- PyTorch推論時間
- MongoDB保存時間
- Workerレプリカ数による比較

## 8.2 Workerレプリカ比較

Worker 1台と2台で100ジョブの処理を比較しました。

過去の検証では、3回の試行のE2E p95中央値について、以下の結果を記録しています。

| 構成 | E2E p95中央値 |
| --- | ---: |
| Worker 1台 | 442.166ms |
| Worker 2台 | 170.095ms |

この比較では、Worker 2台構成の中央値が約61.5%短い結果となりました。

ただし、実行時の負荷・ウォームアップ・Redis待機などの影響を完全には排除できていません。

したがって、この数値だけでWorker増加による性能改善を因果的に証明したものではありません。

## 8.3 Worker内部処理時間の計測

E2Eレイテンシだけではボトルネックを特定しにくいため、Worker内部の処理時間を分離して計測しました。

主な計測項目：

```text
tensor_ms
inference_ms
mongo_save_ms
```

計測結果を利用して、ジョブのRedis待機時間とWorker内の実処理時間を区別します。

## 8.4 MongoDB統計

MongoDBの状態を確認するため、以下の情報を収集しました。

- 接続数
- オペレーション数
- キャッシュ状態
- Dirty bytes
- 接続拒否状況
- MongoDB保存時間の分布

一部のジョブでは、MongoDB保存時間が100msを超える外れ値が観測されました。

ただし、発生原因は特定できておらず、MongoDB自体の負荷・接続管理・実行環境の変動などを引き続き調査対象としています。

---

# 9. ベンチマーク自動化

## 9.1 自動実行

性能試験の再現性を高めるため、ベンチマーク実行スクリプトを整備しました。

代表的なスクリプト：

```text
scripts/run_benchmark.sh
scripts/run_replica_benchmark.sh
```

これらを利用し、Worker構成や試行回数を変えた検証を実施しています。

## 9.2 出力データ

ベンチマークでは、以下の情報を保存する構成としています。

- E2Eレイテンシ
- Dispatch情報
- Worker内部処理時間
- Workerログ
- Producer実行結果
- Kubernetes Job定義
- MongoDB統計（前）
- MongoDB統計（後）
- MongoDB統計の差分
- 集計CSV

## 9.3 分析時の注意

性能試験の比較では、以下の点を考慮します。

- 同一ジョブ数での比較
- Worker数の違い
- ウォームアップの影響
- WSL2・Docker・kindのリソース競合
- Redis待機時間と推論処理時間の分離
- MongoDB保存時間の外れ値

短時間のPoC結果を、そのまま実運用環境の性能保証値として使用しない方針です。

---

# 10. テスト構成

## 10.1 Python単体テスト

pytestを使用して、Workerの処理ロジックや分析処理などを検証しています。

Phase 14終了時点で確認した単体テスト：

```text
31 passed
```

通常の単体テストはRedis・MongoDBの起動を必要としない構成です。

```bash
python -m pytest -q tests \
  --ignore=tests/integration
```

## 10.2 統合テスト

統合テストは `tests/integration/` に配置しています。

| ファイル | 検証内容 |
| --- | --- |
| `test_services.py` | Redis・MongoDBサービス連携 |
| `test_worker_e2e.py` | Worker正常系E2E |
| `test_worker_recovery.py` | Pending再回収 |
| `test_worker_mongo_failure.py` | MongoDB保存失敗時の未ACK |
| `test_worker_mongo_recovery.py` | MongoDB復旧後のPending再処理 |
| `test_worker_dlq.py` | DLQ隔離・ACK |

統合テストは実際のRedis・MongoDBサービスを必要とします。

```bash
python -m pytest -q tests/integration
```

専用環境以外での実行には注意してください。

現在のWorkerには固定Stream名と固定Consumer Group名があるため、既存のデータがある共有環境に対して無条件に実行しないでください。

---

# 11. CircleCI

## 11.1 CIの目的

GitHubへのpushを契機に、ソースコード・Docker・Kubernetesマニフェスト・Redis・MongoDBを利用した処理を自動検証します。

CircleCI設定：

`.circleci/config.yml`

## 11.2 CIジョブ

現在、4種類のジョブを使用しています。

| ジョブ | 検証内容 |
| --- | --- |
| `test` | Python・Bash構文チェック、単体テスト |
| `docker-build` | Dockerイメージビルド・PyTorch起動 |
| `k8s-validate` | Kubernetesマニフェスト検証 |
| `integration-test` | Redis・MongoDB・Worker統合テスト |

ワークフロー：

```yaml
workflows:
  ci:
    jobs:
      - test
      - docker-build
      - k8s-validate
      - integration-test
```

4ジョブは独立して実行します。

## 11.3 Dockerビルド検証

`docker-build` では、Dockerイメージのビルドとスモークテストを実施します。

```bash
docker build \
  --tag ai-mobility-inference-poc:ci \
  .
```

PyTorch起動確認：

```bash
docker run --rm \
  --entrypoint python \
  ai-mobility-inference-poc:ci \
  -c "import torch; print('PyTorch:', torch.__version__)"
```

確認結果：

```text
PyTorch: 2.14.1+cpu
```

## 11.4 JUnit XML

pytest結果はJUnit XML形式で保存しています。

統合テストの出力先：

`test-results/integration.xml`

CircleCIの `store_test_results` と `store_artifacts` を利用し、テスト結果を確認できるようにしています。

---

# 12. Phase 12：正常系E2E統合テスト

Phase 12では、CircleCI上で実際のRedis・MongoDBサービスを起動し、Workerを使用したE2E統合テストを追加しました。

## 12.1 サービス連携テスト

検証内容：

1. Redisへの接続
2. MongoDBへの接続
3. Consumer Group作成
4. Redis Streamsへのジョブ投入
5. `XREADGROUP` による取得
6. MongoDBへの保存
7. 同一job_idによる重複防止
8. Redis ACK
9. Pending解消

## 12.2 Worker E2Eテスト

実際のWorkerを別プロセスとして起動し、以下を検証します。

```text
Redis Streams
    |
    v
Workerがジョブ取得
    |
    v
PyTorch推論
    |
    v
MongoDB保存
    |
    v
Redis ACK
```

確認結果：

```text
2 passed in 2.24s
```

## 12.3 MongoDBバージョンの互換性

CircleCI環境では、MongoDB 8系の起動時にLinuxカーネルとの互換性問題が発生しました。

そのため、CIの統合テストではMongoDB 7.0系を使用しています。

ローカル環境のMongoDB 8系とCIのMongoDB 7.0系では、検証環境が異なります。

CIでの成功によって、MongoDB 8系との完全な互換性を保証するものではありません。

---

# 13. Phase 13：障害復旧シナリオの自動検証

Phase 13では、正常系E2Eに加えて主要な障害復旧シナリオをCIへ追加しました。

## 13.1 Pending再回収

Consumer Aが取得したジョブをACKせず、Pendingに残します。

別Worker Bが `XAUTOCLAIM` で再取得し、MongoDB保存とACKまで実行できることを確認します。

## 13.2 MongoDB保存失敗時の未ACK

WorkerのMongoDB接続先に無効なURIを指定します。

```text
mongodb://127.0.0.1:1/?directConnection=true
```

Workerが保存失敗した後、対象メッセージがRedis Pendingに残ることを確認します。

## 13.3 MongoDB復旧後の再処理

Worker AではMongoDB保存失敗を再現します。

その後、Worker Aを停止し、正常なMongoDBへ接続できるWorker Bを起動します。

Worker BがPendingジョブを再回収し、MongoDB保存とACKを完了することを確認します。

## 13.4 DLQ隔離

Redisの `XCLAIM` で再試行上限直前のPending状態を作成します。

Workerの再回収処理によって上限に到達させ、DLQへの隔離と元メッセージのACKを検証します。

## 13.5 検証結果

2026年10月22日時点でのPhase 13最終結果：

```text
6 passed in 74.99s
```

4つのCircleCIジョブすべてが成功しました。

この時点では、Pending再回収における30秒の待機が統合テスト時間の大半を占めていました。

---

# 14. Phase 14：CircleCI高速化・テスト安定性改善

2026年10月22日、CircleCIの実行効率と統合テストの保守性向上を目的としてPhase 14を実施しました。

主な取り組みは以下です。

- Python仮想環境のキャッシュ
- NumPy依存関係の追加
- 統合テストの実行時間分析
- Pending再回収待機時間の短縮
- Worker起動・終了処理の共通化
- 同一コミットでの繰り返し検証

---

## 14.1 Python仮想環境のキャッシュ

### 改善前

`integration-test` ジョブでは、実行のたびにPyTorch・Redis・PyMongo・pytestなどをインストールしていました。

環境準備に40〜50秒程度かかっていました。

### 改善内容

CircleCIの `restore_cache` と `save_cache` を使用し、`.venv` を再利用する構成へ変更しました。

キャッシュの対象：

`.venv`

使用するPython：

`3.14.8`

PyTorch：

`2.14.1+cpu`

キャッシュの再利用時は、必要なライブラリをimportできることを確認します。

### NumPy依存関係の追加

初期のキャッシュ構成では、PyTorchから以下の警告が表示されました。

```text
UserWarning: Failed to initialize NumPy:
No module named 'numpy'
```

このためNumPyを明示的にインストールし、キャッシュキーをv2へ更新しました。

検証時のNumPyバージョン：

`2.5.4`

### v2キャッシュの結果

| 項目 | 初回 | キャッシュ再利用 |
| --- | ---: | ---: |
| キャッシュ復元 | 0秒（Miss） | 5秒 |
| 環境準備 | 52秒 | 6秒 |
| 合計準備時間 | 52秒 | 11秒 |
| `integration-test` 全体 | 153秒 | 106秒 |

キャッシュ再利用により、環境準備時間を約79%短縮できました。

保存したキャッシュサイズは約228MiBです。

この結果は特定のCI実行における計測値であり、ネットワーク・CI負荷・キャッシュ状態によって変動します。

---

## 14.2 統合テストの実行時間分析

pytestへ `--durations=10` を追加しました。

```bash
.venv/bin/python -m pytest -q tests/integration \
  --durations=10 \
  --junitxml=test-results/integration.xml
```

これにより、各テストの実行時間を確認できるようになりました。

### 最適化前の計測結果

| テスト | 実行時間 |
| --- | ---: |
| MongoDB復旧後のPending再処理 | 33.62秒 |
| Pending再回収 | 32.12秒 |
| MongoDB保存失敗時の未ACK | 6.85秒 |
| Worker正常系E2E | 1.65秒 |
| DLQ隔離 | 1.64秒 |
| サービス連携 | 0.02秒 |

統合テスト全体：

```text
6 passed in 76.11s
```

Pending再回収を含む2件だけで65.74秒を占めていました。

これは統合テスト全体の約86.4%です。

この結果から、Pending再回収条件の30秒待機が最大のボトルネックと判断しました。

---

## 14.3 Pending再回収待機時間の最適化

### 変更前

Workerの再回収アイドル時間は30,000msの固定値でした。

### 変更後

環境変数によって設定できるように変更しました。

```python
RECLAIM_IDLE_MS = int(
    os.getenv("RECLAIM_IDLE_MS", "30000")
)
```

通常環境では30秒を維持します。

CIのPending再回収テストでは、短縮した値を指定して機能を検証します。

### MongoDB復旧テストの注意点

当初、Worker AとWorker Bの双方に1,000msを設定したところ、Worker Aが保存失敗後に自身のPendingジョブを早期再回収し、配信回数が増加しました。

そのため、Workerごとに設定を分けました。

| Worker | 再回収設定 |
| --- | ---: |
| Worker A（保存失敗を再現） | 30,000ms |
| Worker B（復旧処理） | 1,000ms |

この設定により、Worker Aの不要な再回収を抑え、Worker Bで高速な復旧処理を確認できるようにしました。

### 最適化結果

| テスト | 最適化前 | 最適化後 |
| --- | ---: | ---: |
| MongoDB復旧後の再処理 | 33.62秒 | 8.67秒 |
| Pending再回収 | 32.12秒 | 2.05秒 |

統合テスト全体：

```text
最適化前：76.11秒
最適化後：21.50秒
```

約71.8%の短縮となりました。

注意点として、CIで1秒の設定を使用したテストは、通常環境の30秒条件そのものの検証とは区別しています。

---

## 14.4 Workerプロセス管理の共通化

### 改善前

複数の統合テストで、以下の処理が重複していました。

- `subprocess.Popen()` によるWorker起動
- 環境変数の設定
- Worker起動ログの確認
- `terminate()` / `kill()` による終了
- ログファイルの作成・読み取り・クローズ

### 改善後

`tests/integration/conftest.py` に共通の `WorkerProcess` とpytest fixtureを実装しました。

```text
tests/integration/
    |
    +-- conftest.py
    |      |
    |      +-- WorkerProcess
    |      +-- worker_processes fixture
    |
    +-- test_worker_e2e.py
    +-- test_worker_recovery.py
    +-- test_worker_mongo_failure.py
    +-- test_worker_mongo_recovery.py
    +-- test_worker_dlq.py
    +-- test_services.py
```

共通化した主な操作：

| 操作 | 共通メソッド |
| --- | --- |
| Worker起動 | `worker_processes(...)` |
| 起動待機 | `worker.wait_until_started()` |
| プロセス状態 | `worker.process.poll()` |
| ログ読み取り | `worker.read_log()` |
| 停止 | `worker.stop()` |
| 最終後片付け | pytest fixture |

### 使用例

```python
worker = worker_processes(
    consumer="ci-worker",
    mongo_uri="mongodb://localhost:27017/",
    reclaim_idle_ms=1000,
)

worker.wait_until_started()

# テスト処理

worker.stop()
```

fixtureは、テスト終了時に登録済みWorkerを停止し、ログファイルを閉じます。

Redis・MongoDBデータを削除する前には、テスト側でWorkerを明示的に停止しています。

### 共通化対象

以下の5ファイルで共通化を完了しました。

1. `test_worker_mongo_recovery.py`
2. `test_worker_recovery.py`
3. `test_worker_mongo_failure.py`
4. `test_worker_e2e.py`
5. `test_worker_dlq.py`

移行時には、旧 `subprocess.Popen` の戻り値と `WorkerProcess` オブジェクトの違いによる `AttributeError` が発生しました。

旧ヘルパーの呼び出しを共通メソッドへ置き換え、不要になった関数・ログ変数を削除することで解消しました。

各ファイルの移行後はCircleCIで既存6件の統合テストが成功することを確認しています。

### 共通化後の実行結果

DLQテストまで共通化した時点での結果：

```text
6 passed in 21.09s
```

`integration-test` ジョブ全体：

```text
36秒
```

共通化によって、Workerの起動・終了に関する変更を一元管理しやすくなりました。

---

## 14.5 CI繰り返し実行による安定性検証

### 目的

コードを変更せず、同一コミットを複数回実行した際の成功率と処理時間の変動を確認します。

対象コミット：

`0cfbae6`

統合テスト6件を合計3回実行しました。

### 実行結果

| 実行 | pytest時間 | CIジョブ全体 | 結果 |
| --- | ---: | ---: | --- |
| 1回目 | 21.09秒 | 36秒 | 6 passed |
| 2回目 | 22.68秒 | 43秒 | 6 passed |
| 3回目 | 22.20秒 | 50秒 | 6 passed |

### 集計結果

| 評価項目 | 結果 |
| --- | ---: |
| 実行回数 | 3回 |
| 成功回数 | 3回 |
| 失敗回数 | 0回 |
| 観測成功率 | 100% |
| pytest平均時間 | 21.99秒 |
| pytest最短時間 | 21.09秒 |
| pytest最長時間 | 22.68秒 |
| 最大・最小差 | 1.59秒 |

今回の3回では、Pending再回収・MongoDB復旧・DLQ隔離を含む6件すべてのテストが成功しました。

ただし、3回の連続成功はフレーキーテストが存在しないことを証明するものではありません。

今後も実行結果を蓄積し、タイミング依存の失敗を監視する必要があります。

---

## 14.6 Phase 14の改善結果

### 統合テスト実行時間

| 項目 | 改善前 | 改善後 |
| --- | ---: | ---: |
| pytest実行時間 | 76.11秒 | 平均21.99秒 |
| 統合テスト件数 | 6件 | 6件 |
| Worker起動管理 | 各テスト個別 | 共通fixture |
| 再回収待機 | 30秒固定 | 通常30秒・CI短縮可能 |
| CI依存環境 | 毎回構築 | `.venv` キャッシュ |

pytest実行時間を約71%短縮し、検証内容は6件のまま維持しています。

なお、CircleCIジョブ全体の所要時間は、Python環境準備・キャッシュ復元・コンテナ起動などによって変動します。

### 主な成果

- PyTorch依存環境の再インストールを削減
- NumPy初期化警告への対応
- テスト別実行時間の可視化
- Redis Pending再回収待機の短縮
- Workerプロセス管理の一元化
- 6件の統合テストを維持
- 同一コミット3回の成功を確認

---

# 15. 現在の品質検証範囲

Phase 14終了時点の検証構成です。

| 領域 | 検証内容 | 結果 |
| --- | --- | --- |
| Python単体テスト | pytest 31件 | 成功 |
| Redis・MongoDB統合テスト | pytest 6件 | 成功 |
| Docker | イメージビルド・起動 | 成功 |
| PyTorch | CPU環境での起動 | 成功 |
| Kubernetes | マニフェスト9リソース | 成功 |
| Redis Pending | 再回収 | 成功 |
| MongoDB障害 | 未ACK・復旧後再処理 | 成功 |
| DLQ | 隔離・元メッセージACK | 成功 |
| CI安定性 | 同一条件で3回実行 | 3回成功 |

### 検証できていないこと

- 実際のMongoDBサーバー停止・再起動
- Redisサーバー停止・復旧
- Kubernetes Podの強制終了・再スケジューリング
- Worker複数レプリカによる同時Pending回収
- Redisトランザクション中の障害注入
- MongoDB 8系とCI環境との互換性
- 本番同等負荷での復旧性能
- 長期間の連続稼働
- DLQからの再投入・手動復旧
- 大量ジョブ処理時のスケーラビリティ

本PoCのCI成功は、これらの未検証項目を保証するものではありません。

---

# 16. 今後の改善課題

## 16.1 テスト安定性

- 統合テストの繰り返し実行回数を増やす。
- テスト間でRedis Streamを完全に分離する。
- Workerの正常終了とログ取得をさらに改善する。
- タイムアウト値を継続的に評価する。
- 本番設定の30秒再回収条件に対する回帰テストを整備する。

## 16.2 障害復旧

- 実際のWorker強制終了試験
- MongoDBサーバー停止・復旧試験
- Redis停止・復旧試験
- Kubernetes Podの強制終了試験
- 複数Workerによる同時再回収
- DLQからの再投入処理

## 16.3 性能検証

- Workerレプリカ数と処理能力の関係を追加検証する。
- Redis待機時間とWorker内部処理時間を継続的に分析する。
- MongoDB保存時間の外れ値の発生条件を調査する。
- p95・p99の比較条件を明確にする。
- CIで実行可能な軽量性能回帰テストを検討する。

## 16.4 CI/CD

- CI実行時間とクレジット消費の継続的な確認
- pytestカバレッジ計測
- 依存関係のバージョン管理・固定方法の改善
- Pull Requestへの必須チェック設定
- Dockerイメージのセキュリティ検査
- Kubernetes環境へのデプロイ検証
- CIと本番環境のMongoDBバージョン差異の解消検討

---

# 17. 実行・検証コマンド

## 17.1 Python構文チェック

```bash
python -m compileall -q src scripts tests
```

## 17.2 単体テスト

```bash
python -m pytest -q tests \
  --ignore=tests/integration
```

## 17.3 統合テスト

Redis・MongoDBのテスト専用サービスが起動している環境で実行します。

```bash
python -m pytest -q tests/integration \
  --durations=10
```

既存データのある共有Redis環境や通常のkindクラスタでは、無条件に実行しないでください。

## 17.4 Dockerビルド

```bash
docker build \
  -t ai-mobility-inference-poc:ci \
  .
```

## 17.5 Git差分確認

```bash
git diff --check
git status --short
git diff --stat
```

---

# 18. 検証履歴

| Phase | 内容 | 状態 |
| --- | --- | --- |
| 1〜5 | 非同期推論基盤の基本構築・信頼性設計 | 完了 |
| 6 | Workerレプリカ比較 | 完了 |
| 7 | Worker内部処理時間の可視化 | 完了 |
| 8 | ベンチマーク自動化・MongoDB分析 | 完了 |
| 9 | CircleCI単体テスト自動化 | 完了 |
| 10 | Dockerビルド自動検証 | 完了 |
| 11 | Kubernetesマニフェスト検証 | 完了 |
| 12 | Redis・MongoDB・Worker正常系E2E | 完了 |
| 13 | Pending再回収・MongoDB障害・DLQ | 完了 |
| 14 | CI高速化・Worker管理共通化・安定性検証 | 完了 |
| 15 | Workerプロセス強制終了・MongoDB保存前チェックポイント・Redis Pending再回収・統合テスト3回連続成功 | 完了 |
| 16 | Kubernetes Worker Pod強制削除・Redis Pending再回収・MongoDB保存・ACK | 完了 |
| 17 | Kubernetes DeploymentによるPod自動再作成・Redis Pending再回収・MongoDB保存・ACK・Readiness Probe改善 | 完了 |
| 18 | Kubernetes障害復旧時間の定量測定・3回の反復検証・集計JSON生成 | 完了 |
| 19 | Kubernetes障害復旧時間の統計集計・CSV/Markdownレポート自動生成・入力値検証・pytest15件 | 完了 |
| 20 | 測定条件に基づく復旧時間のPoC閾値評価 | 完了 |
| 21 | 旧Pod・代替Podの測定条件記録・一致確認・Phase 20連携の実クラスタ検証（3件） | 完了 |
---

# 19. まとめ

本PoCでは、Python・PyTorch・Redis Streams・MongoDB・Kubernetesを利用し、非同期推論処理基盤の構築と検証を実施しました。

初期段階では、WorkerがRedisからジョブを取得し、推論結果をMongoDBへ保存できることを確認しました。

その後、Pending再回収、冪等保存、再試行、DLQ隔離を実装・検証し、障害時の復旧経路を整備しました。

さらに、性能計測とベンチマーク自動化を行い、E2Eレイテンシだけでなく、Redis待機・PyTorch推論・MongoDB保存を分離して分析できるようにしました。

CircleCIでは、単体テスト31件、統合テスト6件、Kubernetesマニフェスト9リソース、Dockerイメージのビルド・起動を継続的に検証できる構成としています。

Phase 14では、統合テストの実行時間を76.11秒から平均21.99秒へ短縮し、Workerプロセス管理を共通化しました。

同一条件で3回繰り返した統合テストもすべて成功しています。

**本PoCの主な成果は、非同期推論処理の正常系だけでなく、障害復旧と性能特性について、コード・ログ・自動テストに基づいて検証できる基盤を構築したことです。**

今後は、より実運用に近い障害注入・負荷条件・Kubernetes環境で検証を拡張し、非機能要件に対する設計と評価の精度を高めていきます。


---

# 20. Phase 15：Worker強制終了・障害復旧テスト

## 20.1 目的

Phase 15では、非同期推論処理中にWorkerプロセスが異常終了した場合の復旧動作を検証した。

これまでのPhase 13では、Redis StreamsのConsumer操作を利用してPending状態を再現し、別Workerによる再回収とMongoDB保存を検証していた。

Phase 15では、実際に動作しているWorkerプロセスを強制終了し、次の処理が成立することを確認した。

- Worker AがRedis Streamsからジョブを取得する
- PyTorchによる推論処理を実行する
- MongoDB保存前にWorker Aを強制終了する
- Redis Pendingに未ACKジョブが残る
- Worker BがXAUTOCLAIMによりPendingを再回収する
- Worker BがPyTorch推論を再実行する
- MongoDBへ推論結果が保存される
- Redis ACKによってPendingが解消される

本検証の目的は、Workerプロセスの突然の終了に対して、Redis Streamsを利用した再処理経路が機能することを確認することである。

---

## 20.2 検証対象

### 使用技術

| 項目 | 技術 |
| --- | --- |
| プログラミング言語 | Python 3.14 |
| 推論処理 | PyTorch CPU |
| メッセージ管理 | Redis Streams |
| 結果保存 | MongoDB |
| テスト | pytest |
| CI | CircleCI |
| Worker管理 | subprocess / WorkerProcess |

### 関連ファイル

| ファイル | 役割 |
| --- | --- |
| `src/worker.py` | 推論・MongoDB保存・Redis ACK |
| `tests/integration/conftest.py` | Worker起動・停止・ログ管理 |
| `tests/integration/test_worker_crash_recovery.py` | 強制終了・障害復旧の統合テスト |
| `.circleci/config.yml` | CircleCI実行設定 |

### 対象となるRedis Streams

Stream：

`inference_jobs`

Consumer Group：

`inference_workers`

使用するRedisコマンド：

- XADD
- XREADGROUP
- XPENDING
- XAUTOCLAIM
- XACK

---

## 20.3 障害復旧シナリオ

今回のテストでは、Worker AとWorker Bの2つのプロセスを利用した。

```text
Producer
   |
   | XADD
   v
Redis Streams
inference_jobs
   |
   | XREADGROUP
   v
Worker A
   |
   | PyTorch推論
   v
MongoDB保存直前
   |
   | テスト用チェックポイント
   v
Worker A 強制終了（SIGKILL）
   |
   v
Redis Pendingにジョブ保持
   |
   | XAUTOCLAIM
   v
Worker B
   |
   | PyTorch再推論
   v
MongoDB保存
   |
   | XACK
   v
Pending解消
```

Worker Aはジョブを取得した後、MongoDB保存を行う前に強制終了される。

RedisのConsumer Groupでは、取得済みでACKされていないメッセージがPendingとして残る。

Worker BがそのPendingメッセージを再回収し、処理を継続する。

---

## 20.4 Phase 15-1：実際のWorker強制終了テスト

### 実装内容

新規統合テストとして、以下を作成した。

`tests/integration/test_worker_crash_recovery.py`

既存のWorkerProcess共通ヘルパーを使用し、Worker AおよびWorker Bの起動・停止・ログ取得を行う。

Worker Aの起動例：

```python
worker_a = worker_processes(
    consumer=consumer_a,
    mongo_uri=mongo_uri,
    reclaim_idle_ms=30000,
    processing_delay_seconds=10,
)

worker_a.wait_until_started()
```

Redis Streamsへジョブを投入した後、Redis Pendingへの登録とMongoDB未保存状態を確認し、Worker Aを強制終了する。

```python
worker_a.process.kill()
worker_a.process.wait(timeout=5)

assert worker_a.process.poll() is not None
```

Worker Bは再回収時間を1,000msに設定して起動する。

```python
worker_b = worker_processes(
    consumer=consumer_b,
    mongo_uri=mongo_uri,
    reclaim_idle_ms=1000,
    processing_delay_seconds=0,
)

worker_b.wait_until_started()
```

Worker Bによる再回収・保存・ACKが完了すると、MongoDBに対象ジョブの結果が1件存在し、Redis Pendingから対象メッセージが消えることを確認する。

### Phase 15-1の結果

CircleCIで新規テストを含む統合テスト7件が成功した。

```text
7 passed in 25.43s
```

新規強制終了・復旧テストの実行時間：

`3.54秒`

この段階では、Worker AがRedisからジョブを取得したことは確認できたが、強制終了時に推論処理のどの地点まで到達していたかを厳密には確認できていなかった。

そのため、Phase 15-2で保存直前のチェックポイントを追加した。

---

## 20.5 Phase 15-2：MongoDB保存直前のチェックポイント

### 改善の目的

Phase 15-1では、Redis Pending登録とMongoDB未保存を確認してWorker Aを停止していた。

ただし、この条件だけでは、Worker AがPyTorch推論を完了したかどうか判断できない。

そこで、PyTorch推論終了後、MongoDB保存直前にテスト用のチェックポイントを追加した。

### Workerの処理順序

```text
Redisジョブ取得
      |
      v
入力値の検証
      |
      v
Tensor生成
      |
      v
PyTorch推論
      |
      v
保存前チェックポイント
      |
      v
MongoDB保存
      |
      v
Redis ACK
```

### チェックポイントの実装

`src/worker.py` に、次の処理を追加した。

```python
# Phase 15-2: MongoDB保存直前のテスト用同期ポイント
pause_before_save = float(
    os.getenv("WORKER_TEST_PAUSE_BEFORE_SAVE_SECONDS", "0")
)

if pause_before_save > 0:
    print(
        f"Before MongoDB save: job_id={job_id}, "
        f"stream_id={message_id}",
        flush=True,
    )
    time.sleep(pause_before_save)
```

環境変数：

`WORKER_TEST_PAUSE_BEFORE_SAVE_SECONDS`

未設定の場合は0秒となり、追加の待機処理は実行されない。

CIの強制終了テストでは、この値を10秒に設定する。

通常のWorker実行時には、今回のチェックポイントによる待機は発生しない。

### MongoDB保存時間の計測

チェックポイントは、既存のMongoDB保存時間計測より前に配置した。

```python
# チェックポイント終了後に計測開始
mongo_started = time.perf_counter()

save_result(
    collection=collection,
    job_id=job_id,
    input_value=input_value,
    score=score,
    device=worker.device.type,
)

mongo_save_ms = (
    time.perf_counter() - mongo_started
) * 1000
```

これにより、テスト用の待機時間が `mongo_save_ms` に含まれないようにしている。

---

## 20.6 WorkerProcess共通ヘルパーの拡張

`tests/integration/conftest.py` のWorkerProcessに、保存前チェックポイントの設定を追加した。

```python
def __init__(
    self,
    consumer,
    mongo_uri,
    reclaim_idle_ms=30000,
    processing_delay_seconds=0,
    pause_before_save_seconds=0,
):
```

Workerへ渡す環境変数：

```python
"WORKER_TEST_PAUSE_BEFORE_SAVE_SECONDS": str(
    pause_before_save_seconds
),
```

また、pytest fixtureからWorkerProcessへ設定値を渡すように変更した。

```python
def start(
    consumer,
    mongo_uri,
    reclaim_idle_ms=30000,
    processing_delay_seconds=0,
    pause_before_save_seconds=0,
):
    worker = WorkerProcess(
        consumer=consumer,
        mongo_uri=mongo_uri,
        reclaim_idle_ms=reclaim_idle_ms,
        processing_delay_seconds=processing_delay_seconds,
        pause_before_save_seconds=pause_before_save_seconds,
    )

    workers.append(worker)
    return worker
```

これによってWorkerごとに保存前の待機時間を指定できる。

### Worker Aの設定

```python
worker_a = worker_processes(
    consumer=consumer_a,
    mongo_uri=mongo_uri,
    reclaim_idle_ms=30000,
    processing_delay_seconds=0,
    pause_before_save_seconds=10,
)
```

Worker AはPyTorch推論後、MongoDB保存前に10秒間待機する。

### Worker Bの設定

```python
worker_b = worker_processes(
    consumer=consumer_b,
    mongo_uri=mongo_uri,
    reclaim_idle_ms=1000,
    processing_delay_seconds=0,
)
```

Worker Bには保存前待機を設定せず、Pending再回収と結果保存を実行させる。

---

## 20.7 チェックポイント到達確認

テストでは、Worker Aのログに次の文字列が出力されるまで待機する。

```text
Before MongoDB save: job_id=..., stream_id=...
```

判定例：

```python
marker = (
    f"Before MongoDB save: job_id={job_id}, "
    f"stream_id={message_id}"
)

deadline = time.monotonic() + 15

while time.monotonic() < deadline:
    if worker_a.process.poll() is not None:
        raise RuntimeError(
            "Worker A exited unexpectedly:\n"
            + worker_a.read_log()
        )

    if marker in worker_a.read_log():
        break

    time.sleep(0.1)
else:
    raise TimeoutError(
        "Worker A did not reach pre-save checkpoint"
    )
```

チェックポイント到達後、MongoDB未保存とRedis Pendingを確認してWorker Aを強制終了する。

```python
assert collection.find_one({"_id": job_id}) is None

worker_a.process.kill()
worker_a.process.wait(timeout=5)

assert worker_a.process.poll() is not None
```

その後、Worker Bによる復旧を検証する。

これにより、少なくともWorker AがPyTorch推論を完了し、MongoDB保存前のチェックポイントに到達したことを確認できる。

---

## 20.8 実装途中に発生した問題

### 発生したエラー

Phase 15-2の初回CircleCI実行では、次のエラーが発生した。

```text
TimeoutError:
Worker A did not reach pre-save checkpoint
```

テスト結果：

```text
1 failed, 6 passed in 38.42s
```

Worker Aのログには次の記録が残っていた。

```text
Worker started: consumer=ci-crash-a-..., device=cpu
Processed: job_id=ci-crash-..., score=0.500000, ... ack=1
```

Worker Aはチェックポイントで待機せず、MongoDB保存とRedis ACKまで完了していた。

### 原因

`tests/integration/conftest.py` のpytest fixtureで、受け取った `pause_before_save_seconds` がWorkerProcessへ渡されていなかった。

修正前：

```python
worker = WorkerProcess(
    consumer=consumer,
    mongo_uri=mongo_uri,
    reclaim_idle_ms=reclaim_idle_ms,
    processing_delay_seconds=processing_delay_seconds,
)
```

修正後：

```python
worker = WorkerProcess(
    consumer=consumer,
    mongo_uri=mongo_uri,
    reclaim_idle_ms=reclaim_idle_ms,
    processing_delay_seconds=processing_delay_seconds,
    pause_before_save_seconds=pause_before_save_seconds,
)
```

### 原因分析

環境変数の定義とWorker起動時の指定は存在していた。

しかし、fixtureからWorkerProcessへの引数の受け渡しが不足していたため、デフォルト値の0が使用されていた。

この結果、Worker Aでチェックポイントが有効にならず、テストがタイムアウトした。

### 対応結果

引数の受け渡しを修正した後、CircleCIの統合テスト7件が成功した。

```text
7 passed in 25.24s
```

Worker強制終了・復旧テスト：

`3.36秒`

---

## 20.9 Phase 15-3：繰り返し実行による安定性検証

### 目的

保存前チェックポイントとWorker強制終了テストが、同一条件で繰り返し成功するかを確認する。

既存の成功実行を基準とし、CircleCIの `integration-test` を追加で2回再実行した。

コードは変更せず、同一コミット・同一設定で合計3回検証した。

### 実行結果

| 実行 | pytest時間 | Worker強制終了・復旧 | CIジョブ全体 | 結果 |
| --- | ---: | ---: | ---: | --- |
| 1回目 | 25.24秒 | 3.36秒 | 81秒 | 7 passed |
| 2回目 | 24.62秒 | 3.56秒 | 42秒 | 7 passed |
| 3回目 | 27.46秒 | 4.38秒 | 82秒 | 7 passed |

### 集計結果

| 項目 | 結果 |
| --- | ---: |
| 実行回数 | 3回 |
| 成功回数 | 3回 |
| 失敗回数 | 0回 |
| 観測成功率 | 100% |
| pytest平均 | 25.77秒 |
| pytest最短 | 24.62秒 |
| pytest最長 | 27.46秒 |
| pytest最大・最小差 | 2.84秒 |
| Worker強制終了・復旧の平均 | 3.77秒 |

### 評価

3回とも統合テスト7件がすべて成功した。

特に、次のシナリオで失敗は確認されなかった。

- Worker強制終了からの復旧
- MongoDB保存失敗時の未ACK
- MongoDB復旧後の再処理
- Redis Pending再回収
- DLQ隔離
- Worker正常系E2E

今回の3回では、Worker強制終了・復旧テストの実行時間は3.36〜4.38秒の範囲だった。

pytest全体は24.62〜27.46秒で推移した。

CIジョブ全体の時間には42〜82秒の差があったが、その原因は今回の記録だけでは特定できていない。

また、3回の成功は、今後フレーキーテストが発生しないことを保証するものではない。

---

## 20.10 Phase 15の受け入れ条件と結果

| 要件 | 検証結果 |
| --- | --- |
| Worker AがRedisジョブを取得する | 成功 |
| Worker AがPyTorch推論を完了する | 成功 |
| MongoDB保存前チェックポイントに到達する | 成功 |
| Worker Aを実際に強制終了する | 成功 |
| 強制終了後もRedis Pendingが残る | 成功 |
| 強制終了時点でMongoDB未保存 | 成功 |
| Worker BがPendingを再回収する | 成功 |
| Worker Bが推論結果を保存する | 成功 |
| MongoDB保存結果が1件となる | 成功 |
| Redis Pendingが解消する | 成功 |
| 統合テスト7件を維持する | 成功 |
| 同一条件で3回連続成功する | 成功 |

---

## 20.11 本検証の制約

本テストは、実際のWorkerプロセスを強制終了する障害注入テストである。

ただし、検証対象には次の制約がある。

### 1. Kubernetes Podの強制終了ではない

今回はpytestから起動したPython WorkerプロセスにSIGKILLを送信している。

Kubernetes Podの削除・再起動・再スケジューリングは検証していない。

### 2. 保存直前の待機は時間ベース

保存前チェックポイントには `time.sleep()` を使用している。

Worker Aがログを出力した後、一定時間が経過するとMongoDB保存へ進む。

極端なCI遅延が発生した場合には、意図した停止位置を維持できない可能性がある。

より厳密な障害注入には、外部イベントによって解除される同期ポイントなどが必要になる。

### 3. Redisサーバー障害は対象外

今回の障害対象はWorkerプロセスである。

Redis自体の停止、データ消失、接続障害、フェイルオーバーは対象外とした。

### 4. MongoDBサーバーの実停止は対象外

MongoDBの保存失敗・復旧については、別の統合テストで接続失敗を模擬している。

Phase 15では、実際のMongoDBサーバー停止や再起動は実施していない。

### 5. Exactly Onceは保証しない

本PoCはRedis Streamsの再配信を前提としている。

MongoDB保存では `_id=job_id` を利用した冪等保存を採用しているが、Worker処理自体の厳密なExactly Once実行は保証しない。

### 6. 長時間・高負荷での安定性は未検証

今回の安定性確認は3回の繰り返し実行である。

長時間稼働、大量ジョブ、高負荷状態、複数Workerによる同時再回収については追加検証が必要である。

---

## 20.12 今後の改善案

### 障害注入ポイントの改善

- 固定時間の待機からイベント同期方式への変更
- 強制終了位置のより厳密な制御
- Workerログ以外のチェックポイント記録方法の検討

### Kubernetes障害復旧

- Worker Podの強制終了
- DeploymentによるPod再起動
- Workerレプリカ間のPending再回収
- Pod再スケジューリング時の状態確認

### Redis・MongoDB障害

- Redis停止・復旧
- MongoDB停止・復旧
- 接続障害中のWorker動作
- 復旧後の再接続と処理再開

### CI安定性

- 繰り返し実行回数の追加
- 統合テスト間のRedis Stream分離
- Worker停止処理の追加検証
- pytest実行時間の継続的な記録

---

## 20.13 Phase 15まとめ

Phase 15では、実際のWorkerプロセスを強制終了し、Redis Pendingに残ったジョブを別Workerが再回収できることを検証した。

Phase 15-1では、実際のプロセス異常終了とPending再回収の基本動作を確認した。

Phase 15-2では、PyTorch推論後・MongoDB保存前にチェックポイントを設け、強制終了位置をより明確にした。

実装途中では、pytest fixtureからWorkerProcessへの引数の受け渡し漏れによって、チェックポイントが機能しない問題が発生した。

原因を特定して修正し、統合テスト7件の成功を確認した。

Phase 15-3では、同一コミットで3回の繰り返し検証を実施し、すべての回で7件の統合テストが成功した。

これにより、**Workerが推論完了後・MongoDB保存前に異常終了した場合でも、Redis Pendingを通じて別Workerが処理を引き継ぎ、結果保存とACKまで完了できることを、実際のプロセス強制終了を用いて確認した。**

今後は、Kubernetes環境でのPod強制終了や、Redis・MongoDBサービス障害を含む検証へ拡張する。


---

# Phase 16：Kubernetes Worker Pod強制削除・障害復旧検証

## 目的

Phase 15では、pytestから起動したPython Workerプロセスを強制終了し、Redis StreamsのPending再回収を検証した。

Phase 16では、実際のKubernetes環境にWorkerを配置し、Pod強制削除後に別Workerが未ACKジョブを再処理できるか検証した。

検証対象は以下の処理である。

1. Worker AがRedis Streamsからジョブを取得
2. PyTorch推論を実行
3. MongoDB保存前チェックポイントに到達
4. Worker A Podを強制削除
5. Redis Pendingにジョブが保持される
6. 別WorkerがPendingを再回収
7. MongoDBへ推論結果を保存
8. Redis ACKによってPendingを解消

## 検証環境

| 項目 | 内容 |
| --- | --- |
| 実施日 | 2026-10-23 |
| OS | Windows / WSL2 |
| Kubernetes | v1.37.0 |
| クラスタ | kind / go-k8s-poc |
| Namespace | ai-mobility-poc |
| Worker | Python 3.14 / PyTorch CPU |
| Redis | Redis Streams / Consumer Group |
| MongoDB | StatefulSet / PVC |
| テスト用イメージ | ai-mobility-inference-poc:phase16-crash-test |

既存のRedis・MongoDBサービスを使用し、検証用Worker Podを別途作成した。

既存データの削除、Redis Consumer Groupの再作成、MongoDBコレクションの初期化は行っていない。

## 検証準備

### Redisの初期状態

障害注入前に以下を確認した。

- Consumer Group：inference_workers
- Redis Pending：0件
- Consumer Group lag：0件
- Redis Stream保存件数：2,535件

Stream保存件数は既存メッセージの履歴数であり、未処理ジョブ件数ではない。

### Dockerイメージ

Phase 15で追加した保存前チェックポイントを含むイメージを作成した。

```bash
docker build \
  -t ai-mobility-inference-poc:phase16-crash-test \
  .

kind load docker-image \
  ai-mobility-inference-poc:phase16-crash-test \
  --name go-k8s-poc
```

Workerのコード内に次の環境変数が存在することも確認した。

`WORKER_TEST_PAUSE_BEFORE_SAVE_SECONDS`

### Worker構成

検証中は、既存の `inference-worker` Deploymentを一時的に0レプリカに変更し、テスト用Worker Aとジョブ取得が競合しないようにした。

Worker Aの設定：

| 設定 | 値 |
| --- | --- |
| Pod | phase16-worker-a |
| Consumer | phase16-worker-a |
| READ_BATCH_SIZE | 1 |
| RECLAIM_IDLE_MS | 30000 |
| WORKER_TEST_PAUSE_BEFORE_SAVE_SECONDS | 180 |
| restartPolicy | Never |

Worker AはPyTorch推論後、MongoDB保存前に180秒待機する。

## 初回検証：手動によるPod強制削除

最初の検証では、次のジョブを投入した。

- JOB_ID：phase16-crash-20261023140838
- Stream ID：1792732119197-0
- input_value：0.5

Worker Aのログで保存前チェックポイント到達を確認した。

```text
Before MongoDB save:
job_id=phase16-crash-20261023140838,
stream_id=1792732119197-0
```

チェックポイント確認時点では、Redis Pendingが1件、MongoDBは未保存だった。

その後、Worker A Podの強制削除を実施したが、削除後の確認時点では次の結果となった。

- Redis Pending：0件
- MongoDB：保存済み
- score：0.5
- processed_at：2026-10-23 14:11:39 JST

保存時刻がチェックポイント到達から約180秒後だったため、待機時間終了により通常処理が進んだ可能性が高い。

ただし、Pod強制削除の正確な実行時刻とコンテナ終了時刻を記録していないため、処理の前後関係は断定できない。

この初回実行は、障害復旧の成功例として扱わない。

## 再検証：障害注入の自動化

初回検証で手動操作の時間差が問題となったため、チェックポイントを検出した直後にPod削除を実行するスクリプトを作成した。

検証スクリプト：

`/tmp/phase16-kill-worker-a.sh`

実行手順：

1. 一意のJOB_IDを生成
2. Redis Streamsへジョブを投入
3. Worker Aのログを定期確認
4. 対象ジョブの保存前チェックポイントを検出
5. 直後にWorker A Podの強制削除を要求
6. Redis PendingとMongoDB保存状態を確認

Pod削除には次のコマンドを使用した。

```bash
kubectl delete pod phase16-worker-a \
  -n ai-mobility-poc \
  --grace-period=0 \
  --force \
  --wait=false
```

なお、このコマンドはKubernetes API上での即時削除を要求するものであり、コンテナプロセスが同時に終了したことまでは保証しない。

## 再検証結果

### テストジョブ

| 項目 | 値 |
| --- | --- |
| JOB_ID | phase16-crash-20261023141624-retry |
| Stream ID | 1792732584316-0 |
| input_value | 0.5 |

### Pod削除直後

```text
Checkpoint reached. Deleting Worker A immediately...

pod "phase16-worker-a" force deleted

Redis Pending:
1

Consumer:
phase16-worker-a

MongoDB:
null
```

Worker Aの保存前チェックポイント到達後、Pod強制削除を実行した。

削除直後の確認では、ジョブがRedis Pendingに残り、MongoDBは未保存だった。

### 別Workerによる復旧

今回の復旧処理は、専用Worker B Podではなく、再スケールした既存の `inference-worker` Deploymentが担当した。

Worker AのPod削除後、既存Deploymentを1レプリカへ戻した。

その後、既存Workerのログに対象ジョブの処理完了が記録された。

```text
Processed: job_id=phase16-crash-20261023141624-retry,
stream_id=1792732584316-0,
score=0.500000,
elapsed_ms=4.25,
queue_ms=30365.747,
worker_ms=3.771,
e2e_ms=30369.518,
redis_wait_ms=30365.531,
dispatch_wait_ms=0.216,
tensor_ms=0.516,
inference_ms=0.542,
mongo_save_ms=2.690,
ack=1
```

MongoDBには次の結果が保存された。

```json
{
  "_id": "phase16-crash-20261023141624-retry",
  "job_id": "phase16-crash-20261023141624-retry",
  "input_value": 0.5,
  "score": 0.5,
  "device": "cpu"
}
```

復旧後のRedis Pendingは0件となった。

### 復旧処理の計測

| 指標 | 実測値 |
| --- | ---: |
| E2E | 30,369.518 ms |
| Redis待機 | 30,365.531 ms |
| Worker処理 | 3.771 ms |
| Tensor生成 | 0.516 ms |
| PyTorch推論 | 0.542 ms |
| MongoDB保存 | 2.690 ms |
| Redis ACK | 1件 |

今回のログでは、E2E時間の大部分をRedis待機時間が占めている。

約30秒の待機時間は、Worker側のPending再回収設定のデフォルト値30,000msと整合している。

ただし、単一のログからRedis待機時間の内訳やPod削除から復旧までの実際の経過時間を完全には確定できない。

## 検証結果のまとめ

| 検証項目 | 結果 |
| --- | --- |
| Kubernetes上でWorker A起動 | 成功 |
| PyTorch推論完了後のチェックポイント到達 | 成功 |
| Worker A Pod強制削除要求 | 成功 |
| 削除直後のRedis Pending保持 | 成功 |
| 削除直後のMongoDB未保存 | 成功 |
| 別Workerによる再処理 | 成功 |
| MongoDB結果保存 | 成功 |
| Redis ACK | 成功 |
| Redis Pending解消 | 成功 |
| 既存Deployment復旧 | 成功 |

## 検証終了後の環境状態

検証終了後、以下を確認した。

- inference-worker Deployment：1/1
- redis Deployment：1/1
- MongoDB Pod：Running
- Redis Pending：0件
- テスト専用Worker Pod：残存なし

RedisとMongoDBの既存データは維持した。

## 制約・未検証事項

### 1. コンテナプロセスの終了時刻

Podの強制削除要求とAPI上の削除を確認したが、コンテナプロセスが実際に終了した時刻までは計測していない。

### 2. Deploymentによる自動復旧ではない

今回の復旧Workerは、手動で1レプリカへ戻した既存Deploymentによって起動した。

KubernetesがPod障害を検出し、Deploymentにより自動的に代替Podを作成するシナリオは検証していない。

### 3. 固定時間ベースのチェックポイント

保存前チェックポイントは180秒の待機処理を使用している。

障害注入の自動化によって操作遅延を減らしたが、実行時刻の完全な同期は保証していない。

### 4. 他サービスの障害

Redisサーバーの停止、MongoDB Podの停止、Node障害、ネットワーク分断は今回の対象外とした。

### 5. Exactly Once

MongoDB保存には `_id=job_id` を用いた冪等保存を採用している。

ただし、Redis Streamsの再処理が発生するため、Workerによる厳密なExactly Once実行は保証しない。

## Phase 16の結論

Kubernetes上で稼働するWorker AがPyTorch推論完了後・MongoDB保存前のチェックポイントに到達した状態で、Pod強制削除を実施した。

削除直後にはRedis Pendingにジョブが残り、MongoDBは未保存だった。

その後、別のWorker Podが対象ジョブを再処理し、MongoDB保存とRedis ACKまで完了した。

以上より、今回のkind環境では、Worker Pod強制削除後にRedis StreamsのPendingを利用した再処理経路が機能することを確認できた。

今後は、Deploymentによる自動Pod再作成やNode障害、Redis・MongoDB自体の障害を対象とした検証へ拡張する。


---

# Phase 17：Kubernetes DeploymentによるWorker Pod自動復旧

## 目的

Phase 16ではWorker Podを強制削除した後、既存Deploymentを手動で再スケールし、Redis Pendingからの復旧を確認した。

Phase 17では、Deploymentのレプリカ数を維持したままWorker Podを強制削除し、Kubernetesによる代替Podの自動作成と、アプリケーションによる未ACKジョブの再処理を検証した。

## 検証環境

- 実施日：2026年10月26日
- Kubernetes：v1.37.0
- クラスタ：kind / go-k8s-poc
- Namespace：ai-mobility-poc
- 推論：Python / PyTorch CPU
- メッセージ管理：Redis Streams
- 結果保存：MongoDB
- テストイメージ：ai-mobility-inference-poc:phase16-crash-test

## MongoDB Readiness Probe改善

事前確認でMongoDBのReadiness Probeが断続的にタイムアウトしていることを確認した。

既存設定ではmongoshによるpingの制限時間が1秒だったため、5秒へ変更した。

```yaml
readinessProbe:
  exec:
    command:
      - mongosh
      - --quiet
      - --eval
      - "db.adminCommand({ping:1}).ok"
  initialDelaySeconds: 15
  periodSeconds: 10
  timeoutSeconds: 5
```

StatefulSet更新後、MongoDB Podの再作成とReady状態への復帰、ping応答を確認した。

長期間にわたる安定性改善については継続観測が必要である。

## Phase 17-2：Deploymentの自己修復確認

検証専用Deploymentを1レプリカで起動し、Podを強制削除した。

削除前Pod：

`phase17-worker-7848d6bc97-nplwg`

自動生成された代替Pod：

`phase17-worker-7848d6bc97-5wsmh`

レプリカ数を操作せず、新PodがRunningとなり、起動ログが出力されることを確認した。

この段階ではRedisやMongoDBを使用せず、KubernetesのPod自動再作成機能だけを検証した。

## Phase 17-3〜4：実際の推論Workerによる復旧

検証専用Deploymentを実際のPython Workerへ切り替えた。

主要設定：

- replicas：1
- CONSUMER_NAME：Pod名をDownward APIから取得
- RECLAIM_IDLE_MS：1000ms
- WORKER_TEST_PAUSE_BEFORE_SAVE_SECONDS：30秒
- restartPolicy：DeploymentによるPod管理
- imagePullPolicy：Never

検証中は既存Workerを0レプリカとし、ジョブ取得の競合を防止した。

### 障害注入

`scripts/phase17-auto-recovery.sh` を作成し、次の処理を自動化した。

1. Redis Streamsへテストジョブ投入
2. WorkerのMongoDB保存前チェックポイントを検出
3. 旧Podの強制削除
4. Redis Pending・MongoDB未保存状態の確認
5. 代替Podの自動生成を確認
6. 代替Podの処理完了ログを確認
7. Redis ACK・MongoDB保存・Pending解消を確認

### テスト対象

| 項目 | 値 |
| --- | --- |
| JOB_ID | phase17-recovery-20261026100643 |
| Stream ID | 1792976804825-0 |
| input_value | 0.5 |

旧Pod：

`phase17-worker-8556d79564-vl6vv`

代替Pod：

`phase17-worker-8556d79564-tgw62`

### 障害直後の状態

- 保存前チェックポイント：到達
- Pod強制削除要求：成功
- Redis Pending：1件
- MongoDB：未保存（null）

### 代替Podによる復旧

代替Workerのログで次を確認した。

```text
Processed: job_id=phase17-recovery-20261026100643,
stream_id=1792976804825-0,
score=0.500000,
ack=1
```

最終確認結果：

- Deployment：1/1
- Redis Pending：0件
- MongoDB保存：成功
- score：0.5
- ACK：1件

復旧中にDeploymentのレプリカ数を手動変更する操作は行わなかった。

## 計測結果

| 指標 | 実測値 |
| --- | ---: |
| E2E | 34,099.210 ms |
| Redis待機 | 4,064.528 ms |
| Worker処理 | 30,034.244 ms |
| Tensor生成 | 0.646 ms |
| PyTorch推論 | 0.670 ms |
| MongoDB保存 | 32.637 ms |

Worker処理時間には、テスト用の保存前待機30秒が含まれている。

また、E2E時間はジョブ投入から保存までの計測であり、Pod削除から復旧までの時間を直接表すものではない。

## Phase 17の検証結果

| 検証項目 | 結果 |
| --- | --- |
| Pod強制削除 | 成功 |
| Deploymentによる代替Pod自動作成 | 成功 |
| 代替Workerの起動 | 成功 |
| Redis Pending保持 | 成功 |
| Pending再回収・再処理 | 成功 |
| MongoDB保存 | 成功 |
| Redis ACK | 成功 |
| Pending解消 | 成功 |
| 通常Workerへの切り戻し | 成功 |
| Python単体テスト31件 | 成功 |

## 制約

- Pod強制削除後の旧コンテナの実際の終了時刻は未計測
- Podの削除からReady復帰までの時間は未計測
- 保存前チェックポイントには30秒の固定待機を使用
- Redis・MongoDB自体の停止やNode障害は対象外
- Worker処理の厳密なExactly Once実行は保証しない

## 結論

Kubernetes DeploymentがWorker Podを自動再作成し、代替WorkerがRedis Pendingの未ACKジョブを再回収してMongoDB保存とRedis ACKを完了することを確認した。

これにより、Phase 16で手動操作が必要だったWorkerの再起動を、Kubernetesの自己修復機能によって自動化できることを検証した。


---

# Phase 18：Kubernetes障害復旧時間の定量測定

## 目的

Phase 17で確認したKubernetes DeploymentによるWorker Pod自動復旧について、復旧に要する時間を定量的に測定した。

Pod削除要求から代替Podの生成、Ready到達、ジョブ復旧完了までを段階的に計測した。

## 測定方法

Pythonの `time.monotonic()` を用いて、以下の時点を記録した。

- T0：旧Pod削除要求開始
- T1：代替Podの作成を検出
- T2：代替PodのReady状態を検出
- T3：代替Workerによるジョブ処理完了を検出

測定指標：

- Pod再作成検出時間：T1 − T0
- Pod Ready検出時間：T2 − T0
- ジョブ復旧完了検出時間：T3 − T0
- Ready後の処理完了時間：T3 − T2

これらはkubectlを利用したポーリングによる観測値であり、Kubernetes内部のイベント発生時刻そのものではない。

## 実装内容

新規スクリプト：

`scripts/phase18-measure-recovery.py`

主な処理：

1. 既存Workerの停止状態を確認
2. Redis Pendingが0件であることを確認
3. テストジョブ投入
4. MongoDB保存前チェックポイント到達を確認
5. 旧Podを強制削除
6. Redis Pending保持・MongoDB未保存を確認
7. 代替Podの生成とReady到達を計測
8. ジョブ処理完了を計測
9. MongoDB保存・Redis ACK・Pending解消を確認
10. 計測結果をJSONに保存

## 測定結果

| 指標 | 1回目 | 2回目 | 3回目 | 平均 |
| --- | ---: | ---: | ---: | ---: |
| Pod再作成検出 | 4.291秒 | 5.827秒 | 8.495秒 | 6.204秒 |
| Pod Ready検出 | 4.782秒 | 6.454秒 | 9.439秒 | 6.892秒 |
| ジョブ復旧完了検出 | 35.208秒 | 38.631秒 | 39.171秒 | 37.670秒 |
| Ready後の処理完了 | 30.426秒 | 32.177秒 | 29.732秒 | 30.778秒 |

実行回数：3回  
成功回数：3回  
失敗回数：0回

## 評価

今回の3回では、Pod削除要求から代替PodのReady検出まで平均6.892秒だった。

ジョブ復旧完了の検出までは平均37.670秒だった。

ただし、代替WorkerにもMongoDB保存前のテスト用待機30秒が適用されるため、ジョブ復旧完了時間は通常運用時の復旧時間を表していない。

また、今回のPodにはWorkerアプリケーション固有のReadiness Probeがないため、Pod ReadyとWorkerの処理可能状態は同義ではない。

## 制約

- Kubernetesイベントそのものの時刻ではなく、ポーリング検出時刻を計測
- 旧Podの実際のコンテナ終了時刻は未計測
- 復旧処理に30秒のテスト用待機を含む
- 測定回数は3回
- Node障害、Redis障害、MongoDB障害は対象外
- 実運用の復旧時間やSLO達成を保証する結果ではない

## 今後の改善

- Workerの起動完了時刻の計測
- アプリケーション固有のReadiness Probe導入
- テスト用待機時間を除いた復旧時間の測定
- Kubernetes Eventsとの時刻比較
- 測定回数の追加と分散評価
- CSV・Markdown形式による集計レポート生成

## 結論

Kubernetes DeploymentによるWorker Pod自動復旧について、代替Podの作成・Ready到達・ジョブ処理完了の各段階を計測できるようになった。

3回の測定すべてでRedis Pending再回収、MongoDB保存、Redis ACKが成功した。

Phase 17の機能検証から、Phase 18では障害復旧の時間特性を評価する段階へ進めることができた。


---

## Phase 19：Kubernetes障害復旧時間の集計・レポート自動生成

### 目的

Phase 18で取得した障害復旧時間の測定結果を再利用し、複数回の測定結果を自動集計する仕組みを実装した。

### 実装内容

- JSON形式の測定結果を一括読み込み
- 成功件数・失敗件数の集計
- 平均・中央値・最小値・最大値・標本標準偏差の算出
- 失敗記録を性能統計の計算対象から除外
- 負数・NaN・Infinityなどの不正測定値を検出
- JSON・CSV・Markdown形式のレポートを自動生成
- pytestによる正常系・異常系の検証

### 実行方法

```bash
python scripts/phase19-recovery-report.py \
  --input output/phase18 \
  --output output/phase19
```

### 測定結果

| 指標 | 平均（秒） | 中央値（秒） | 標準偏差（秒） |
| --- | ---: | ---: | ---: |
| Pod再作成検出 | 6.204 | 5.827 | 2.127 |
| Pod Ready検出 | 6.892 | 6.454 | 2.359 |
| ジョブ復旧完了検出 | 37.670 | 38.631 | 2.149 |
| Ready後の処理完了 | 30.778 | 30.426 | 1.260 |

### 成果物

- `scripts/phase19-recovery-report.py`
- `tests/test_phase19_recovery_report.py`
- `output/phase19/summary.json`
- `output/phase19/recovery-results.csv`
- `output/phase19/recovery-report.md`

※ `output/` 配下はローカル生成物であり、GitHubへの登録対象とは限らない。

### テスト結果

- Phase 19専用テスト：15件成功
- 単体テスト全体：46件成功
- Phase 18の実測JSONを使用した回帰確認：成功

### 制約・今後の課題

- 復旧時間にはテスト用の30秒待機を含む
- Kubernetesイベントの実発生時刻ではなくポーリングによる観測値
- 測定件数3件のため、p95などの評価には使用しない
- 測定レポートの自動生成は可能だが、復旧時間の合否判定は未実装

### 結論

Phase 18の測定データを自動集計し、JSON・CSV・Markdownの3形式で出力できるようになった。

これにより、障害復旧試験の実行から測定結果の保存、統計集計、レポート生成までをPythonで処理できる基盤を構築した。
```

---

## Phase 20：復旧時間のSLO評価

Phase 19の `summary.json` を入力として、復旧時間の平均値を例示閾値と比較する。判定は `PASS`、`FAIL`、`INCONCLUSIVE` の3種類で、必須指標に比較可能な `FAIL` が1つでもあれば総合判定を `FAIL` とする。

```bash
python scripts/phase20-slo-evaluation.py \
  --input output/phase19/summary.json \
  --config config/phase20-slo.json \
  --output output/phase20
```

JSONとMarkdownの評価結果を `output/phase20/` に作成する。Phase 18/19の測定値や既存レポートは変更しない。

設定ファイルの閾値はPoCの例示値であり、正式な本番SLOではない。Phase 20の実装時点ではPhase 18/19の測定条件が不足していたため、そのデータをそのまま評価すると `INCONCLUSIVE` となる。Phase 21では旧Podと代替Podそれぞれの条件を記録し、測定条件が設定と一致し、比較対象の平均値と最低サンプル数も満たされた場合のみ閾値を評価する。不一致や取得不能時は比較不能として `INCONCLUSIVE` を返す。

---

## Phase 21：測定条件の記録とSLO評価連携

### 測定方法

Phase 18で旧Worker Podの削除前と代替PodのReady検出後に、それぞれworkerコンテナの実効`WORKER_TEST_PAUSE_BEFORE_SAVE_SECONDS`とPod定義の`readinessProbe`を取得する。条件・取得不能理由・Pod名・条件取得時刻を測定JSONに保存し、2つのPodの必須条件がすべて既知かつ一致する場合のみ比較可能とする。条件取得時刻（`collected_at`）は観測時刻であり、コンテナ終了時刻を示すものではない。

Phase 19で3件の測定結果を集計し、全測定で条件が一致することを確認した後、Phase 20で平均値をPoC用閾値と比較した。条件の不一致・欠損・取得失敗がある場合はPhase 19が比較不能の理由を記録し、Phase 20は閾値評価を行わず`INCONCLUSIVE`とする。旧形式JSONは読み込み可能だが、旧Pod・代替Pod双方の条件比較ができないため比較不能となる。

```bash
python scripts/phase18-measure-recovery.py
python scripts/phase19-recovery-report.py \
  --input output/phase18 \
  --output output/phase19
python scripts/phase20-slo-evaluation.py \
  --input output/phase19/summary.json \
  --config config/phase20-slo.json \
  --output output/phase20
```

### 実クラスタ検証結果

| 指標 | 平均実測値 | PoC閾値 | 結果 |
| --- | ---: | ---: | --- |
| Pod再作成検出 | 5.277秒 | 10秒 | PASS |
| Pod Ready検出 | 5.736秒 | 15秒 | PASS |
| ジョブ復旧完了検出 | 36.208秒 | 45秒 | PASS |
| Ready後の処理完了 | 30.472秒 | 35秒 | PASS |

- 測定成功：3/3
- 測定条件比較可能：`true`
- 総合評価：**PASS**

### 測定条件

3回の測定で旧Podと代替Podの条件一致を確認し、Phase 20設定と比較した。

| 条件 | 記録値 |
| --- | --- |
| 障害シナリオ | `worker_pod_deletion` |
| 測定方法 | `kubectl_polling_observation` |
| `WORKER_TEST_PAUSE_BEFORE_SAVE_SECONDS` | 30秒 |
| ジョブ復旧時間へのテスト用待機の包含 | 含む |
| WorkerのReadiness Probe | なし |

### 制約・注意事項

- 今回の閾値はPoC検証用の例示値であり、実運用のSLOではない。今回のPASSは本番SLO達成を保証しない。
- ジョブ復旧時間およびReady後の処理完了時間には、テスト用の30秒待機が含まれる。
- 時刻はKubernetesイベントの実発生時刻ではなく、`kubectl`によるポーリングで状態を観測した値である。
- 測定は3件のため、p95などの高パーセンタイル評価には十分でない。
- 既存のPhase 18〜20の説明・記録を維持し、今回のPhase 21記載はこれらに追記した。
