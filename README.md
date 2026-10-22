
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
