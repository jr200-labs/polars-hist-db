# Changelog

## [0.12.47](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.46...v0.12.47) (2026-07-15)


### Performance Improvements

* **xtdb:** benchmark ingest lookup scaling ([#202](https://github.com/jr200-labs/polars-hist-db/issues/202)) ([d1178bf](https://github.com/jr200-labs/polars-hist-db/commit/d1178bf23e43a4cbf1515d86491c2c90bf7636b0))
* **xtdb:** bound delta reads to incoming keys ([#204](https://github.com/jr200-labs/polars-hist-db/issues/204)) ([88d1f8c](https://github.com/jr200-labs/polars-hist-db/commit/88d1f8c3c6c533c17c8d5951aba051186ca25de9))

## [0.12.46](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.45...v0.12.46) (2026-07-15)


### Performance Improvements

* **xtdb:** keep ingest staging in memory ([#200](https://github.com/jr200-labs/polars-hist-db/issues/200)) ([c5b791e](https://github.com/jr200-labs/polars-hist-db/commit/c5b791efbe32a3f8a5498d44480b4fff5ebc55e8))

## [0.12.45](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.44...v0.12.45) (2026-07-15)


### Performance Improvements

* **xtdb:** clear serialized staging table directly ([#198](https://github.com/jr200-labs/polars-hist-db/issues/198)) ([d9a51fd](https://github.com/jr200-labs/polars-hist-db/commit/d9a51fdeb8d8f402b733ebb09ac51fdb16da7a63))

## [0.12.44](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.43...v0.12.44) (2026-07-15)


### Performance Improvements

* **xtdb:** bulk ingest pgwire writes with arrow copy ([#196](https://github.com/jr200-labs/polars-hist-db/issues/196)) ([7fc00e1](https://github.com/jr200-labs/polars-hist-db/commit/7fc00e1a5b032be768ee664c7a8476c6d277c742))

## [0.12.43](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.42...v0.12.43) (2026-07-15)


### Bug Fixes

* **deps:** update all non-major dependencies ([#192](https://github.com/jr200-labs/polars-hist-db/issues/192)) ([567ac20](https://github.com/jr200-labs/polars-hist-db/commit/567ac2098a17e2ecf22626013b3a17544803efbc))


### Performance Improvements

* **xtdb:** erase staging rows by document id ([#194](https://github.com/jr200-labs/polars-hist-db/issues/194)) ([e661000](https://github.com/jr200-labs/polars-hist-db/commit/e6610008af5820a6f63e0595eb8c882d122b55d1))

## [0.12.42](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.41...v0.12.42) (2026-07-13)


### Features

* **overrides:** add operations, composition, and purge contracts ([#189](https://github.com/jr200-labs/polars-hist-db/issues/189)) ([1d6f0e1](https://github.com/jr200-labs/polars-hist-db/commit/1d6f0e1364bbd6586816a718e0f4f833ff44f796))

## [0.12.41](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.40...v0.12.41) (2026-07-13)


### Features

* add persistent CRDT state-vector diffs ([#187](https://github.com/jr200-labs/polars-hist-db/issues/187)) ([4d6a9a7](https://github.com/jr200-labs/polars-hist-db/commit/4d6a9a72d066e12a9452e597fb3a326526f2c1a3))

## [0.12.40](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.39...v0.12.40) (2026-07-13)


### Bug Fixes

* expose guard on in-memory document access store ([#185](https://github.com/jr200-labs/polars-hist-db/issues/185)) ([d929499](https://github.com/jr200-labs/polars-hist-db/commit/d92949922577f0e9af375978439e8ba70ad4913d))

## [0.12.39](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.38...v0.12.39) (2026-07-13)


### Features

* add backend-neutral document access store ([#183](https://github.com/jr200-labs/polars-hist-db/issues/183)) ([9dbc5d0](https://github.com/jr200-labs/polars-hist-db/commit/9dbc5d03e6ef93574e6f192f0e8838bf4d1e5976))

## [0.12.38](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.37...v0.12.38) (2026-07-12)


### Features

* support atomic CRDT state updates ([#180](https://github.com/jr200-labs/polars-hist-db/issues/180)) ([5d43438](https://github.com/jr200-labs/polars-hist-db/commit/5d43438eec3238e2950cf8e27b0711089e2bff0d))

## [0.12.37](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.36...v0.12.37) (2026-07-12)


### Features

* add persistent CRDT storage contract (JWL-13) ([#171](https://github.com/jr200-labs/polars-hist-db/issues/171)) ([da01297](https://github.com/jr200-labs/polars-hist-db/commit/da012974b36da5186e0dcfb54b9b7ec76a5a65c3))
* add replicated override CRDT core (JWL-13) ([#169](https://github.com/jr200-labs/polars-hist-db/issues/169)) ([7056739](https://github.com/jr200-labs/polars-hist-db/commit/7056739fd165a4b3834923d7efe9e6b34f280d30))
* persist CRDT commits in MariaDB (JWL-13) ([#175](https://github.com/jr200-labs/polars-hist-db/issues/175)) ([bd81d7a](https://github.com/jr200-labs/polars-hist-db/commit/bd81d7a4374d7016e9ae26c7cc8c2031a2432a41))
* persist CRDT commits in XTDB (JWL-13) ([#176](https://github.com/jr200-labs/polars-hist-db/issues/176)) ([1e2d546](https://github.com/jr200-labs/polars-hist-db/commit/1e2d546c812b75cab10858407646b916b8777961))
* prepare CRDT commits in memory (JWL-13) ([#173](https://github.com/jr200-labs/polars-hist-db/issues/173)) ([1c523b2](https://github.com/jr200-labs/polars-hist-db/commit/1c523b2065e9fbe743057e744441cc40b1172621))


### Bug Fixes

* clear implicit XTDB DML transactions ([#179](https://github.com/jr200-labs/polars-hist-db/issues/179)) ([8c510ed](https://github.com/jr200-labs/polars-hist-db/commit/8c510ed5a3c0f127d603e6baf42e5d4e5bdfbc04))
* repair CRDT adapter integration paths ([#178](https://github.com/jr200-labs/polars-hist-db/issues/178)) ([909554e](https://github.com/jr200-labs/polars-hist-db/commit/909554ec9a8630abcd4f444da1b6c0a1b956e29c))
* support shared override projection ownership (JWL-13) ([#174](https://github.com/jr200-labs/polars-hist-db/issues/174)) ([c836b96](https://github.com/jr200-labs/polars-hist-db/commit/c836b9684ba3c901aa719616271828ef1401b25d))

## [0.12.36](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.35...v0.12.36) (2026-07-12)


### Features

* **deps:** update dependency pyarrow to v25 ([#163](https://github.com/jr200-labs/polars-hist-db/issues/163)) ([46fa4b8](https://github.com/jr200-labs/polars-hist-db/commit/46fa4b89daf16efb6e091b2e1f519e7d6b0a47c2))


### Bug Fixes

* **deps:** update dependency ruff to &gt;=0.15.21 ([#162](https://github.com/jr200-labs/polars-hist-db/issues/162)) ([b4cead8](https://github.com/jr200-labs/polars-hist-db/commit/b4cead88aab165c7ec2e854fb27e64c2f5d60d6f))
* **deps:** update quarto-cli digest to 8577ed1 ([#161](https://github.com/jr200-labs/polars-hist-db/issues/161)) ([ae4c31b](https://github.com/jr200-labs/polars-hist-db/commit/ae4c31bee434abaaa6c8cc0e3da5d6706b29ccd9))


### Tests

* cover override ledger audit invariants ([#166](https://github.com/jr200-labs/polars-hist-db/issues/166)) ([66060b6](https://github.com/jr200-labs/polars-hist-db/commit/66060b613f91a233c7cd5d92cdec9520c8ae0849))

## [0.12.35](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.34...v0.12.35) (2026-07-10)


### Features

* **jrl-222:** add override ledger contract ([#158](https://github.com/jr200-labs/polars-hist-db/issues/158)) ([7313ecc](https://github.com/jr200-labs/polars-hist-db/commit/7313ecc5a9e593fe03c1b2db950c0b4204381acd))


### Bug Fixes

* **deps:** update astral-sh/setup-uv action to v8.3.2 ([#155](https://github.com/jr200-labs/polars-hist-db/issues/155)) ([dd89afd](https://github.com/jr200-labs/polars-hist-db/commit/dd89afde904b9e4f90e65043c232c72241574d15))

## [0.12.34](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.33...v0.12.34) (2026-07-08)


### Features

* **jrl-258:** enforce valid-time ingestion contract ([#153](https://github.com/jr200-labs/polars-hist-db/issues/153)) ([33e8b8b](https://github.com/jr200-labs/polars-hist-db/commit/33e8b8b89d780ffc9834de647e07a865e068a3f3))


### Bug Fixes

* **deps:** update astral-sh/setup-uv action to v8.3.1 ([#152](https://github.com/jr200-labs/polars-hist-db/issues/152)) ([b9cebb2](https://github.com/jr200-labs/polars-hist-db/commit/b9cebb235438a558c9930c94399bd985a820a3ce))
* **deps:** update dependency jr200-labs/github-action-templates to shared-v0.1.29 ([#151](https://github.com/jr200-labs/polars-hist-db/issues/151)) ([8630def](https://github.com/jr200-labs/polars-hist-db/commit/8630defa35e744f50f222327ca78789098995122))
* **deps:** update quarto-cli digest to effe145 ([#150](https://github.com/jr200-labs/polars-hist-db/issues/150)) ([754240e](https://github.com/jr200-labs/polars-hist-db/commit/754240e1dbb1db64b7d42d46cbb67ab362dfb487))

## [0.12.33](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.32...v0.12.33) (2026-07-06)


### Bug Fixes

* **deps:** update astral-sh/setup-uv action to v8.3.0 ([#147](https://github.com/jr200-labs/polars-hist-db/issues/147)) ([05d84b9](https://github.com/jr200-labs/polars-hist-db/commit/05d84b9e3d6ed2cf63dc56f025b9c61335842065))
* **xtdb:** ERASE stage rows in cleanup_run ([#149](https://github.com/jr200-labs/polars-hist-db/issues/149)) ([0a8ef5e](https://github.com/jr200-labs/polars-hist-db/commit/0a8ef5e21ca6c20c4172e40a267325504aa8c930))

## [0.12.32](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.31...v0.12.32) (2026-07-05)


### Features

* **observability:** uploader row-flow OTel counters (JRL-269) ([#145](https://github.com/jr200-labs/polars-hist-db/issues/145)) ([2065c7f](https://github.com/jr200-labs/polars-hist-db/commit/2065c7f24f5a7bdb46b7194c9ad686d5d5156f80))

## [0.12.31](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.30...v0.12.31) (2026-07-05)


### Features

* **audit:** reset_dataset() erases data + audit-log atomically (JRL-272) ([#144](https://github.com/jr200-labs/polars-hist-db/issues/144)) ([3465089](https://github.com/jr200-labs/polars-hist-db/commit/34650897a02f4bc2b8f412e9055ef85c85416412))


### Bug Fixes

* **deps:** update dependency jr200-labs/github-action-templates to shared-v0.1.27 ([#141](https://github.com/jr200-labs/polars-hist-db/issues/141)) ([34e0e58](https://github.com/jr200-labs/polars-hist-db/commit/34e0e5858bf32e22eea1faa3c841bba4bd216f77))
* **deps:** update quarto-cli digest to 2c89bd9 ([#140](https://github.com/jr200-labs/polars-hist-db/issues/140)) ([a4859fa](https://github.com/jr200-labs/polars-hist-db/commit/a4859fa6895d08e4a5c69fb1c0927a9de4b33bf7))

## [0.12.30](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.29...v0.12.30) (2026-07-02)


### Bug Fixes

* **deps:** update dependency coverage to &gt;=7.15.0 ([#138](https://github.com/jr200-labs/polars-hist-db/issues/138)) ([e7f42bf](https://github.com/jr200-labs/polars-hist-db/commit/e7f42bfd86621504e0973bd96333bcb35b6dfca6))
* **deps:** update dependency jr200-labs/github-action-templates to shared-v0.1.25 ([#137](https://github.com/jr200-labs/polars-hist-db/issues/137)) ([0a072b2](https://github.com/jr200-labs/polars-hist-db/commit/0a072b282e8241ec34818883611123b8092d5d0e))
* **deps:** update quarto-cli digest to e22b5ed ([#136](https://github.com/jr200-labs/polars-hist-db/issues/136)) ([3cb349e](https://github.com/jr200-labs/polars-hist-db/commit/3cb349ee48b0cb7bd828ab04b075e973ea7c06ea))
* **xtdb:** fall back to pgwire on ADBC ExecuteIngest not implemented ([#135](https://github.com/jr200-labs/polars-hist-db/issues/135)) ([27eed2a](https://github.com/jr200-labs/polars-hist-db/commit/27eed2ab0e4ac5f59d0fc16fcde9696d353380a6))

## [0.12.29](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.28...v0.12.29) (2026-07-02)


### Features

* **deps:** update dependency sql-metadata to v3 ([#132](https://github.com/jr200-labs/polars-hist-db/issues/132)) ([6bbc71d](https://github.com/jr200-labs/polars-hist-db/commit/6bbc71d53fbf75f46a7f23607782c4492e4baa50))


### Bug Fixes

* **deps:** update dependency jr200-labs/github-action-templates to shared-v0.1.23 ([#131](https://github.com/jr200-labs/polars-hist-db/issues/131)) ([74c6017](https://github.com/jr200-labs/polars-hist-db/commit/74c6017d5eb4451527ea16444e45d3f689b21b65))
* **xtdb:** pin _valid_from to epoch for non-temporal tables ([#134](https://github.com/jr200-labs/polars-hist-db/issues/134)) ([70f7a01](https://github.com/jr200-labs/polars-hist-db/commit/70f7a01bc19ec7f16d4796d51787a10c4b9d08af))

## [0.12.28](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.27...v0.12.28) (2026-07-01)


### Bug Fixes

* use bulk xtdb writes for non-temporal ingests ([#129](https://github.com/jr200-labs/polars-hist-db/issues/129)) ([5313939](https://github.com/jr200-labs/polars-hist-db/commit/5313939a24a84ffd0e40e2ed5b1e1384de0be003))

## [0.12.27](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.26...v0.12.27) (2026-07-01)


### Bug Fixes

* **deps:** update dependency jr200-labs/github-action-templates to shared-v0.1.21 ([#125](https://github.com/jr200-labs/polars-hist-db/issues/125)) ([cccf1a5](https://github.com/jr200-labs/polars-hist-db/commit/cccf1a5ae5f2deb86ce29bc09a3b77a3c031fd4b))
* **deps:** update dependency polars to &gt;=1.42.1 ([#126](https://github.com/jr200-labs/polars-hist-db/issues/126)) ([3c383e1](https://github.com/jr200-labs/polars-hist-db/commit/3c383e1140d4224da8b9893dac28a53b0a8080cd))
* **deps:** update quarto-cli digest to ab395f6 ([#124](https://github.com/jr200-labs/polars-hist-db/issues/124)) ([d4783b0](https://github.com/jr200-labs/polars-hist-db/commit/d4783b021b7645862cd4d0cb118550341c04a031))
* preserve xtdb primary key columns ([#128](https://github.com/jr200-labs/polars-hist-db/issues/128)) ([162ca3b](https://github.com/jr200-labs/polars-hist-db/commit/162ca3bb96cb3959ecb62f5bb40fdccf74fa1f33))

## [0.12.26](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.25...v0.12.26) (2026-06-30)


### Bug Fixes

* fall back when xtdb adbc ingest is unavailable ([#122](https://github.com/jr200-labs/polars-hist-db/issues/122)) ([047d31e](https://github.com/jr200-labs/polars-hist-db/commit/047d31e0482ef89337fab0248aad5fb59755f79d))

## [0.12.25](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.24...v0.12.25) (2026-06-30)


### Features

* use xtdb adbc for bulk staging ingest ([#121](https://github.com/jr200-labs/polars-hist-db/issues/121)) ([5c6d04e](https://github.com/jr200-labs/polars-hist-db/commit/5c6d04e2cac41bdff5fc23a7e4a53c01859f6637))


### Bug Fixes

* parameterize xtdb pgwire inserts ([#119](https://github.com/jr200-labs/polars-hist-db/issues/119)) ([2318362](https://github.com/jr200-labs/polars-hist-db/commit/2318362dacdd69f914a9ca2063393f618a692e51))

## [0.12.24](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.23...v0.12.24) (2026-06-30)


### Bug Fixes

* apply xtdb insert row limit to staging writes ([#117](https://github.com/jr200-labs/polars-hist-db/issues/117)) ([8171fd4](https://github.com/jr200-labs/polars-hist-db/commit/8171fd4c0e747bed06422caf595fb7789e83e879))

## [0.12.23](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.22...v0.12.23) (2026-06-30)


### Bug Fixes

* cache empty xtdb staging partitions ([#115](https://github.com/jr200-labs/polars-hist-db/issues/115)) ([0183201](https://github.com/jr200-labs/polars-hist-db/commit/018320120e5d0e225b593b033e81f542c60bc12b))

## [0.12.22](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.21...v0.12.22) (2026-06-30)


### Bug Fixes

* align xtdb staging projection with configured schema ([#112](https://github.com/jr200-labs/polars-hist-db/issues/112)) ([3a4efa4](https://github.com/jr200-labs/polars-hist-db/commit/3a4efa48dbe5278bbf83ccfcb538059523b81fdd))

## [0.12.21](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.20...v0.12.21) (2026-06-30)


### Bug Fixes

* support xtdb pgwire credentials ([#110](https://github.com/jr200-labs/polars-hist-db/issues/110)) ([0b7f692](https://github.com/jr200-labs/polars-hist-db/commit/0b7f6928b21172f48d7a92b4026f1adab8495f59))

## [0.12.20](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.19...v0.12.20) (2026-06-30)


### Features

* **deps:** update dependency pyarrow to v24 ([#107](https://github.com/jr200-labs/polars-hist-db/issues/107)) ([f3f94a5](https://github.com/jr200-labs/polars-hist-db/commit/f3f94a55aa111d516e13fc854da3ae113e4e2cb6))


### Bug Fixes

* **deps:** update all non-major dependencies ([#106](https://github.com/jr200-labs/polars-hist-db/issues/106)) ([b438ad1](https://github.com/jr200-labs/polars-hist-db/commit/b438ad14a2e854170b737805e2613ce8c6ab8a2a))
* **deps:** update quarto-cli digest to 5807604 ([#105](https://github.com/jr200-labs/polars-hist-db/issues/105)) ([050aafc](https://github.com/jr200-labs/polars-hist-db/commit/050aafc904f7d2c49942a1b21fabf0cdcee07bdd))
* normalize xtdb physical column names ([#108](https://github.com/jr200-labs/polars-hist-db/issues/108)) ([95b4003](https://github.com/jr200-labs/polars-hist-db/commit/95b4003124ade3e080cc27ecc824e1e0e1bfdaeb))

## [0.12.19](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.18...v0.12.19) (2026-06-29)


### Bug Fixes

* cast XTDB categorical values through strings ([#103](https://github.com/jr200-labs/polars-hist-db/issues/103)) ([35ee591](https://github.com/jr200-labs/polars-hist-db/commit/35ee5917419a460fb454b1f6e947641227d462a9))

## [0.12.18](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.17...v0.12.18) (2026-06-29)


### Bug Fixes

* tolerate missing nullable XTDB projection columns ([#101](https://github.com/jr200-labs/polars-hist-db/issues/101)) ([2d003f4](https://github.com/jr200-labs/polars-hist-db/commit/2d003f45f1d89f275418e3f2b320a5a1fb6ed710))

## [0.12.17](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.16...v0.12.17) (2026-06-29)


### Bug Fixes

* bound xtdb insert chunk size ([#99](https://github.com/jr200-labs/polars-hist-db/issues/99)) ([870395a](https://github.com/jr200-labs/polars-hist-db/commit/870395a240250bb18b915ab0462132696bc30e49))

## [0.12.16](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.15...v0.12.16) (2026-06-29)


### Bug Fixes

* quote reserved XTDB insert columns ([#97](https://github.com/jr200-labs/polars-hist-db/issues/97)) ([7222888](https://github.com/jr200-labs/polars-hist-db/commit/722288842c0f22cdccac42fb72f7def8c3349e82))

## [0.12.15](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.14...v0.12.15) (2026-06-29)


### Bug Fixes

* preserve xtdb null column types ([#95](https://github.com/jr200-labs/polars-hist-db/issues/95)) ([1d54227](https://github.com/jr200-labs/polars-hist-db/commit/1d542274c00945083a2708f7e62dd5ffcb2e6d63))

## [0.12.14](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.13...v0.12.14) (2026-06-29)


### Bug Fixes

* unblock XTDB upload smoke path ([#93](https://github.com/jr200-labs/polars-hist-db/issues/93)) ([ff04754](https://github.com/jr200-labs/polars-hist-db/commit/ff04754aa133357552e7a10422cfbc36e090307b))

## [0.12.13](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.12...v0.12.13) (2026-06-28)


### Features

* add experimental XTDB backend parity ([#91](https://github.com/jr200-labs/polars-hist-db/issues/91)) ([7d9a192](https://github.com/jr200-labs/polars-hist-db/commit/7d9a192abafa0bb614582684f32fab4358af5eb3))

## [0.12.12](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.11...v0.12.12) (2026-06-28)


### Bug Fixes

* **deps:** update dependency jr200-labs/github-action-templates to shared-v0.1.19 ([#88](https://github.com/jr200-labs/polars-hist-db/issues/88)) ([dc46a93](https://github.com/jr200-labs/polars-hist-db/commit/dc46a93e932386e02bc9dee76661730b4f112725))
* **deps:** update dependency jr200-labs/github-action-templates to shared-v0.1.20 ([#90](https://github.com/jr200-labs/polars-hist-db/issues/90)) ([b202da8](https://github.com/jr200-labs/polars-hist-db/commit/b202da890187654e5cbeaaf9ac99d1a63fbf54d2))
* **deps:** update quarto-cli digest to 02ffed3 ([#89](https://github.com/jr200-labs/polars-hist-db/issues/89)) ([c0ae38e](https://github.com/jr200-labs/polars-hist-db/commit/c0ae38e0f0bb8b322064b660d5c5c6e825d23ba8))
* **deps:** update quarto-cli digest to 45ecb6e ([#84](https://github.com/jr200-labs/polars-hist-db/issues/84)) ([1c5158e](https://github.com/jr200-labs/polars-hist-db/commit/1c5158e45975aee7f282f34a4de0846711d36a17))
* **deps:** update quarto-cli digest to f8a22ff ([#86](https://github.com/jr200-labs/polars-hist-db/issues/86)) ([d90b31d](https://github.com/jr200-labs/polars-hist-db/commit/d90b31d1adc3463c5c90b377bb48ebb2d9765c33))
* **deps:** update shared workflow ref ([#79](https://github.com/jr200-labs/polars-hist-db/issues/79)) ([b2ed129](https://github.com/jr200-labs/polars-hist-db/commit/b2ed129160454b37c3389a122f6ede1cdbdf2042))
* **deps:** update shared workflow ref ([#81](https://github.com/jr200-labs/polars-hist-db/issues/81)) ([c751c67](https://github.com/jr200-labs/polars-hist-db/commit/c751c67c762343697a88a7f3c2d60b096f73ec9c))
* **deps:** update shared workflow ref ([#82](https://github.com/jr200-labs/polars-hist-db/issues/82)) ([49e2ffc](https://github.com/jr200-labs/polars-hist-db/commit/49e2ffcb68cdd182e88e99a1848dbc9e74d382ea))
* **deps:** update shared workflow ref ([#83](https://github.com/jr200-labs/polars-hist-db/issues/83)) ([344c2ad](https://github.com/jr200-labs/polars-hist-db/commit/344c2ad34310cebbbe27997dd6ec45e5b246e1e0))
* **deps:** update shared workflow ref ([#85](https://github.com/jr200-labs/polars-hist-db/issues/85)) ([57e28f5](https://github.com/jr200-labs/polars-hist-db/commit/57e28f5ab536ddd82542b31f922452c1f78b6167))
* **deps:** update shared workflow ref ([#87](https://github.com/jr200-labs/polars-hist-db/issues/87)) ([0fdcf3f](https://github.com/jr200-labs/polars-hist-db/commit/0fdcf3f01252f84d4a2eb440097f074f62db8944))

## [0.12.11](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.10...v0.12.11) (2026-06-19)


### Features

* **deps:** update github-actions to v7 ([#78](https://github.com/jr200-labs/polars-hist-db/issues/78)) ([7d66192](https://github.com/jr200-labs/polars-hist-db/commit/7d66192831f1662b7b2dfa987e44f08a51067bfa))


### Bug Fixes

* **deps:** update quarto-cli digest to 50f3d27 ([#77](https://github.com/jr200-labs/polars-hist-db/issues/77)) ([12b4843](https://github.com/jr200-labs/polars-hist-db/commit/12b4843881677e57615010b5b315e103e81c0096))
* **deps:** update shared workflow ref to shared-v0.1.9 ([#75](https://github.com/jr200-labs/polars-hist-db/issues/75)) ([f3d36f7](https://github.com/jr200-labs/polars-hist-db/commit/f3d36f7352066ebe689e7254f4d1fac665b97859))

## [0.12.10](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.9...v0.12.10) (2026-06-10)


### Bug Fixes

* **deps:** update quarto-cli digest to 1072f7a ([#73](https://github.com/jr200-labs/polars-hist-db/issues/73)) ([cf597cf](https://github.com/jr200-labs/polars-hist-db/commit/cf597cfdf3b7b3230731e46bd0eb1cc837487d91))

## [0.12.9](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.8...v0.12.9) (2026-06-10)


### Bug Fixes

* **deps:** update quarto-cli digest to 838d227 ([#72](https://github.com/jr200-labs/polars-hist-db/issues/72)) ([8fe6be0](https://github.com/jr200-labs/polars-hist-db/commit/8fe6be0a8807cefa807eef6c507cdd6b211cd1e5))
* **deps:** update quarto-cli digest to 90307d3 ([#70](https://github.com/jr200-labs/polars-hist-db/issues/70)) ([962290f](https://github.com/jr200-labs/polars-hist-db/commit/962290ff68f46262751eb8df44c75994670d02e0))

## [0.12.8](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.7...v0.12.8) (2026-06-06)


### Features

* **deps:** update github-actions to v12 ([#53](https://github.com/jr200-labs/polars-hist-db/issues/53)) ([61a318e](https://github.com/jr200-labs/polars-hist-db/commit/61a318e087b4fd677328bc409d3cd0c5ac44b413))


### Bug Fixes

* **deps:** update github-actions to v8.2.0 ([#68](https://github.com/jr200-labs/polars-hist-db/issues/68)) ([e79a368](https://github.com/jr200-labs/polars-hist-db/commit/e79a368fd80389e67a8ee497219da7781a3b8b76))
* **deps:** update quarto-cli digest to 2c283e1 ([#67](https://github.com/jr200-labs/polars-hist-db/issues/67)) ([3faefcc](https://github.com/jr200-labs/polars-hist-db/commit/3faefcc80e5baa6ad182fa6583d177792072d84e))

## [0.12.7](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.6...v0.12.7) (2026-06-01)


### Bug Fixes

* evolve persistent table columns ([#64](https://github.com/jr200-labs/polars-hist-db/issues/64)) ([667d95b](https://github.com/jr200-labs/polars-hist-db/commit/667d95bc7384f7e2a5c619a022b1fa2e130a8079))

## [0.12.6](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.5...v0.12.6) (2026-06-01)


### Bug Fixes

* **deps:** update quarto-cli digest to 5748ba2 ([#61](https://github.com/jr200-labs/polars-hist-db/issues/61)) ([2972242](https://github.com/jr200-labs/polars-hist-db/commit/29722424eb97fe41729fdc5025adb93aac9852cb))
* evolve additive delta table columns ([#63](https://github.com/jr200-labs/polars-hist-db/issues/63)) ([56a04c1](https://github.com/jr200-labs/polars-hist-db/commit/56a04c1da5b6c1ac1c5b2e5d210600040405fcff))

## [0.12.5](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.4...v0.12.5) (2026-05-23)


### Bug Fixes

* **deps:** update quarto-cli digest to 36e2232 ([#58](https://github.com/jr200-labs/polars-hist-db/issues/58)) ([803e743](https://github.com/jr200-labs/polars-hist-db/commit/803e7430311a171049ea2fbd8e27b080e1bb09e8))
* **deps:** update quarto-cli digest to fcb70f3 ([#60](https://github.com/jr200-labs/polars-hist-db/issues/60)) ([1e6e2a1](https://github.com/jr200-labs/polars-hist-db/commit/1e6e2a177512c37e4c7c4e3319603e04a793b419))

## [0.12.4](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.3...v0.12.4) (2026-05-18)


### Bug Fixes

* **deps:** update actions/create-github-app-token action to v3.2.0 ([#56](https://github.com/jr200-labs/polars-hist-db/issues/56)) ([547de1a](https://github.com/jr200-labs/polars-hist-db/commit/547de1afec43bc4fdf190c25ffd2dbe58f9ac04e))
* **deps:** update quarto-cli digest to 8607f5e ([#55](https://github.com/jr200-labs/polars-hist-db/issues/55)) ([9825cd5](https://github.com/jr200-labs/polars-hist-db/commit/9825cd5212c5a1335ccd18006f0de90ee2174478))

## [0.12.3](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.2...v0.12.3) (2026-05-08)


### Features

* **deps:** update github-actions (major) ([#50](https://github.com/jr200-labs/polars-hist-db/issues/50)) ([7ddcc45](https://github.com/jr200-labs/polars-hist-db/commit/7ddcc45b9e8aac827d9219cee23b6663ad9733f4))

## [0.12.2](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.1...v0.12.2) (2026-05-07)


### Bug Fixes

* **deps:** update vulnerable jupyter lockfile deps ([#47](https://github.com/jr200-labs/polars-hist-db/issues/47)) ([3c68f73](https://github.com/jr200-labs/polars-hist-db/commit/3c68f73dc96cdab7d566cc11e46d27b94891cd46))

## [0.12.1](https://github.com/jr200-labs/polars-hist-db/compare/v0.12.0...v0.12.1) (2026-05-07)


### Features

* **deps:** update dependency mypy to v2 ([#41](https://github.com/jr200-labs/polars-hist-db/issues/41)) ([7fcc7da](https://github.com/jr200-labs/polars-hist-db/commit/7fcc7da5aaba03b688d7db229343d08b8f59e580))

## [0.12.0](https://github.com/jr200-labs/polars-hist-db/compare/v0.11.0...v0.12.0) (2026-04-21)


### Features

* shared lint config ([ca22aeb](https://github.com/jr200-labs/polars-hist-db/commit/ca22aeba9049f86cf0cef54467a583372579fd47))
* wire up shared lint config from github-action-templates ([99e5e08](https://github.com/jr200-labs/polars-hist-db/commit/99e5e08dfa53c6fe18359b0dea33c29b0963122b))


### Bug Fixes

* **ci:** add security-events:read permission for renovate vulnerability alerts ([f39a25f](https://github.com/jr200-labs/polars-hist-db/commit/f39a25fc0e154a0dcce93a4bc63555cc7b41ae73))
* **ci:** remove duplicate runner line from workflow_dispatch inputs ([2c8314b](https://github.com/jr200-labs/polars-hist-db/commit/2c8314bfe4ca2009b9a315ed35d734836fd5abdc))
* **ci:** use arc-linux runner label for renovate ([988f4fd](https://github.com/jr200-labs/polars-hist-db/commit/988f4fd4d868754dfea28e6e168021828f532f8d))
* **ci:** use arc-linux-jr200-labs runner for renovate ([ee55fe9](https://github.com/jr200-labs/polars-hist-db/commit/ee55fe9b6a1260efc6623352cd8f5ee341665982))
* **ci:** use arc-linux-jr200-labs runner, not private_consumer ([c1af380](https://github.com/jr200-labs/polars-hist-db/commit/c1af3806016ba6e64d5b937dbec868b325d2fb1c))
* **ci:** use arc-linux-private_consumer runner label ([2914df0](https://github.com/jr200-labs/polars-hist-db/commit/2914df075307d32592c96fa0b450a2d336029dfb))
* **deps:** update all non-major dependencies ([66ca978](https://github.com/jr200-labs/polars-hist-db/commit/66ca978a3f80c3a0a9e8e6250a3b659d3fa16451))
* **deps:** update all non-major dependencies ([122ba6b](https://github.com/jr200-labs/polars-hist-db/commit/122ba6bd6ac90b7e66f28ee4ee4e57460fe736d1))
* **renovate:** pass INTEGRATION_APP_PRIVATE_KEY explicitly across orgs ([b6bb5d7](https://github.com/jr200-labs/polars-hist-db/commit/b6bb5d79ef05f24572563fdf310c7dc59ecfa7fb))
* use factory functions for loop closures — fixes mypy and ruff B023 ([c1f94f3](https://github.com/jr200-labs/polars-hist-db/commit/c1f94f3942182010a3efe98b68d7840e374460b7))
* use semver tags for quarto-cli Renovate updates ([82cf683](https://github.com/jr200-labs/polars-hist-db/commit/82cf6830a2293ce9c22944b7db423d04ede84c69))


### Reverts

* remove incorrect security-events permission from renovate caller ([be4eac8](https://github.com/jr200-labs/polars-hist-db/commit/be4eac81f2dd4aa2c3a1895c4e1ad69aa36b71f8))
