# Changelog

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
* **ci:** use arc-linux-jr200-labs runner, not whengas ([c1af380](https://github.com/jr200-labs/polars-hist-db/commit/c1af3806016ba6e64d5b937dbec868b325d2fb1c))
* **ci:** use arc-linux-whengas runner label ([2914df0](https://github.com/jr200-labs/polars-hist-db/commit/2914df075307d32592c96fa0b450a2d336029dfb))
* **deps:** update all non-major dependencies ([66ca978](https://github.com/jr200-labs/polars-hist-db/commit/66ca978a3f80c3a0a9e8e6250a3b659d3fa16451))
* **deps:** update all non-major dependencies ([122ba6b](https://github.com/jr200-labs/polars-hist-db/commit/122ba6bd6ac90b7e66f28ee4ee4e57460fe736d1))
* **renovate:** pass INTEGRATION_APP_PRIVATE_KEY explicitly across orgs ([b6bb5d7](https://github.com/jr200-labs/polars-hist-db/commit/b6bb5d79ef05f24572563fdf310c7dc59ecfa7fb))
* use factory functions for loop closures — fixes mypy and ruff B023 ([c1f94f3](https://github.com/jr200-labs/polars-hist-db/commit/c1f94f3942182010a3efe98b68d7840e374460b7))
* use semver tags for quarto-cli Renovate updates ([82cf683](https://github.com/jr200-labs/polars-hist-db/commit/82cf6830a2293ce9c22944b7db423d04ede84c69))


### Reverts

* remove incorrect security-events permission from renovate caller ([be4eac8](https://github.com/jr200-labs/polars-hist-db/commit/be4eac81f2dd4aa2c3a1895c4e1ad69aa36b71f8))
