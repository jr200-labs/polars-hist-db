window.BENCHMARK_DATA = {
  "lastUpdate": 1784345245099,
  "repoUrl": "https://github.com/jr200-labs/polars-hist-db",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "jr200@users.noreply.github.com",
            "name": "Jayshan Raghunandan",
            "username": "jr200"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9fa60ae83960adb073c04632a3397c27c816c086",
          "message": "perf: reduce XTDB lookup data movement (#226)",
          "timestamp": "2026-07-18T12:22:37+09:00",
          "tree_id": "9dff470202d981b45bdb541970b4de6030e6b09e",
          "url": "https://github.com/jr200-labs/polars-hist-db/commit/9fa60ae83960adb073c04632a3397c27c816c086"
        },
        "date": 1784345033036,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "delta 50000 stored / 50000 uploaded",
            "value": 0.006007741000018996,
            "unit": "seconds"
          },
          {
            "name": "delta 500000 stored / 50000 uploaded",
            "value": 0.008996892999988404,
            "unit": "seconds"
          },
          {
            "name": "delta 5000000 stored / 50000 uploaded",
            "value": 0.02664681900000687,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0 matched",
            "value": 0.592318382000002,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.48985970299997916,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 1 matched",
            "value": 0.3767290909999872,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0 matched",
            "value": 0.5929883450000091,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.492267083999991,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 1 matched",
            "value": 0.3873785039999973,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0 matched",
            "value": 0.6431489129999761,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.5303673870000125,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 1 matched",
            "value": 0.40186781700001006,
            "unit": "seconds"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "273732104+jr200-labs-cicd-bot[bot]@users.noreply.github.com",
            "name": "jr200-labs-cicd-bot[bot]",
            "username": "jr200-labs-cicd-bot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1bb7698ed80503413007744bb4966b3a0fb35d27",
          "message": "chore(master): release 0.12.55 (#225)\n\n* chore(master): release 0.12.55\n\n* chore: update release lockfile\n\n---------\n\nCo-authored-by: jr200-labs-cicd-bot[bot] <273732104+jr200-labs-cicd-bot[bot]@users.noreply.github.com>\nCo-authored-by: github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-07-18T12:26:07+09:00",
          "tree_id": "c86916a97f3e8f736e3e4c17179684a8c4f0169d",
          "url": "https://github.com/jr200-labs/polars-hist-db/commit/1bb7698ed80503413007744bb4966b3a0fb35d27"
        },
        "date": 1784345244076,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "delta 50000 stored / 50000 uploaded",
            "value": 0.006888317000004918,
            "unit": "seconds"
          },
          {
            "name": "delta 500000 stored / 50000 uploaded",
            "value": 0.009263319999988084,
            "unit": "seconds"
          },
          {
            "name": "delta 5000000 stored / 50000 uploaded",
            "value": 0.025165688000001296,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0 matched",
            "value": 0.7750319550000029,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6321264289999959,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 1 matched",
            "value": 0.495541750000001,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0 matched",
            "value": 0.7769105320000023,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6373705629999904,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 1 matched",
            "value": 0.49594977699999276,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0 matched",
            "value": 0.8060146270000104,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6683498220000104,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 1 matched",
            "value": 0.5114797570000036,
            "unit": "seconds"
          }
        ]
      }
    ]
  }
}