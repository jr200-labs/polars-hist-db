window.BENCHMARK_DATA = {
  "lastUpdate": 1784343117719,
  "repoUrl": "https://github.com/jr200-labs/polars-hist-db",
  "entries": {
    "Benchmark": [
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
          "id": "287ece4fd6b056f2c794e61375d3d934e77bcc73",
          "message": "fix(deps): update all non-major dependencies (#221)\n\nCo-authored-by: jr200-labs-cicd-bot[bot] <273732104+jr200-labs-cicd-bot[bot]@users.noreply.github.com>",
          "timestamp": "2026-07-18T11:50:18+09:00",
          "tree_id": "025b6345c110f7047e0a8fdd5519e0d9c5e82ca0",
          "url": "https://github.com/jr200-labs/polars-hist-db/commit/287ece4fd6b056f2c794e61375d3d934e77bcc73"
        },
        "date": 1784343116386,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "delta 50000 stored / 50000 uploaded",
            "value": 0.006488126999997235,
            "unit": "seconds"
          },
          {
            "name": "delta 500000 stored / 50000 uploaded",
            "value": 0.009187736999990648,
            "unit": "seconds"
          },
          {
            "name": "delta 5000000 stored / 50000 uploaded",
            "value": 0.026203716999987137,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0 matched",
            "value": 0.7752480789999936,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6361382689999999,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 1 matched",
            "value": 0.49578616399999476,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0 matched",
            "value": 0.7778213769999951,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6375672690000016,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 1 matched",
            "value": 0.5005818970000036,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0 matched",
            "value": 0.8028589940000046,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6664692699999932,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 1 matched",
            "value": 0.5123772350000024,
            "unit": "seconds"
          }
        ]
      }
    ]
  }
}