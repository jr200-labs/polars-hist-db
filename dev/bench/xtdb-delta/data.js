window.BENCHMARK_DATA = {
  "lastUpdate": 1784113479328,
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
          "id": "20320fa5475a35cffdc4f29d5d74dbcbbca72b9a",
          "message": "chore(master): release 0.12.47 (#203)",
          "timestamp": "2026-07-15T20:03:20+09:00",
          "tree_id": "512d42cf8127f1944791b75f4a6376a049788444",
          "url": "https://github.com/jr200-labs/polars-hist-db/commit/20320fa5475a35cffdc4f29d5d74dbcbbca72b9a"
        },
        "date": 1784113478477,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "delta 50000 stored / 50000 uploaded",
            "value": 0.007543713000004004,
            "unit": "seconds"
          },
          {
            "name": "delta 500000 stored / 50000 uploaded",
            "value": 0.009289537000000792,
            "unit": "seconds"
          },
          {
            "name": "delta 5000000 stored / 50000 uploaded",
            "value": 0.023237618999999654,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0 matched",
            "value": 0.7813027389999974,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6443121579999911,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 1 matched",
            "value": 0.5070203329999998,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0 matched",
            "value": 0.7873246980000062,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6426363030000033,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 1 matched",
            "value": 0.5127586090000023,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0 matched",
            "value": 0.8025694399999992,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6756022590000015,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 1 matched",
            "value": 0.5325795339999928,
            "unit": "seconds"
          }
        ]
      }
    ]
  }
}