window.BENCHMARK_DATA = {
  "lastUpdate": 1784352389880,
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
          "id": "9d1a38d0a129405b7a87c14a9fc48a79f35e9071",
          "message": "chore(master): release 0.12.56 (#228)\n\n* chore(master): release 0.12.56\n\n* chore: update release lockfile\n\n---------\n\nCo-authored-by: jr200-labs-cicd-bot[bot] <273732104+jr200-labs-cicd-bot[bot]@users.noreply.github.com>\nCo-authored-by: github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-07-18T12:41:20+09:00",
          "tree_id": "30019cec0c8ca0a32293560641f833b73b537469",
          "url": "https://github.com/jr200-labs/polars-hist-db/commit/9d1a38d0a129405b7a87c14a9fc48a79f35e9071"
        },
        "date": 1784346157047,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "delta 50000 stored / 50000 uploaded",
            "value": 0.007587450000002605,
            "unit": "seconds"
          },
          {
            "name": "delta 500000 stored / 50000 uploaded",
            "value": 0.00994167300000015,
            "unit": "seconds"
          },
          {
            "name": "delta 5000000 stored / 50000 uploaded",
            "value": 0.028109030000010193,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0 matched",
            "value": 0.7807999919999986,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6528227499999986,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 1 matched",
            "value": 0.5066392519999994,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0 matched",
            "value": 0.7868258039999887,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6453357359999927,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 1 matched",
            "value": 0.49804008899999985,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0 matched",
            "value": 0.8127532230000014,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6760736530000031,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 1 matched",
            "value": 0.5186927120000036,
            "unit": "seconds"
          }
        ]
      },
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
          "id": "1cd7db60fddc99ea221d2fffcf70170aa43ba381",
          "message": "perf: remove duplicate XTDB parent join (#229)\n\n* perf: remove duplicate XTDB parent join\n\n* test: cover normalized XTDB foreign keys",
          "timestamp": "2026-07-18T12:53:44+09:00",
          "tree_id": "ef640d04af3a7e795d30a73aa27d94a7c53ec052",
          "url": "https://github.com/jr200-labs/polars-hist-db/commit/1cd7db60fddc99ea221d2fffcf70170aa43ba381"
        },
        "date": 1784346892284,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "delta 50000 stored / 50000 uploaded",
            "value": 0.005518172999998683,
            "unit": "seconds"
          },
          {
            "name": "delta 500000 stored / 50000 uploaded",
            "value": 0.007322564000006082,
            "unit": "seconds"
          },
          {
            "name": "delta 5000000 stored / 50000 uploaded",
            "value": 0.019719303000002242,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0 matched",
            "value": 0.6007595659999936,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.493218677999991,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 1 matched",
            "value": 0.38290830900000117,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0 matched",
            "value": 0.6018306899999999,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.4929752119999904,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 1 matched",
            "value": 0.3851962349999951,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0 matched",
            "value": 0.6233067829999897,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.5196071920000094,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 1 matched",
            "value": 0.40172924899999884,
            "unit": "seconds"
          }
        ]
      },
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
          "id": "6491f8a4749d6260156ca69a58a1d856b83cbf07",
          "message": "perf: batch XTDB numeric key collision checks (#232)",
          "timestamp": "2026-07-18T13:36:32+09:00",
          "tree_id": "b126777511144dd5e73f195d5634596d78970d98",
          "url": "https://github.com/jr200-labs/polars-hist-db/commit/6491f8a4749d6260156ca69a58a1d856b83cbf07"
        },
        "date": 1784349469823,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "delta 50000 stored / 50000 uploaded",
            "value": 0.006716412000002947,
            "unit": "seconds"
          },
          {
            "name": "delta 500000 stored / 50000 uploaded",
            "value": 0.009260627999992721,
            "unit": "seconds"
          },
          {
            "name": "delta 5000000 stored / 50000 uploaded",
            "value": 0.02279743199999018,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0 matched",
            "value": 0.8644589720000084,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.683042357000005,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 1 matched",
            "value": 0.5057780399999956,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0 matched",
            "value": 0.868199611999998,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.6927653719999967,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 1 matched",
            "value": 0.5143726919999949,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0 matched",
            "value": 0.883791305999992,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.7111272629999945,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 1 matched",
            "value": 0.5314837519999998,
            "unit": "seconds"
          }
        ]
      },
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
          "id": "31556f4e4da57e3620a9f314401db5ec8f9f93b2",
          "message": "perf: vectorize XTDB foreign key payloads (#234)",
          "timestamp": "2026-07-18T14:22:55+09:00",
          "tree_id": "42fb0fd3c46c2d2a7b8a68b2277a8ac419103b44",
          "url": "https://github.com/jr200-labs/polars-hist-db/commit/31556f4e4da57e3620a9f314401db5ec8f9f93b2"
        },
        "date": 1784352247343,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "delta 50000 stored / 50000 uploaded",
            "value": 0.00656603999999561,
            "unit": "seconds"
          },
          {
            "name": "delta 500000 stored / 50000 uploaded",
            "value": 0.009161903000006077,
            "unit": "seconds"
          },
          {
            "name": "delta 5000000 stored / 50000 uploaded",
            "value": 0.02273709600001439,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0 matched",
            "value": 0.6525741029999779,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.48135388299999704,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 1 matched",
            "value": 0.30597225799999705,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0 matched",
            "value": 0.6658981380000171,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.4906119900000192,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 1 matched",
            "value": 0.3098262680000232,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0 matched",
            "value": 0.6786045959999853,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.5005241289999844,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 1 matched",
            "value": 0.3227472490000025,
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
          "id": "eafd42cc829988ac9a65b19a88e0ca9ca24f263e",
          "message": "chore(master): release 0.12.57 (#230)\n\n* chore(master): release 0.12.57\n\n* chore: update release lockfile\n\n---------\n\nCo-authored-by: jr200-labs-cicd-bot[bot] <273732104+jr200-labs-cicd-bot[bot]@users.noreply.github.com>\nCo-authored-by: github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>",
          "timestamp": "2026-07-18T14:25:15+09:00",
          "tree_id": "888815bf728622c5e37aab8ec0d0743c6d9492a4",
          "url": "https://github.com/jr200-labs/polars-hist-db/commit/eafd42cc829988ac9a65b19a88e0ca9ca24f263e"
        },
        "date": 1784352388748,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "delta 50000 stored / 50000 uploaded",
            "value": 0.006549295999974447,
            "unit": "seconds"
          },
          {
            "name": "delta 500000 stored / 50000 uploaded",
            "value": 0.009077915000005987,
            "unit": "seconds"
          },
          {
            "name": "delta 5000000 stored / 50000 uploaded",
            "value": 0.025119823000011365,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0 matched",
            "value": 0.6452561940000123,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.47835375599998997,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 50000 stored / 50000 uploaded / 1 matched",
            "value": 0.3036887140000033,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0 matched",
            "value": 0.6543754879999995,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.48967784399999914,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 500000 stored / 50000 uploaded / 1 matched",
            "value": 0.3062197830000173,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0 matched",
            "value": 0.6724964219999947,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 0.5 matched",
            "value": 0.4972429830000067,
            "unit": "seconds"
          },
          {
            "name": "foreign keys 5000000 stored / 50000 uploaded / 1 matched",
            "value": 0.3245794209999815,
            "unit": "seconds"
          }
        ]
      }
    ]
  }
}