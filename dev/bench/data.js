window.BENCHMARK_DATA = {
  "lastUpdate": 1613426303117,
  "repoUrl": "https://github.com/terminusdb/terminusdb-store",
  "entries": {
    "Rust Benchmark": [
      {
        "commit": {
          "author": {
            "email": "rderooij685@gmail.com",
            "name": "Robin de Rooij",
            "username": "rrooij"
          },
          "committer": {
            "email": "rderooij685@gmail.com",
            "name": "Robin de Rooij",
            "username": "rrooij"
          },
          "distinct": true,
          "id": "fbc665b7e34901ffb2de5a4dc693cd28a9b2ac5c",
          "message": "add benchmarking",
          "timestamp": "2021-02-15T17:53:12+01:00",
          "tree_id": "09cf9bb975962a8d677197729d3b9cfc62c70518",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/fbc665b7e34901ffb2de5a4dc693cd28a9b2ac5c"
        },
        "date": 1613410080703,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 385,
            "range": "± 45",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rderooij685@gmail.com",
            "name": "Robin de Rooij",
            "username": "rrooij"
          },
          "committer": {
            "email": "rderooij685@gmail.com",
            "name": "Robin de Rooij",
            "username": "rrooij"
          },
          "distinct": true,
          "id": "f8f7dce87eb4bc10b71a3fac4da908e45d31b921",
          "message": "Merge branch 'benchmark_test'",
          "timestamp": "2021-02-15T18:38:21+01:00",
          "tree_id": "f0390a59b8c6838097894e54486750f9c1d31cdb",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/f8f7dce87eb4bc10b71a3fac4da908e45d31b921"
        },
        "date": 1613411132481,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 348,
            "range": "± 57",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "matthijs@terminusdb.com",
            "name": "Matthijs van Otterdijk",
            "username": "matko"
          },
          "committer": {
            "email": "matthijs@terminusdb.com",
            "name": "Matthijs van Otterdijk",
            "username": "matko"
          },
          "distinct": true,
          "id": "84b898e2657e8bd2f9fea4e18e843ab5bb00701c",
          "message": "add builder benchmark tests",
          "timestamp": "2021-02-15T22:13:33+01:00",
          "tree_id": "bf9ce8b671f8587bd7a16928f8f70e4c92fba3cb",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/84b898e2657e8bd2f9fea4e18e843ab5bb00701c"
        },
        "date": 1613424196561,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 442,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 63898479,
            "range": "± 6691017",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12672234,
            "range": "± 4900615",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16194044,
            "range": "± 5110904",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 68713438,
            "range": "± 5487940",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 45796244,
            "range": "± 5709148",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "matthijs@terminusdb.com",
            "name": "Matthijs van Otterdijk",
            "username": "matko"
          },
          "committer": {
            "email": "matthijs@terminusdb.com",
            "name": "Matthijs van Otterdijk",
            "username": "matko"
          },
          "distinct": true,
          "id": "e7f85868738fcde6d451afd25a265c760070644b",
          "message": "logarray benchmarks",
          "timestamp": "2021-02-15T22:47:04+01:00",
          "tree_id": "910fcd22890406a4ccf5c4266df63980e6ba18a8",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/e7f85868738fcde6d451afd25a265c760070644b"
        },
        "date": 1613426302668,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 430,
            "range": "± 95",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 78457047,
            "range": "± 15098233",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 22347674,
            "range": "± 6343705",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 29709932,
            "range": "± 10104639",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 86283660,
            "range": "± 17803514",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 55707949,
            "range": "± 7269104",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 26819,
            "range": "± 3176",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 582,
            "range": "± 83",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 755,
            "range": "± 102",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 3077,
            "range": "± 531",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 23746,
            "range": "± 4384",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 243508,
            "range": "± 35457",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_vec",
            "value": 152009,
            "range": "± 16401",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 474,
            "range": "± 81",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}