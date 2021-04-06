window.BENCHMARK_DATA = {
  "lastUpdate": 1617713165880,
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
          "id": "35b6626a83cbb171646e2b9bba1b6e48a76d9466",
          "message": "update readme",
          "timestamp": "2021-02-15T22:50:30+01:00",
          "tree_id": "14a18a1794249422dd141490d5e3e7f13cde429b",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/35b6626a83cbb171646e2b9bba1b6e48a76d9466"
        },
        "date": 1613426397484,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 346,
            "range": "± 75",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 61432707,
            "range": "± 13884447",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13874043,
            "range": "± 4297416",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20888381,
            "range": "± 6335289",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 66780340,
            "range": "± 10579021",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 43103284,
            "range": "± 5867412",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 22440,
            "range": "± 6490",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 478,
            "range": "± 173",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 617,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2308,
            "range": "± 307",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 19467,
            "range": "± 3150",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 190760,
            "range": "± 35016",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_vec",
            "value": 118016,
            "range": "± 21521",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 385,
            "range": "± 45",
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
          "id": "644b150d23d6421c6ee4b6fce379099de869a3c2",
          "message": "use LogArrayFileBuilder::push_vec where appropriate",
          "timestamp": "2021-02-15T23:13:05+01:00",
          "tree_id": "d457d1a6dd9b7890168252967866c23a41b563c5",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/644b150d23d6421c6ee4b6fce379099de869a3c2"
        },
        "date": 1613427832229,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 434,
            "range": "± 99",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 82085396,
            "range": "± 64158190",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12691467,
            "range": "± 5631654",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20546551,
            "range": "± 9372583",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 69636748,
            "range": "± 29782332",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 51005025,
            "range": "± 13212969",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 27625,
            "range": "± 2938",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 577,
            "range": "± 87",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 684,
            "range": "± 150",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2158,
            "range": "± 553",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15157,
            "range": "± 3663",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 148841,
            "range": "± 18672",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 234919,
            "range": "± 38749",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 479,
            "range": "± 61",
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
          "id": "cec3f64ebe8124af975ed769e2319e907c5846dd",
          "message": "add persistent logarray tests for perspective",
          "timestamp": "2021-02-15T23:49:29+01:00",
          "tree_id": "953c15b0d87d2199c34b9e6db03447e35f83d1a5",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/cec3f64ebe8124af975ed769e2319e907c5846dd"
        },
        "date": 1613429872570,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 354,
            "range": "± 84",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 51092801,
            "range": "± 3423544",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 10723534,
            "range": "± 2480843",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 11516805,
            "range": "± 4090191",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 55473701,
            "range": "± 3083780",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 35994704,
            "range": "± 2562637",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 15946,
            "range": "± 225",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 507,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 567,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1818,
            "range": "± 26",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12461,
            "range": "± 184",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 120008,
            "range": "± 1850",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 185633,
            "range": "± 1662",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 17512324,
            "range": "± 831481",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 17639590,
            "range": "± 666669",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 409,
            "range": "± 2",
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
          "id": "b7a352f68f1c9326b199801256bf1cd29f57d2df",
          "message": "bring logarray_w10_1000 in line with the rest",
          "timestamp": "2021-02-15T23:46:31+01:00",
          "tree_id": "0d0c3b6e7484d03a055372410bafd093a577bbbf",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/b7a352f68f1c9326b199801256bf1cd29f57d2df"
        },
        "date": 1613429899849,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 469,
            "range": "± 75",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 83191150,
            "range": "± 19326718",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 19999023,
            "range": "± 6477415",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 26685990,
            "range": "± 8882576",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 87392946,
            "range": "± 15817154",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 58114386,
            "range": "± 9754679",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19356,
            "range": "± 2346",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 652,
            "range": "± 111",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 752,
            "range": "± 136",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2315,
            "range": "± 458",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 16003,
            "range": "± 2602",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 158805,
            "range": "± 20693",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 250816,
            "range": "± 54623",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 507,
            "range": "± 74",
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
          "id": "968b312b71e098bba5bfc58a8a28edd8f349e941",
          "message": "use BufWriter for all persistent writes",
          "timestamp": "2021-02-16T00:20:32+01:00",
          "tree_id": "b61ce9de268837a2c16e69fb8716fb3a8a019e32",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/968b312b71e098bba5bfc58a8a28edd8f349e941"
        },
        "date": 1613432264201,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 433,
            "range": "± 51",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 30581666,
            "range": "± 9225547",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13571907,
            "range": "± 3061997",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19335620,
            "range": "± 9774584",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31692751,
            "range": "± 7232520",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31216043,
            "range": "± 4875959",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 18554,
            "range": "± 2485",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 575,
            "range": "± 77",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 659,
            "range": "± 166",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2136,
            "range": "± 421",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 14760,
            "range": "± 1584",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 144153,
            "range": "± 15828",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 217508,
            "range": "± 30119",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 970355,
            "range": "± 420265",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1067167,
            "range": "± 309566",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 477,
            "range": "± 87",
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
          "id": "bdc02fe439749e56270e6dac90809a0ddba35fbe",
          "message": "ci: dont run benchmark on pull requests",
          "timestamp": "2021-02-16T12:23:39+01:00",
          "tree_id": "5230d0c4307cdd687cf2dc42a636e36f25467e99",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/bdc02fe439749e56270e6dac90809a0ddba35fbe"
        },
        "date": 1613475206249,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 372,
            "range": "± 69",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 34205121,
            "range": "± 8537709",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16450643,
            "range": "± 5918810",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22124576,
            "range": "± 8917414",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 34432845,
            "range": "± 11645542",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31363662,
            "range": "± 7541851",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 17009,
            "range": "± 4010",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 494,
            "range": "± 101",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 579,
            "range": "± 109",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1860,
            "range": "± 360",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12909,
            "range": "± 1767",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 118594,
            "range": "± 19238",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 180517,
            "range": "± 33396",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1117097,
            "range": "± 469723",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1305789,
            "range": "± 366794",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 429,
            "range": "± 150",
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
          "id": "4da18a4f341acf2140a1cb2b1bc315b497d42ed0",
          "message": "various docstrings",
          "timestamp": "2021-02-17T15:15:41+01:00",
          "tree_id": "a6382e03764c389b9019cfb3f766f31cce97904c",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/4da18a4f341acf2140a1cb2b1bc315b497d42ed0"
        },
        "date": 1613571988859,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 430,
            "range": "± 46",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 32102360,
            "range": "± 11256335",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15411794,
            "range": "± 5940023",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22595679,
            "range": "± 13898805",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 33767549,
            "range": "± 9198112",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31468115,
            "range": "± 5754069",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 18647,
            "range": "± 4234",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 556,
            "range": "± 66",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 659,
            "range": "± 75",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2087,
            "range": "± 571",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 14701,
            "range": "± 2836",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 139687,
            "range": "± 16715",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 216686,
            "range": "± 28940",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1093773,
            "range": "± 376288",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1189382,
            "range": "± 251415",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 468,
            "range": "± 65",
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
          "id": "e11a72964aae513dcf99d4f0765ab6e65c47b4ad",
          "message": "fully document sync module",
          "timestamp": "2021-02-17T15:31:18+01:00",
          "tree_id": "17ac9849bcc4b0c1f5078a3ad323a30a0cf28fdc",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/e11a72964aae513dcf99d4f0765ab6e65c47b4ad"
        },
        "date": 1613572913328,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 437,
            "range": "± 53",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 32111822,
            "range": "± 9366490",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13537490,
            "range": "± 3756170",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22663521,
            "range": "± 12683058",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 32849475,
            "range": "± 11989898",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30860263,
            "range": "± 6381315",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 18961,
            "range": "± 2321",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 594,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 710,
            "range": "± 54",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2255,
            "range": "± 146",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 14983,
            "range": "± 4041",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 150019,
            "range": "± 8526",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 223889,
            "range": "± 20317",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 980429,
            "range": "± 436636",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1085138,
            "range": "± 340760",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 491,
            "range": "± 165",
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
          "id": "6af253433c7bec1239e15e080afcb8e462793fce",
          "message": "turn rollup_upto upto ourselves in a noop rather than crashing",
          "timestamp": "2021-02-18T14:27:20+01:00",
          "tree_id": "c1e5d370eaa1985943d3301447023a9d46944ac9",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/6af253433c7bec1239e15e080afcb8e462793fce"
        },
        "date": 1613655451061,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 447,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 29309603,
            "range": "± 8161644",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13102656,
            "range": "± 3764419",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 17253405,
            "range": "± 6726359",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 32537755,
            "range": "± 13838285",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 33234540,
            "range": "± 8353578",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19673,
            "range": "± 2363",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 623,
            "range": "± 230",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 696,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2210,
            "range": "± 114",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15271,
            "range": "± 588",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 147603,
            "range": "± 14315",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 226808,
            "range": "± 47903",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 967146,
            "range": "± 600261",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1018650,
            "range": "± 412622",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 508,
            "range": "± 62",
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
          "id": "45598c89e6dffaa2d665166aee299720a9485ae8",
          "message": "Raise crate version to 0.16.2",
          "timestamp": "2021-02-18T14:41:49+01:00",
          "tree_id": "ddc9774146b0fce69bda156c25c3d96e0dc40fce",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/45598c89e6dffaa2d665166aee299720a9485ae8"
        },
        "date": 1613656348446,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 420,
            "range": "± 58",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 36359415,
            "range": "± 14741501",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 18519006,
            "range": "± 6805814",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 24433383,
            "range": "± 8573922",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36749434,
            "range": "± 7948821",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 39768632,
            "range": "± 12050083",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19120,
            "range": "± 1430",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 604,
            "range": "± 26",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 680,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2186,
            "range": "± 154",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 14887,
            "range": "± 740",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 144199,
            "range": "± 837",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 223279,
            "range": "± 10064",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1339474,
            "range": "± 665220",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1328094,
            "range": "± 411694",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 514,
            "range": "± 75",
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
          "id": "45598c89e6dffaa2d665166aee299720a9485ae8",
          "message": "Raise crate version to 0.16.2",
          "timestamp": "2021-02-18T14:41:49+01:00",
          "tree_id": "ddc9774146b0fce69bda156c25c3d96e0dc40fce",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/45598c89e6dffaa2d665166aee299720a9485ae8"
        },
        "date": 1613656743209,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 472,
            "range": "± 91",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 29790545,
            "range": "± 10617846",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12830391,
            "range": "± 5275301",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 17480350,
            "range": "± 4511988",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31747684,
            "range": "± 10529672",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31006931,
            "range": "± 5362291",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19473,
            "range": "± 2852",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 618,
            "range": "± 188",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 699,
            "range": "± 69",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2215,
            "range": "± 187",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15112,
            "range": "± 2465",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 145110,
            "range": "± 7600",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 221374,
            "range": "± 89366",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 968909,
            "range": "± 519651",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1046939,
            "range": "± 433982",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 511,
            "range": "± 38",
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
          "id": "96483cc719a7f92e6f3e17c68c5c5db1513625af",
          "message": "remove accidental debug print",
          "timestamp": "2021-02-18T15:57:23+01:00",
          "tree_id": "00057f13c15997ec753ff0ed7c433a7868eb1e12",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/96483cc719a7f92e6f3e17c68c5c5db1513625af"
        },
        "date": 1613660939526,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 469,
            "range": "± 51",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 35429684,
            "range": "± 11333265",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16980843,
            "range": "± 6235375",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 21653277,
            "range": "± 9876610",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 38060046,
            "range": "± 11334836",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 35290114,
            "range": "± 7914989",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 20467,
            "range": "± 1716",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 670,
            "range": "± 87",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 771,
            "range": "± 120",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2358,
            "range": "± 151",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15851,
            "range": "± 1662",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 154510,
            "range": "± 12748",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 233940,
            "range": "± 30454",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1134311,
            "range": "± 314984",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1250891,
            "range": "± 342227",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 566,
            "range": "± 98",
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
          "id": "5e9d1641e1ec2a648c5d73ebd7040a2741d43107",
          "message": "exclude rollup file from exports",
          "timestamp": "2021-02-25T14:33:13+01:00",
          "tree_id": "c231876b6c943b549d745260f09665d61c7c8311",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/5e9d1641e1ec2a648c5d73ebd7040a2741d43107"
        },
        "date": 1614260598420,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 443,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 33956060,
            "range": "± 6052727",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16280259,
            "range": "± 5122928",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20448399,
            "range": "± 7626533",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36366538,
            "range": "± 7865128",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 36872995,
            "range": "± 7816972",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19489,
            "range": "± 2836",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 602,
            "range": "± 54",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 676,
            "range": "± 211",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2165,
            "range": "± 516",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 14910,
            "range": "± 251",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 144019,
            "range": "± 930",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 220480,
            "range": "± 1892",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1144405,
            "range": "± 411688",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1168476,
            "range": "± 437900",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 491,
            "range": "± 63",
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
          "id": "6cc60f94f4b56bc7c9f2660be99f3084dca4176d",
          "message": "raise crate version to 0.16.3",
          "timestamp": "2021-02-18T15:57:44+01:00",
          "tree_id": "b66fb43f0f4212d99d18795832ea3cd700ee5423",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/6cc60f94f4b56bc7c9f2660be99f3084dca4176d"
        },
        "date": 1614261250568,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 352,
            "range": "± 56",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 23626089,
            "range": "± 7261706",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 10116965,
            "range": "± 1905399",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 14360708,
            "range": "± 5473850",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 25943933,
            "range": "± 4792425",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 24803901,
            "range": "± 2883404",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 16036,
            "range": "± 181",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 505,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 569,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1824,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12389,
            "range": "± 171",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 120443,
            "range": "± 1805",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 182650,
            "range": "± 747",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 935704,
            "range": "± 672328",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 922053,
            "range": "± 589342",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 413,
            "range": "± 2",
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
          "id": "b8f85be87c2a629b47d30919759e5920b4ed39db",
          "message": "raise crate version to 0.16.4",
          "timestamp": "2021-02-25T14:44:24+01:00",
          "tree_id": "95498e6a7c6a82a7e3ca917ea2a59b38a77a6ea7",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/b8f85be87c2a629b47d30919759e5920b4ed39db"
        },
        "date": 1614261358900,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 405,
            "range": "± 82",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 42023769,
            "range": "± 9649747",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 21747741,
            "range": "± 9117352",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 26760671,
            "range": "± 7528601",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 40753484,
            "range": "± 7954565",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 41865960,
            "range": "± 12332963",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 16270,
            "range": "± 3436",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 529,
            "range": "± 117",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 596,
            "range": "± 100",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2027,
            "range": "± 571",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13038,
            "range": "± 2580",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 137733,
            "range": "± 25501",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 195196,
            "range": "± 67760",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1093748,
            "range": "± 648617",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1518728,
            "range": "± 679446",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 446,
            "range": "± 58",
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
          "id": "b8f85be87c2a629b47d30919759e5920b4ed39db",
          "message": "raise crate version to 0.16.4",
          "timestamp": "2021-02-25T14:44:24+01:00",
          "tree_id": "95498e6a7c6a82a7e3ca917ea2a59b38a77a6ea7",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/b8f85be87c2a629b47d30919759e5920b4ed39db"
        },
        "date": 1614261404816,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 438,
            "range": "± 69",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 36706034,
            "range": "± 6598269",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 20609882,
            "range": "± 11045611",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 24493642,
            "range": "± 7720268",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 39622033,
            "range": "± 10418717",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 35435548,
            "range": "± 7448222",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 18781,
            "range": "± 2230",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 576,
            "range": "± 88",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 668,
            "range": "± 85",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2075,
            "range": "± 283",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 14502,
            "range": "± 1940",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 139291,
            "range": "± 18575",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 208068,
            "range": "± 24568",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1331108,
            "range": "± 424925",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1472251,
            "range": "± 426735",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 478,
            "range": "± 60",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "cheukting.ho@gmail.com",
            "name": "Cheuk Ting Ho",
            "username": "Cheukting"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "799bf928a8d96a53c3dc1ba4f623365cae314c63",
          "message": "Adding auto triage label",
          "timestamp": "2021-03-08T22:32:01Z",
          "tree_id": "3b23e288c3823f3b3d83037154ed81685fd4b911",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/799bf928a8d96a53c3dc1ba4f623365cae314c63"
        },
        "date": 1615243297095,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 430,
            "range": "± 65",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 38813999,
            "range": "± 7421602",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 21342323,
            "range": "± 10344422",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 24799349,
            "range": "± 6321418",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 40334033,
            "range": "± 9729246",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 39126790,
            "range": "± 7697246",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19642,
            "range": "± 3225",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 582,
            "range": "± 75",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 713,
            "range": "± 134",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2159,
            "range": "± 393",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 14912,
            "range": "± 3087",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 147464,
            "range": "± 17925",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 222620,
            "range": "± 81257",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1463709,
            "range": "± 706014",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1707570,
            "range": "± 570912",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 491,
            "range": "± 54",
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
          "id": "a54bf1c217afb399b4bfeac658c55670040f40fb",
          "message": "retrieve_layer_stack_names_upto implementation",
          "timestamp": "2021-03-24T14:16:10+01:00",
          "tree_id": "f7cccb40fe6421c0a165557a0f2fba3dc20f8ce4",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/a54bf1c217afb399b4bfeac658c55670040f40fb"
        },
        "date": 1616592410560,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 441,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 35874255,
            "range": "± 13163949",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 20421135,
            "range": "± 8210098",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 26589257,
            "range": "± 9320330",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 40249431,
            "range": "± 9015832",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 36741765,
            "range": "± 11060246",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 20239,
            "range": "± 5105",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 596,
            "range": "± 105",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 695,
            "range": "± 145",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2290,
            "range": "± 346",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15650,
            "range": "± 2379",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 148375,
            "range": "± 19102",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 224764,
            "range": "± 49217",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1365964,
            "range": "± 701459",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1400791,
            "range": "± 572966",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 497,
            "range": "± 77",
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
          "id": "c0e976b19d1bd7b34e25ac3c9a2831b03802ea10",
          "message": "function for determining a safe bound for in-memory rollup",
          "timestamp": "2021-03-24T15:08:07+01:00",
          "tree_id": "9b65486fc9cbc38dbd4058e0d856023dea19ae57",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/c0e976b19d1bd7b34e25ac3c9a2831b03802ea10"
        },
        "date": 1616595415294,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 434,
            "range": "± 64",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 27270367,
            "range": "± 5670561",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13176227,
            "range": "± 4149676",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16846248,
            "range": "± 6905335",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30640257,
            "range": "± 7547218",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28453205,
            "range": "± 6693845",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 20813,
            "range": "± 3428",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 578,
            "range": "± 70",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 671,
            "range": "± 95",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2213,
            "range": "± 364",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 16334,
            "range": "± 2636",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 147065,
            "range": "± 55237",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 222855,
            "range": "± 23617",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 941588,
            "range": "± 424454",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1033118,
            "range": "± 263452",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 476,
            "range": "± 67",
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
          "id": "f69b5a73e83e8e5b03e4571ae5238f5fe8202c23",
          "message": "imprecise rollup upto implementation",
          "timestamp": "2021-03-24T15:11:35+01:00",
          "tree_id": "3b4ca432a3f066aa8354ecf27cdc69525004a0f2",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/f69b5a73e83e8e5b03e4571ae5238f5fe8202c23"
        },
        "date": 1616595802101,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 469,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 38211197,
            "range": "± 10398013",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 18956029,
            "range": "± 5521144",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 23746790,
            "range": "± 9792175",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 41379461,
            "range": "± 11030890",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 37390296,
            "range": "± 11175825",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 20256,
            "range": "± 1631",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 653,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 728,
            "range": "± 111",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2389,
            "range": "± 215",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 16197,
            "range": "± 1516",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 152096,
            "range": "± 20449",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 228691,
            "range": "± 22270",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1240592,
            "range": "± 508675",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1366196,
            "range": "± 903882",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 549,
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
          "id": "32708fa8c925f1bd824d6d76f23438dd5ac008bc",
          "message": "big reformat",
          "timestamp": "2021-03-24T17:19:08+01:00",
          "tree_id": "08acf178e561aa2b09bfceb7a708f3a85a41031a",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/32708fa8c925f1bd824d6d76f23438dd5ac008bc"
        },
        "date": 1616603314063,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 424,
            "range": "± 69",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 29123200,
            "range": "± 5927175",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12587524,
            "range": "± 4578695",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18808114,
            "range": "± 7358390",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30769793,
            "range": "± 6743682",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 29486768,
            "range": "± 7874375",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 20125,
            "range": "± 2675",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 564,
            "range": "± 54",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 652,
            "range": "± 100",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2227,
            "range": "± 244",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15341,
            "range": "± 3112",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 144222,
            "range": "± 29736",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 225097,
            "range": "± 32644",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 958345,
            "range": "± 382347",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1067017,
            "range": "± 285432",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 477,
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
          "id": "425be907aa9dfcf1700a9aa8878a4daa14447283",
          "message": "retrieve layer parent name through storage without loading the layer",
          "timestamp": "2021-03-29T14:55:02+02:00",
          "tree_id": "88316d7b5c5f6fac9108612bfb8e7ee690d52856",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/425be907aa9dfcf1700a9aa8878a4daa14447283"
        },
        "date": 1617023026495,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 438,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 32898797,
            "range": "± 6481217",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14678755,
            "range": "± 6326018",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 21282325,
            "range": "± 9282966",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 34133686,
            "range": "± 7684587",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31433370,
            "range": "± 6057060",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19254,
            "range": "± 318",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 607,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 693,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2224,
            "range": "± 100",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15513,
            "range": "± 335",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 147899,
            "range": "± 5152",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 228897,
            "range": "± 6801",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1122433,
            "range": "± 563416",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1117045,
            "range": "± 366827",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 498,
            "range": "± 3",
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
          "id": "23ea34974737011bbb44b4b1ae83c8fd68fd4738",
          "message": "merge dictionaries from disk+memory",
          "timestamp": "2021-03-30T12:41:51+02:00",
          "tree_id": "fc1ffecdb577dffc8f37e439e61b3f5779abfd8c",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/23ea34974737011bbb44b4b1ae83c8fd68fd4738"
        },
        "date": 1617101477412,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 418,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 36889646,
            "range": "± 8939022",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 17907684,
            "range": "± 5835211",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20639591,
            "range": "± 7787974",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 37554094,
            "range": "± 12191713",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 34708799,
            "range": "± 6857549",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 18433,
            "range": "± 2244",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 620,
            "range": "± 86",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 690,
            "range": "± 61",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2147,
            "range": "± 312",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 14800,
            "range": "± 2815",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 146438,
            "range": "± 17506",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 224671,
            "range": "± 20350",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1344453,
            "range": "± 247333",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1438966,
            "range": "± 233932",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 473,
            "range": "± 43",
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
          "id": "53b036c30bce8bb4318ba98bb643de28a7a6812a",
          "message": "refactor layer store trait to return inner interators",
          "timestamp": "2021-03-30T14:15:17+02:00",
          "tree_id": "41dfd85b38a790db2650f32054789c63d8a7bbde",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/53b036c30bce8bb4318ba98bb643de28a7a6812a"
        },
        "date": 1617107044804,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 429,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 31226003,
            "range": "± 6813567",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15538702,
            "range": "± 5498587",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 21929232,
            "range": "± 8083432",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 34565952,
            "range": "± 11263833",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 43517541,
            "range": "± 35410663",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19441,
            "range": "± 377",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 617,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 709,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2182,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15389,
            "range": "± 189",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 147499,
            "range": "± 2816",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 225498,
            "range": "± 3683",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1029464,
            "range": "± 481241",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1066275,
            "range": "± 524495",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 505,
            "range": "± 31",
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
          "id": "3ffffd1fee6c07e155a4e7acc68123502fa1a3c4",
          "message": "get triple additions and removals directly from storage",
          "timestamp": "2021-03-31T07:31:40+02:00",
          "tree_id": "053308b34ef2ab49107c466fa4d776eacca60305",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/3ffffd1fee6c07e155a4e7acc68123502fa1a3c4"
        },
        "date": 1617169276246,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 424,
            "range": "± 69",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 37828969,
            "range": "± 8173653",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 20068163,
            "range": "± 12521418",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 26050121,
            "range": "± 11783506",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 39906933,
            "range": "± 13706527",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 36899377,
            "range": "± 8698397",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 18961,
            "range": "± 2103",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 587,
            "range": "± 72",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 673,
            "range": "± 117",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2206,
            "range": "± 411",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15525,
            "range": "± 1503",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 150986,
            "range": "± 19772",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 224110,
            "range": "± 21968",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1353486,
            "range": "± 468751",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1430928,
            "range": "± 455080",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 475,
            "range": "± 53",
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
          "id": "21c5732ddf5a15a846cd10a26c07327e02a71c62",
          "message": "finished reimplementation of delta_rollup_upto",
          "timestamp": "2021-03-31T08:17:17+02:00",
          "tree_id": "daf7cf253df046dfcaa1189706413db414477814",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/21c5732ddf5a15a846cd10a26c07327e02a71c62"
        },
        "date": 1617172057668,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 455,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 40692097,
            "range": "± 7435714",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 20314313,
            "range": "± 8517442",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 25748252,
            "range": "± 10545872",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 42218658,
            "range": "± 6244757",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 38561593,
            "range": "± 8791109",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 20143,
            "range": "± 2286",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 633,
            "range": "± 73",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 720,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2309,
            "range": "± 121",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 16901,
            "range": "± 1935",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 162271,
            "range": "± 16179",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 235053,
            "range": "± 24448",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1581793,
            "range": "± 862292",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1624373,
            "range": "± 543439",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 508,
            "range": "± 59",
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
          "id": "21c5732ddf5a15a846cd10a26c07327e02a71c62",
          "message": "finished reimplementation of delta_rollup_upto",
          "timestamp": "2021-03-31T08:17:17+02:00",
          "tree_id": "daf7cf253df046dfcaa1189706413db414477814",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/21c5732ddf5a15a846cd10a26c07327e02a71c62"
        },
        "date": 1617172789449,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 417,
            "range": "± 84",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 29223236,
            "range": "± 7018689",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14049809,
            "range": "± 5618466",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18824648,
            "range": "± 7155204",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31547790,
            "range": "± 5854652",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30233305,
            "range": "± 10253198",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19139,
            "range": "± 114",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 614,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 701,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2207,
            "range": "± 24",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15180,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 144489,
            "range": "± 1358",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 224755,
            "range": "± 4996",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 983745,
            "range": "± 631493",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1067835,
            "range": "± 441806",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 503,
            "range": "± 4",
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
          "id": "fe00c2817f2258cc3a55a405d238717d458a1e2e",
          "message": "remove open_write_from",
          "timestamp": "2021-03-31T09:09:44+02:00",
          "tree_id": "0004cf7bd700992c560323fdbd047d8c1fce38e7",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/fe00c2817f2258cc3a55a405d238717d458a1e2e"
        },
        "date": 1617175094318,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 357,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 31818738,
            "range": "± 7464322",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16573462,
            "range": "± 6520205",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22061784,
            "range": "± 10624592",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31394107,
            "range": "± 7455507",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30455301,
            "range": "± 6690811",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 16252,
            "range": "± 8125",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 460,
            "range": "± 114",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 558,
            "range": "± 171",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1714,
            "range": "± 873",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12483,
            "range": "± 4195",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 156207,
            "range": "± 40289",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 232143,
            "range": "± 73629",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1076063,
            "range": "± 387908",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1158332,
            "range": "± 350192",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 536,
            "range": "± 120",
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
          "id": "340ab53b3378a8728b70108f35c67950298f47f6",
          "message": "starting on a new in-memory file representation in a fresh file",
          "timestamp": "2021-03-31T09:44:36+02:00",
          "tree_id": "9a9be28ed1457d95fb69698a76749ceb8c7f2325",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/340ab53b3378a8728b70108f35c67950298f47f6"
        },
        "date": 1617177283697,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 421,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 74100090,
            "range": "± 68366609",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 43249812,
            "range": "± 28094649",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 49598184,
            "range": "± 24843398",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 70973763,
            "range": "± 36210259",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 66013499,
            "range": "± 35309732",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19375,
            "range": "± 1531",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 606,
            "range": "± 82",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 697,
            "range": "± 143",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2147,
            "range": "± 293",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15136,
            "range": "± 2318",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 144341,
            "range": "± 23111",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 219812,
            "range": "± 30939",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 2685618,
            "range": "± 2943006",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 2665446,
            "range": "± 3294515",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 499,
            "range": "± 85",
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
          "id": "f0309590d8c89d911a7e6da88f98782e23b51cfa",
          "message": "finished implementing new memory store minus export/import",
          "timestamp": "2021-03-31T10:09:38+02:00",
          "tree_id": "3fbb1f0ec5c7972d4b035dd7a1859018dbd9d283",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/f0309590d8c89d911a7e6da88f98782e23b51cfa"
        },
        "date": 1617178699648,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 347,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 36446434,
            "range": "± 12650099",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 21898824,
            "range": "± 8847197",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 28048177,
            "range": "± 11449421",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 39517510,
            "range": "± 7530355",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 32343191,
            "range": "± 9879556",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 17811,
            "range": "± 3141",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 549,
            "range": "± 124",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 565,
            "range": "± 160",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1918,
            "range": "± 482",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13571,
            "range": "± 2544",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 141474,
            "range": "± 32223",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 202077,
            "range": "± 35496",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1227128,
            "range": "± 494303",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1268818,
            "range": "± 246991",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 419,
            "range": "± 285",
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
          "id": "5a380b76b81df7e8ebacd9fa2dad75c54f54c1ae",
          "message": "replace existing memory layer store implementation with new one",
          "timestamp": "2021-03-31T10:31:33+02:00",
          "tree_id": "ecbbf2cd6005c043ec291b2be89fba9339d77190",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/5a380b76b81df7e8ebacd9fa2dad75c54f54c1ae"
        },
        "date": 1617179997009,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 403,
            "range": "± 202",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 26973627,
            "range": "± 4805529",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 10737798,
            "range": "± 3108305",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 15197321,
            "range": "± 6058242",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 27166887,
            "range": "± 4910506",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 27309071,
            "range": "± 5194842",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13955,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 498,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 588,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1917,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12702,
            "range": "± 315",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 119683,
            "range": "± 6200",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 198527,
            "range": "± 3986",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1001602,
            "range": "± 748444",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 955571,
            "range": "± 690029",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 372,
            "range": "± 75",
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
          "id": "b4caa8fb953a8f77d1ae4b8cc796ecd7adde99d7",
          "message": "Merge branch 'delta_rollup_fix' into memory_store_replacement",
          "timestamp": "2021-03-31T10:32:20+02:00",
          "tree_id": "08eb70387b924d0ed43171867471b69380932a10",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/b4caa8fb953a8f77d1ae4b8cc796ecd7adde99d7"
        },
        "date": 1617180058376,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 428,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 31373483,
            "range": "± 7574432",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14961506,
            "range": "± 5215169",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20674006,
            "range": "± 9198164",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 32181923,
            "range": "± 11486368",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 32408929,
            "range": "± 7392812",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13930,
            "range": "± 2719",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 504,
            "range": "± 90",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 591,
            "range": "± 192",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1924,
            "range": "± 408",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12519,
            "range": "± 392",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 118648,
            "range": "± 6129",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 198975,
            "range": "± 6774",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1019267,
            "range": "± 472439",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1189449,
            "range": "± 461438",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 426,
            "range": "± 32",
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
          "id": "94b1b7f1399c86f45b3d05fb4798ff121ab14893",
          "message": "remove accidental debug prints",
          "timestamp": "2021-03-31T10:32:07+02:00",
          "tree_id": "416f72bcd80cfd23fdca604d721c7f4b9c82aa99",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/94b1b7f1399c86f45b3d05fb4798ff121ab14893"
        },
        "date": 1617180148821,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 462,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 35465028,
            "range": "± 6597818",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 17239355,
            "range": "± 5919457",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22537629,
            "range": "± 6796072",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36915498,
            "range": "± 9482351",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 34042495,
            "range": "± 7122735",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 20669,
            "range": "± 2624",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 626,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 721,
            "range": "± 87",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2285,
            "range": "± 269",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15992,
            "range": "± 1610",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 156494,
            "range": "± 15335",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 237505,
            "range": "± 21628",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1190380,
            "range": "± 330154",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1259748,
            "range": "± 554309",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 510,
            "range": "± 49",
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
          "id": "6e2764b38567e324ba97499504aacf9e48667574",
          "message": "delete commented code",
          "timestamp": "2021-04-01T12:36:52+02:00",
          "tree_id": "573a86279dee4b3babd0c6310624fca3104f1903",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/6e2764b38567e324ba97499504aacf9e48667574"
        },
        "date": 1617273951006,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 434,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 31907943,
            "range": "± 9023977",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15518073,
            "range": "± 5011118",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22305807,
            "range": "± 8472464",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 32238814,
            "range": "± 7915305",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 32894416,
            "range": "± 6573903",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 19222,
            "range": "± 1001",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 604,
            "range": "± 33",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 686,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2208,
            "range": "± 144",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 15392,
            "range": "± 688",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 145916,
            "range": "± 5073",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 226027,
            "range": "± 8603",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1183952,
            "range": "± 447766",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1245537,
            "range": "± 556335",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 505,
            "range": "± 51",
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
          "id": "d37150f8ba7498128996bfba447f90076c88c00b",
          "message": "expose imprecise rollup through the layerstore trait",
          "timestamp": "2021-04-01T12:54:31+02:00",
          "tree_id": "952febbf8871cbf9ba5e777448d7bda3284e1a58",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/d37150f8ba7498128996bfba447f90076c88c00b"
        },
        "date": 1617275028552,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 452,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 32527387,
            "range": "± 6704932",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15390432,
            "range": "± 8979385",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20823925,
            "range": "± 4813312",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 35605434,
            "range": "± 9577233",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 35888486,
            "range": "± 7311855",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 20249,
            "range": "± 1394",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 634,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 733,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2304,
            "range": "± 450",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 16511,
            "range": "± 1766",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 155306,
            "range": "± 8167",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 292893,
            "range": "± 70202",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1207718,
            "range": "± 518410",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1205211,
            "range": "± 404280",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 524,
            "range": "± 4",
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
          "id": "855bc810291734114095a976b56750be67887c65",
          "message": "fix implementation of file_exists for memorystore",
          "timestamp": "2021-04-01T13:19:36+02:00",
          "tree_id": "841eea88107a0a1957e53bd26097d90bad448ec7",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/855bc810291734114095a976b56750be67887c65"
        },
        "date": 1617276571817,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 445,
            "range": "± 79",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 40982243,
            "range": "± 8701220",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 18841564,
            "range": "± 7228636",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 25255153,
            "range": "± 8509517",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 42676415,
            "range": "± 6335546",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 39794492,
            "range": "± 7582129",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13970,
            "range": "± 4379",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 469,
            "range": "± 126",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 610,
            "range": "± 165",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1872,
            "range": "± 322",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11751,
            "range": "± 2565",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 111918,
            "range": "± 21922",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 176483,
            "range": "± 43057",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1389888,
            "range": "± 634172",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1538114,
            "range": "± 809022",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 399,
            "range": "± 81",
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
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "38ec5d68736edf31644fef0659057d5dc0ca839b",
          "message": "Merge pull request #55 from terminusdb/delta_rollup_fix\n\nDelta rollup reimplementation which will load information from disk when required due to rollups",
          "timestamp": "2021-04-01T13:24:12+02:00",
          "tree_id": "fc606d5cf0802110141d6d5911a72dacda34a0d0",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/38ec5d68736edf31644fef0659057d5dc0ca839b"
        },
        "date": 1617276819784,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 444,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 34938650,
            "range": "± 10044256",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13937603,
            "range": "± 4407391",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18521218,
            "range": "± 6132329",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36573081,
            "range": "± 6631622",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 33453256,
            "range": "± 9190821",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 22565,
            "range": "± 754",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 631,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 723,
            "range": "± 89",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2343,
            "range": "± 119",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 17193,
            "range": "± 487",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 165390,
            "range": "± 7865",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 234823,
            "range": "± 16365",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 927639,
            "range": "± 222396",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1063096,
            "range": "± 336355",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 525,
            "range": "± 35",
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
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5ca661b439e892e9212e5dad72d3da0e946e69cf",
          "message": "Merge pull request #57 from terminusdb/memory_store_replacement\n\nMemory store replacement",
          "timestamp": "2021-04-01T13:48:14+02:00",
          "tree_id": "68eaabe247aff9102272bbb3eec3a486ce454398",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/5ca661b439e892e9212e5dad72d3da0e946e69cf"
        },
        "date": 1617278206837,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 394,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 29085873,
            "range": "± 7495538",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13668000,
            "range": "± 7097984",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18733104,
            "range": "± 7310795",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 29844093,
            "range": "± 5685816",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30721144,
            "range": "± 5856774",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12741,
            "range": "± 2589",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 446,
            "range": "± 51",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 542,
            "range": "± 103",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1687,
            "range": "± 207",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10781,
            "range": "± 1141",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 105506,
            "range": "± 11114",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 173050,
            "range": "± 22372",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1156023,
            "range": "± 557433",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1155591,
            "range": "± 473048",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 404,
            "range": "± 67",
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
          "id": "e556781163f123cf8d308e9e3072dd587c600473",
          "message": "failing tests",
          "timestamp": "2021-04-01T16:22:29+02:00",
          "tree_id": "8f9badcbe110d192bfca7bd775b0e54aead2e5cd",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/e556781163f123cf8d308e9e3072dd587c600473"
        },
        "date": 1617287474590,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 418,
            "range": "± 92",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 30824408,
            "range": "± 8623455",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15672752,
            "range": "± 5671139",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18729923,
            "range": "± 8276919",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 34886215,
            "range": "± 6519972",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 32920974,
            "range": "± 11028574",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13776,
            "range": "± 355",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 515,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 601,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1929,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12363,
            "range": "± 144",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 115907,
            "range": "± 1151",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 197343,
            "range": "± 6036",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1143305,
            "range": "± 382130",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1193445,
            "range": "± 430878",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 442,
            "range": "± 17",
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
          "id": "b81f482b62e360a92fd22737b71c38ddf540e846",
          "message": "reimplement idmap construction using underlying structures",
          "timestamp": "2021-04-06T12:56:57+02:00",
          "tree_id": "713c10839cd9468731f90d07d59bd4adf8254bb3",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/b81f482b62e360a92fd22737b71c38ddf540e846"
        },
        "date": 1617707171949,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 424,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 26613773,
            "range": "± 6173631",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12096509,
            "range": "± 2879390",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16696355,
            "range": "± 4307435",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 28867967,
            "range": "± 5144759",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28325885,
            "range": "± 4016349",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14190,
            "range": "± 971",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 496,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 586,
            "range": "± 86",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1949,
            "range": "± 193",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12768,
            "range": "± 2028",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 121453,
            "range": "± 4884",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 196949,
            "range": "± 18844",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 915016,
            "range": "± 225901",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 959014,
            "range": "± 156392",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 417,
            "range": "± 68",
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
          "id": "5457716f56862ed1e9d038cffe5af2cbe4a49bc0",
          "message": "reimplement idmap construction using underlying structures",
          "timestamp": "2021-04-06T13:09:24+02:00",
          "tree_id": "de50a1b9f2c06dd31c886a5b32e181926d287faa",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/5457716f56862ed1e9d038cffe5af2cbe4a49bc0"
        },
        "date": 1617711029471,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 434,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 24141926,
            "range": "± 3990847",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12183619,
            "range": "± 2651387",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 13337147,
            "range": "± 5274388",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 25399419,
            "range": "± 3648098",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 26724915,
            "range": "± 5412598",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13779,
            "range": "± 1159",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 496,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 585,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1900,
            "range": "± 106",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12729,
            "range": "± 760",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 120597,
            "range": "± 2510",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 200122,
            "range": "± 12236",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 741232,
            "range": "± 140533",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 808835,
            "range": "± 231100",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 432,
            "range": "± 86",
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
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5ca661b439e892e9212e5dad72d3da0e946e69cf",
          "message": "Merge pull request #57 from terminusdb/memory_store_replacement\n\nMemory store replacement",
          "timestamp": "2021-04-01T13:48:14+02:00",
          "tree_id": "68eaabe247aff9102272bbb3eec3a486ce454398",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/5ca661b439e892e9212e5dad72d3da0e946e69cf"
        },
        "date": 1617711084160,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 420,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 30821496,
            "range": "± 5908775",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15643711,
            "range": "± 3062585",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19362836,
            "range": "± 5482974",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31667308,
            "range": "± 6818661",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31124937,
            "range": "± 5597672",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13516,
            "range": "± 1081",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 492,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 560,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1882,
            "range": "± 74",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12773,
            "range": "± 510",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 118415,
            "range": "± 5901",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 193074,
            "range": "± 9601",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1105976,
            "range": "± 344235",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1233519,
            "range": "± 524953",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 401,
            "range": "± 24",
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
          "id": "988a53eb5e363a7e3aa59a65163ed59801fd29f4",
          "message": "implement retrieval of idmaps from disk",
          "timestamp": "2021-04-06T14:37:12+02:00",
          "tree_id": "49037dcbe7dc081d31a88c96b59054963af6c557",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/988a53eb5e363a7e3aa59a65163ed59801fd29f4"
        },
        "date": 1617713164984,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 408,
            "range": "± 34",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 30110823,
            "range": "± 6327232",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14378019,
            "range": "± 5859334",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20280480,
            "range": "± 8663895",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 29866173,
            "range": "± 6225126",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 29635933,
            "range": "± 8109793",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13795,
            "range": "± 1165",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 442,
            "range": "± 66",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 475,
            "range": "± 109",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1728,
            "range": "± 318",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11812,
            "range": "± 1202",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 114105,
            "range": "± 8475",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 183386,
            "range": "± 15902",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1039046,
            "range": "± 429739",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1192385,
            "range": "± 421484",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 392,
            "range": "± 46",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}