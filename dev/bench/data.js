window.BENCHMARK_DATA = {
  "lastUpdate": 1613475206709,
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
      }
    ]
  }
}