window.BENCHMARK_DATA = {
  "lastUpdate": 1694770829878,
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
          "id": "8a35f307162e3cc8f2bc8354a69705844cf506cb",
          "message": "delta rollup no longer crashes when encountering a rollup",
          "timestamp": "2021-04-06T16:07:17+02:00",
          "tree_id": "bb91f304d8cea1a21b129fe8e527836b502e2977",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/8a35f307162e3cc8f2bc8354a69705844cf506cb"
        },
        "date": 1617718580172,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 368,
            "range": "± 26",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 30434162,
            "range": "± 5900826",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14136100,
            "range": "± 6526650",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20042103,
            "range": "± 7559411",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31178447,
            "range": "± 5631530",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28587772,
            "range": "± 4994909",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11623,
            "range": "± 2924",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 436,
            "range": "± 170",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 497,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1585,
            "range": "± 233",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10632,
            "range": "± 1219",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 104790,
            "range": "± 18452",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 162571,
            "range": "± 15385",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 998735,
            "range": "± 205927",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1093448,
            "range": "± 247823",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 342,
            "range": "± 82",
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
          "id": "68ea06dbeebaa8ea1b5e7f5748cb0b15fbc2f4e7",
          "message": "remove debug print",
          "timestamp": "2021-04-06T16:18:59+02:00",
          "tree_id": "0637c9c8efe6d0bb887a8d47c2202aff494b07c1",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/68ea06dbeebaa8ea1b5e7f5748cb0b15fbc2f4e7"
        },
        "date": 1617719348421,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 437,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 38169121,
            "range": "± 10049689",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 20394338,
            "range": "± 10628520",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 26511075,
            "range": "± 11093838",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 38407079,
            "range": "± 32463573",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 36078003,
            "range": "± 22271259",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14033,
            "range": "± 3087",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 482,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 581,
            "range": "± 126",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2000,
            "range": "± 379",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12729,
            "range": "± 2046",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 117756,
            "range": "± 19866",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 195668,
            "range": "± 42956",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1368671,
            "range": "± 345739",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1557198,
            "range": "± 546028",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 421,
            "range": "± 82",
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
          "id": "5ca53cf3510f30570f767ac575707b4972a77419",
          "message": "move rollup tests to delta.rs",
          "timestamp": "2021-04-06T16:24:53+02:00",
          "tree_id": "ce61ea89711855feca33be0698d8b18db7357049",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/5ca53cf3510f30570f767ac575707b4972a77419"
        },
        "date": 1617719605756,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 446,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 26158983,
            "range": "± 7574272",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 10025216,
            "range": "± 4760025",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 15141290,
            "range": "± 6798283",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 26459893,
            "range": "± 3774578",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 26742475,
            "range": "± 5985302",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13958,
            "range": "± 405",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 502,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 590,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1933,
            "range": "± 51",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12826,
            "range": "± 680",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 120495,
            "range": "± 1670",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 199407,
            "range": "± 1083",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 823171,
            "range": "± 378025",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 770000,
            "range": "± 297447",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 430,
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
          "id": "1e0c1bd43deb25a675fac9e29b1ec9c1e1b78387",
          "message": "add a content test for rollup from disk",
          "timestamp": "2021-04-06T16:44:42+02:00",
          "tree_id": "33b2a5e1eac7a715e4a2a54e1201706b1e932c25",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/1e0c1bd43deb25a675fac9e29b1ec9c1e1b78387"
        },
        "date": 1617720795020,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 416,
            "range": "± 109",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 25235289,
            "range": "± 2580494",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 10968316,
            "range": "± 2814359",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 14854866,
            "range": "± 4919879",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 26590222,
            "range": "± 2078130",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 26699846,
            "range": "± 2154406",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13914,
            "range": "± 668",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 501,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 593,
            "range": "± 34",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1916,
            "range": "± 109",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12744,
            "range": "± 695",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 119613,
            "range": "± 6496",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 197571,
            "range": "± 7454",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 845326,
            "range": "± 214275",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 902341,
            "range": "± 201548",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 424,
            "range": "± 50",
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
          "id": "0d3c066b5c2aff2857e4a4e1f8ce04e615829c10",
          "message": "rename functions to make it more clear that they work in-memory",
          "timestamp": "2021-04-06T16:49:15+02:00",
          "tree_id": "51b7a66d389018340ce89ac3dd0b7fc3b3987ca6",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/0d3c066b5c2aff2857e4a4e1f8ce04e615829c10"
        },
        "date": 1617721124830,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 441,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 28227797,
            "range": "± 10861733",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12460675,
            "range": "± 7116579",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19798904,
            "range": "± 10945780",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31495981,
            "range": "± 14911049",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30996246,
            "range": "± 13585055",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14327,
            "range": "± 635",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 534,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 627,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2030,
            "range": "± 82",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12964,
            "range": "± 363",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 123350,
            "range": "± 2818",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 198007,
            "range": "± 5231",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 876496,
            "range": "± 327507",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 982269,
            "range": "± 323635",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 459,
            "range": "± 15",
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
          "id": "bc49e46c2c3d180fb1ab314a2b75c2d38068fbef",
          "message": "remove dead code allow attribute",
          "timestamp": "2021-04-06T17:09:50+02:00",
          "tree_id": "0d5a9f665be13de5595d3c25d1f66f89bd79967e",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/bc49e46c2c3d180fb1ab314a2b75c2d38068fbef"
        },
        "date": 1617722324660,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 449,
            "range": "± 46",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 30840044,
            "range": "± 5795254",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13557133,
            "range": "± 4828521",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 17518034,
            "range": "± 7471544",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30510664,
            "range": "± 4488856",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 29698348,
            "range": "± 5184388",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13942,
            "range": "± 128",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 503,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 604,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1928,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12772,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 120248,
            "range": "± 903",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 199607,
            "range": "± 442",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 971866,
            "range": "± 238716",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1055039,
            "range": "± 484109",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 429,
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
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e88991c44fa5b5d7f8ebbd6563b769f5c11222cd",
          "message": "Merge pull request #61 from terminusdb/rollup_tests\n\nRollup fixes and tests",
          "timestamp": "2021-04-06T17:14:31+02:00",
          "tree_id": "0d5a9f665be13de5595d3c25d1f66f89bd79967e",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/e88991c44fa5b5d7f8ebbd6563b769f5c11222cd"
        },
        "date": 1617722677726,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 416,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 45472538,
            "range": "± 17738368",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 21921794,
            "range": "± 8831294",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 31736761,
            "range": "± 13420918",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 43114388,
            "range": "± 15131713",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 44315278,
            "range": "± 19342196",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14004,
            "range": "± 4240",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 491,
            "range": "± 110",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 574,
            "range": "± 191",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1924,
            "range": "± 327",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12373,
            "range": "± 3238",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 116122,
            "range": "± 19361",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 192730,
            "range": "± 44734",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1832814,
            "range": "± 1582510",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1657571,
            "range": "± 711319",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 416,
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
          "id": "e88991c44fa5b5d7f8ebbd6563b769f5c11222cd",
          "message": "Merge pull request #61 from terminusdb/rollup_tests\n\nRollup fixes and tests",
          "timestamp": "2021-04-06T17:14:31+02:00",
          "tree_id": "0d5a9f665be13de5595d3c25d1f66f89bd79967e",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/e88991c44fa5b5d7f8ebbd6563b769f5c11222cd"
        },
        "date": 1617791502709,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 335,
            "range": "± 37",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 25752766,
            "range": "± 5047928",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12614482,
            "range": "± 5015721",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 15850164,
            "range": "± 3893062",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 27754577,
            "range": "± 6033892",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 25802926,
            "range": "± 7592491",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10323,
            "range": "± 1333",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 389,
            "range": "± 108",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 458,
            "range": "± 59",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1467,
            "range": "± 135",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9573,
            "range": "± 2147",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 92488,
            "range": "± 23714",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 152407,
            "range": "± 40446",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 847032,
            "range": "± 266755",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 917539,
            "range": "± 141360",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 324,
            "range": "± 94",
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
          "id": "0409dcffd5af391d1020d5d6ceb840e3a7749dcc",
          "message": "start work on moving pack functionality into its own file",
          "timestamp": "2021-04-07T13:31:23+02:00",
          "tree_id": "2a31216e05baa4647031cad0567bcf115cd13480",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/0409dcffd5af391d1020d5d6ceb840e3a7749dcc"
        },
        "date": 1617795617758,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 391,
            "range": "± 97",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 35953010,
            "range": "± 7414463",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 20070130,
            "range": "± 9459067",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 25675350,
            "range": "± 12741612",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 35608355,
            "range": "± 9923237",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 34652426,
            "range": "± 11628012",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11290,
            "range": "± 1681",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 392,
            "range": "± 68",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 466,
            "range": "± 85",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1583,
            "range": "± 268",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10196,
            "range": "± 3083",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 105592,
            "range": "± 26449",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 187131,
            "range": "± 45235",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1339942,
            "range": "± 610370",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1464096,
            "range": "± 723041",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 368,
            "range": "± 264",
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
          "id": "6ca033c5ee79b9e8d81436eaa7736702d7c38f41",
          "message": "raise crate version to 0.17.0",
          "timestamp": "2021-04-07T13:35:28+02:00",
          "tree_id": "46fe318b55ca4d61e8503726df542d404ac123f5",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/6ca033c5ee79b9e8d81436eaa7736702d7c38f41"
        },
        "date": 1617796069589,
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
            "value": 37321290,
            "range": "± 4863354",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 19916656,
            "range": "± 5046520",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 25113104,
            "range": "± 6742308",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 37464548,
            "range": "± 6441799",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 34636800,
            "range": "± 7123080",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13618,
            "range": "± 1564",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 495,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 564,
            "range": "± 74",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1889,
            "range": "± 303",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12407,
            "range": "± 973",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 116929,
            "range": "± 11718",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 187355,
            "range": "± 14626",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1480393,
            "range": "± 403111",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1497033,
            "range": "± 404678",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 408,
            "range": "± 44",
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
          "id": "4a197d2d6c78d0751dcd64d4bf4eb3224b99ffb4",
          "message": "raise crate version to 0.17.1",
          "timestamp": "2021-04-07T13:53:12+02:00",
          "tree_id": "bc7bdf9d900f2aa0c38e94e6b42484d61fb0d0d7",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/4a197d2d6c78d0751dcd64d4bf4eb3224b99ffb4"
        },
        "date": 1617796837726,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 363,
            "range": "± 61",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 18397899,
            "range": "± 1451031",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 9692143,
            "range": "± 2350365",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 10322849,
            "range": "± 4297339",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 19745204,
            "range": "± 1401141",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 20501395,
            "range": "± 2236167",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11559,
            "range": "± 107",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 419,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 497,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1604,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10406,
            "range": "± 103",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 97177,
            "range": "± 746",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 167242,
            "range": "± 282",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 529707,
            "range": "± 43616",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 587746,
            "range": "± 49415",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 357,
            "range": "± 1",
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
          "id": "6ca033c5ee79b9e8d81436eaa7736702d7c38f41",
          "message": "raise crate version to 0.17.0",
          "timestamp": "2021-04-07T13:35:28+02:00",
          "tree_id": "46fe318b55ca4d61e8503726df542d404ac123f5",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/6ca033c5ee79b9e8d81436eaa7736702d7c38f41"
        },
        "date": 1617796936807,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 374,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 33008038,
            "range": "± 4476653",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14429406,
            "range": "± 1984306",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 23672441,
            "range": "± 8007279",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31403345,
            "range": "± 7104725",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28009011,
            "range": "± 3262888",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13102,
            "range": "± 3197",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 435,
            "range": "± 69",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 515,
            "range": "± 53",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1689,
            "range": "± 136",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11046,
            "range": "± 823",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 105096,
            "range": "± 4810",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 173320,
            "range": "± 14738",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1014115,
            "range": "± 143366",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1107345,
            "range": "± 70010",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 367,
            "range": "± 21",
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
          "id": "4a197d2d6c78d0751dcd64d4bf4eb3224b99ffb4",
          "message": "raise crate version to 0.17.1",
          "timestamp": "2021-04-07T13:53:12+02:00",
          "tree_id": "bc7bdf9d900f2aa0c38e94e6b42484d61fb0d0d7",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/4a197d2d6c78d0751dcd64d4bf4eb3224b99ffb4"
        },
        "date": 1617797045621,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 455,
            "range": "± 37",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 29980117,
            "range": "± 5448638",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13337106,
            "range": "± 4739719",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18598861,
            "range": "± 6098212",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30622487,
            "range": "± 6671915",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 29762099,
            "range": "± 4855151",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 15133,
            "range": "± 1770",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 553,
            "range": "± 64",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 642,
            "range": "± 76",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2093,
            "range": "± 200",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13858,
            "range": "± 1340",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 130574,
            "range": "± 11812",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 217028,
            "range": "± 27835",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1028956,
            "range": "± 157111",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1151751,
            "range": "± 457716",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 459,
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
          "id": "1eaf6209ef656ff9ba4584d65a5ad29f00e5bdf3",
          "message": "start work on moving pack functionality into its own file",
          "timestamp": "2021-04-12T08:21:46+02:00",
          "tree_id": "c2c59391f28d350209afddfa8b857d5ae6e0a94a",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/1eaf6209ef656ff9ba4584d65a5ad29f00e5bdf3"
        },
        "date": 1618209087701,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 380,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 32420440,
            "range": "± 4353602",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 17126171,
            "range": "± 8200323",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 21112556,
            "range": "± 9273968",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 34932379,
            "range": "± 9054964",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31370064,
            "range": "± 5973696",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12647,
            "range": "± 2899",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 429,
            "range": "± 60",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 506,
            "range": "± 89",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1715,
            "range": "± 285",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11291,
            "range": "± 1805",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 102909,
            "range": "± 14993",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 186117,
            "range": "± 35012",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1126919,
            "range": "± 399878",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1171203,
            "range": "± 342783",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 386,
            "range": "± 95",
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
          "id": "3b60ce6f105eea9ab8b8edaa98145b31a79b614c",
          "message": "implementation of layer export on top of PersistentLayerStore",
          "timestamp": "2021-04-12T10:15:27+02:00",
          "tree_id": "b01146732dc8659b74e25a1e89daad4870d2f550",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/3b60ce6f105eea9ab8b8edaa98145b31a79b614c"
        },
        "date": 1618215829027,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 442,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 25297670,
            "range": "± 5200448",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 11706458,
            "range": "± 3403475",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16013412,
            "range": "± 4199786",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 27873022,
            "range": "± 3822792",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28124290,
            "range": "± 4694054",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12691,
            "range": "± 2173",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 502,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 593,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1917,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12800,
            "range": "± 54",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 119504,
            "range": "± 2233",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 198046,
            "range": "± 3395",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 848995,
            "range": "± 302584",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 971003,
            "range": "± 153348",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 382,
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
          "id": "7337ebcf2b30e717125c9e679139d107aae3a714",
          "message": "use async_trait for pack reimplementation",
          "timestamp": "2021-04-12T12:06:53+02:00",
          "tree_id": "7e0fbbcf892d2c1f2da3f496be70399ecab07406",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/7337ebcf2b30e717125c9e679139d107aae3a714"
        },
        "date": 1618222542810,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 367,
            "range": "± 60",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 27254752,
            "range": "± 4461607",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13190386,
            "range": "± 4265442",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18148994,
            "range": "± 5836466",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30543752,
            "range": "± 4873301",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30742135,
            "range": "± 5499308",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12119,
            "range": "± 2300",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 425,
            "range": "± 76",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 495,
            "range": "± 126",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1584,
            "range": "± 303",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10752,
            "range": "± 2217",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 113401,
            "range": "± 7763",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 196060,
            "range": "± 33399",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 943436,
            "range": "± 304423",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1027406,
            "range": "± 254975",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 407,
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
          "id": "5befce9e35d0702b55badceee8ac3f93deac3577",
          "message": "implementing layer import",
          "timestamp": "2021-04-12T13:07:26+02:00",
          "tree_id": "ca4131a659b51df5d8edac0e07acd137f17dca39",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/5befce9e35d0702b55badceee8ac3f93deac3577"
        },
        "date": 1618226224262,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 380,
            "range": "± 59",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 38403708,
            "range": "± 7650824",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 22295219,
            "range": "± 7870027",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 27279158,
            "range": "± 7695722",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 41015812,
            "range": "± 10603171",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 37566474,
            "range": "± 7363961",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11781,
            "range": "± 1908",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 428,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 508,
            "range": "± 105",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1698,
            "range": "± 322",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10953,
            "range": "± 1628",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 101118,
            "range": "± 17901",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 168056,
            "range": "± 29533",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1679201,
            "range": "± 565964",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1699549,
            "range": "± 837081",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 364,
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
          "id": "57a5480f891347c6fa1ab3f95cdf86fdbfa05c4f",
          "message": "use block_in_place to mark use of the tar library as long running",
          "timestamp": "2021-04-12T13:10:33+02:00",
          "tree_id": "07a9ac051da044fbe7872d1077f6b3b3538971f6",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/57a5480f891347c6fa1ab3f95cdf86fdbfa05c4f"
        },
        "date": 1618226404043,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 425,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 25662360,
            "range": "± 6140980",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12121331,
            "range": "± 2693800",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16565118,
            "range": "± 3834323",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 28425088,
            "range": "± 3632394",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28654406,
            "range": "± 12049311",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14083,
            "range": "± 10948",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 487,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 580,
            "range": "± 24",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1971,
            "range": "± 99",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12818,
            "range": "± 793",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 120664,
            "range": "± 15438",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 195776,
            "range": "± 92402",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 912709,
            "range": "± 537454",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1018202,
            "range": "± 367021",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 415,
            "range": "± 19",
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
          "id": "3170fc8043e9956454a1f54c35a2cf3dece5ff20",
          "message": "replace existing pack implementation with new one and fix issues",
          "timestamp": "2021-04-12T14:34:38+02:00",
          "tree_id": "e3e1ce8c8adbf4bfaaf40ad36519fdc4b5a12e00",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/3170fc8043e9956454a1f54c35a2cf3dece5ff20"
        },
        "date": 1618231491904,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 425,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 31249504,
            "range": "± 6897622",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14735832,
            "range": "± 5046105",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19177939,
            "range": "± 9016124",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30781989,
            "range": "± 5779137",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30026223,
            "range": "± 5340466",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14674,
            "range": "± 1581",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 504,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 585,
            "range": "± 100",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1995,
            "range": "± 217",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12976,
            "range": "± 2116",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 126193,
            "range": "± 10606",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 200717,
            "range": "± 23329",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1019449,
            "range": "± 301772",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1150805,
            "range": "± 234875",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 416,
            "range": "± 27",
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
          "id": "e061d4bf8ff1052f3b326c4a7e2b5acf2b2b6849",
          "message": "move more pack code in pack.rs",
          "timestamp": "2021-04-12T14:41:19+02:00",
          "tree_id": "6a6f3c3c801e38c3205bf8a2c2dc433c5e0172a4",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/e061d4bf8ff1052f3b326c4a7e2b5acf2b2b6849"
        },
        "date": 1618231753131,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 369,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 22308775,
            "range": "± 2979968",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 10063282,
            "range": "± 2559361",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 13912675,
            "range": "± 6346661",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 24874815,
            "range": "± 3822931",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 23970317,
            "range": "± 2921434",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11748,
            "range": "± 128",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 417,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 493,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1613,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10523,
            "range": "± 135",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 97862,
            "range": "± 1216",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 164220,
            "range": "± 507",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 741687,
            "range": "± 251970",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 773757,
            "range": "± 202619",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 355,
            "range": "± 1",
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
          "id": "dbd2967e0d9a050158fd45bd8682c7d53f5c22af",
          "message": "remove debug println",
          "timestamp": "2021-04-12T17:15:59+02:00",
          "tree_id": "04e0beda3cdf7e1f591126dcb5178479778f18e2",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/dbd2967e0d9a050158fd45bd8682c7d53f5c22af"
        },
        "date": 1618241088530,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 417,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 28343320,
            "range": "± 6678368",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12859593,
            "range": "± 5502569",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16765967,
            "range": "± 20656198",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30040139,
            "range": "± 7191834",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28557195,
            "range": "± 6394655",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13721,
            "range": "± 1182",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 498,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 588,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1934,
            "range": "± 153",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12440,
            "range": "± 786",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 116630,
            "range": "± 1776",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 196291,
            "range": "± 12507",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 822549,
            "range": "± 164920",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 948909,
            "range": "± 137808",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 429,
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
          "id": "5719d52385b598729207da80a5260d1c6a2a279e",
          "message": "refactor bad contains_key pattern",
          "timestamp": "2021-04-12T17:22:30+02:00",
          "tree_id": "a2a2681032263da5bab74356c73d3b34fa3a9856",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/5719d52385b598729207da80a5260d1c6a2a279e"
        },
        "date": 1618241487935,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 398,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 24732161,
            "range": "± 5069747",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 11951814,
            "range": "± 3997253",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 15321457,
            "range": "± 5981831",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 27106834,
            "range": "± 5862097",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 25936653,
            "range": "± 3506431",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13209,
            "range": "± 3173",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 430,
            "range": "± 59",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 517,
            "range": "± 108",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1641,
            "range": "± 372",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10785,
            "range": "± 1732",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 102339,
            "range": "± 23278",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 172513,
            "range": "± 33269",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 723329,
            "range": "± 194904",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 825481,
            "range": "± 133686",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 426,
            "range": "± 30",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rrooij@users.noreply.github.com",
            "name": "rrooij",
            "username": "rrooij"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7f3a2ca72b34a370bf641203e85231ca988017c3",
          "message": "Merge pull request #64 from terminusdb/pack_generalization\n\nImplement layer export and import more generically",
          "timestamp": "2021-04-12T17:29:13+02:00",
          "tree_id": "a2a2681032263da5bab74356c73d3b34fa3a9856",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/7f3a2ca72b34a370bf641203e85231ca988017c3"
        },
        "date": 1618241978400,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 427,
            "range": "± 79",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 36744864,
            "range": "± 8842374",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 18711958,
            "range": "± 7690319",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 25614615,
            "range": "± 10174199",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 37442166,
            "range": "± 9808437",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 36906329,
            "range": "± 7105739",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13095,
            "range": "± 2142",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 478,
            "range": "± 70",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 621,
            "range": "± 65",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2043,
            "range": "± 207",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12940,
            "range": "± 1307",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 123699,
            "range": "± 10645",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 196572,
            "range": "± 30345",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1288078,
            "range": "± 313843",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1402199,
            "range": "± 320499",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 442,
            "range": "± 49",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rrooij@users.noreply.github.com",
            "name": "rrooij",
            "username": "rrooij"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7f3a2ca72b34a370bf641203e85231ca988017c3",
          "message": "Merge pull request #64 from terminusdb/pack_generalization\n\nImplement layer export and import more generically",
          "timestamp": "2021-04-12T17:29:13+02:00",
          "tree_id": "a2a2681032263da5bab74356c73d3b34fa3a9856",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/7f3a2ca72b34a370bf641203e85231ca988017c3"
        },
        "date": 1618309557700,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 416,
            "range": "± 26",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 33796271,
            "range": "± 7549234",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15108144,
            "range": "± 5928330",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22771562,
            "range": "± 11236773",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 35711743,
            "range": "± 7073933",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 32061935,
            "range": "± 5900923",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14281,
            "range": "± 961",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 479,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 572,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1938,
            "range": "± 98",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12813,
            "range": "± 2037",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 121198,
            "range": "± 8646",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 196753,
            "range": "± 13050",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1115819,
            "range": "± 291405",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1234671,
            "range": "± 206001",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 400,
            "range": "± 36",
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
          "id": "2432a0b5c0a709c31eef44342990d881ce50a398",
          "message": "new method on NamedGraph for retrieving both layer and label version",
          "timestamp": "2021-04-13T12:33:41+02:00",
          "tree_id": "2a9c45e2daa290ee641d1a38aa10687c8b244de1",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/2432a0b5c0a709c31eef44342990d881ce50a398"
        },
        "date": 1618310674825,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 435,
            "range": "± 100",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 35965989,
            "range": "± 9446841",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 18347161,
            "range": "± 7680976",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 24045448,
            "range": "± 10794074",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36144131,
            "range": "± 9772544",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 35352490,
            "range": "± 6608660",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14960,
            "range": "± 3447",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 513,
            "range": "± 88",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 592,
            "range": "± 193",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2062,
            "range": "± 450",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13296,
            "range": "± 2234",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 127431,
            "range": "± 40144",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 196760,
            "range": "± 48343",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1214775,
            "range": "± 494038",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1401207,
            "range": "± 511243",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 410,
            "range": "± 101",
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
          "id": "c14218e5c7473d8917d31c5100ae0975bdf54aee",
          "message": "better locking when updating label files",
          "timestamp": "2021-04-13T13:04:04+02:00",
          "tree_id": "04350bbf1cd9d24dbe4e00c83e16c672f217e1ff",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/c14218e5c7473d8917d31c5100ae0975bdf54aee"
        },
        "date": 1618312474218,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 385,
            "range": "± 90",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 38725663,
            "range": "± 7619302",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 19232928,
            "range": "± 7398277",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22038246,
            "range": "± 11048394",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 38384815,
            "range": "± 7217729",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 35908613,
            "range": "± 6982645",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14427,
            "range": "± 1282",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 525,
            "range": "± 139",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 607,
            "range": "± 164",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1979,
            "range": "± 504",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13074,
            "range": "± 2866",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 127539,
            "range": "± 24633",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 206855,
            "range": "± 29319",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1208691,
            "range": "± 417727",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1449019,
            "range": "± 688495",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 441,
            "range": "± 134",
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
          "id": "53375702377ccf7220e94a812f1c71758557a4f0",
          "message": "change return type for force_set_head to io::Result<()>",
          "timestamp": "2021-04-13T13:06:45+02:00",
          "tree_id": "2d1b5118fb00eb3aec5dbb9849a3640580828351",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/53375702377ccf7220e94a812f1c71758557a4f0"
        },
        "date": 1618312566534,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 378,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 32676837,
            "range": "± 8533995",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15251399,
            "range": "± 6180968",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20129157,
            "range": "± 9183320",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31054748,
            "range": "± 4077670",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28564569,
            "range": "± 3943748",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12203,
            "range": "± 1296",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 427,
            "range": "± 93",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 506,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1682,
            "range": "± 239",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11425,
            "range": "± 1174",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 105206,
            "range": "± 17193",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 176896,
            "range": "± 27884",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 995460,
            "range": "± 134080",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1093968,
            "range": "± 96170",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 357,
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
          "id": "1365b14ca3f21625a48a308bac749084e8062dac",
          "message": "implement force_set_head_version + tests",
          "timestamp": "2021-04-13T13:27:20+02:00",
          "tree_id": "d7eb184a0fceb3275766d4e132e8939bceb1556f",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/1365b14ca3f21625a48a308bac749084e8062dac"
        },
        "date": 1618313861827,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 401,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 36644328,
            "range": "± 6812081",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 19488345,
            "range": "± 6386171",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 24162632,
            "range": "± 5965306",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36149617,
            "range": "± 8718354",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 34746453,
            "range": "± 5260959",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13552,
            "range": "± 1896",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 467,
            "range": "± 70",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 583,
            "range": "± 111",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1848,
            "range": "± 375",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12377,
            "range": "± 2276",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 111913,
            "range": "± 19372",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 184582,
            "range": "± 35683",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1304071,
            "range": "± 338235",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1393186,
            "range": "± 539377",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 436,
            "range": "± 48",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rrooij@users.noreply.github.com",
            "name": "rrooij",
            "username": "rrooij"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "defcaf52cdda7cfafe55890dd09b200ce7669e7a",
          "message": "Merge pull request #66 from terminusdb/label_versioned_update\n\nLabel versioned update",
          "timestamp": "2021-04-13T13:43:31+02:00",
          "tree_id": "d7eb184a0fceb3275766d4e132e8939bceb1556f",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/defcaf52cdda7cfafe55890dd09b200ce7669e7a"
        },
        "date": 1618314663884,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 384,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 20895210,
            "range": "± 10740164",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 11871358,
            "range": "± 13000372",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 14232616,
            "range": "± 11505331",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 23215939,
            "range": "± 10073751",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 24290922,
            "range": "± 12316164",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11720,
            "range": "± 177",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 418,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 493,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1629,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10485,
            "range": "± 163",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 97747,
            "range": "± 1319",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 164586,
            "range": "± 304",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 599238,
            "range": "± 317315",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 710413,
            "range": "± 896409",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 358,
            "range": "± 1",
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
          "id": "8deb06e4a03e1ad95767e978862c9b107ca3d994",
          "message": "raise version to 0.18.0",
          "timestamp": "2021-04-13T13:46:25+02:00",
          "tree_id": "144c70c1e077fd331f4081bd1c9b95c95a2a4606",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/8deb06e4a03e1ad95767e978862c9b107ca3d994"
        },
        "date": 1618315057437,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 437,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 28250785,
            "range": "± 4498115",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13954267,
            "range": "± 5319274",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 17836526,
            "range": "± 6161054",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31553609,
            "range": "± 6800378",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31581427,
            "range": "± 4658982",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13435,
            "range": "± 1870",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 496,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 581,
            "range": "± 105",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1863,
            "range": "± 179",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12206,
            "range": "± 1925",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 113679,
            "range": "± 17778",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 191127,
            "range": "± 34143",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1043657,
            "range": "± 435960",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1422017,
            "range": "± 815109",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 420,
            "range": "± 41",
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
          "id": "8deb06e4a03e1ad95767e978862c9b107ca3d994",
          "message": "raise version to 0.18.0",
          "timestamp": "2021-04-13T13:46:25+02:00",
          "tree_id": "144c70c1e077fd331f4081bd1c9b95c95a2a4606",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/8deb06e4a03e1ad95767e978862c9b107ca3d994"
        },
        "date": 1618315144259,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 430,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 31423140,
            "range": "± 6296254",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14463037,
            "range": "± 5327353",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22145500,
            "range": "± 7822079",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30829855,
            "range": "± 4195755",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30018342,
            "range": "± 5622201",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14296,
            "range": "± 1570",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 505,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 595,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2024,
            "range": "± 192",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13141,
            "range": "± 1240",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 125067,
            "range": "± 9198",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 199766,
            "range": "± 14265",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1019802,
            "range": "± 193868",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1122767,
            "range": "± 257172",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 418,
            "range": "± 48",
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
          "id": "8deb06e4a03e1ad95767e978862c9b107ca3d994",
          "message": "raise version to 0.18.0",
          "timestamp": "2021-04-13T13:46:25+02:00",
          "tree_id": "144c70c1e077fd331f4081bd1c9b95c95a2a4606",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/8deb06e4a03e1ad95767e978862c9b107ca3d994"
        },
        "date": 1618447309320,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 451,
            "range": "± 97",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 31830530,
            "range": "± 16103281",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15331237,
            "range": "± 6967326",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19472782,
            "range": "± 10899467",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 34118160,
            "range": "± 10215582",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 33032027,
            "range": "± 8207794",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13929,
            "range": "± 299",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 496,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 587,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1946,
            "range": "± 56",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12715,
            "range": "± 157",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 119413,
            "range": "± 830",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 199550,
            "range": "± 802",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1016531,
            "range": "± 487202",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1102913,
            "range": "± 458325",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 415,
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
          "id": "8a866323fbeeb3cb524f66270214ae024309aba8",
          "message": "rewrite LabelStore trait to use async_trait",
          "timestamp": "2021-04-15T02:45:12+02:00",
          "tree_id": "ad1171e7735af4b1372a9e4f4eef2a0f6e5b2285",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/8a866323fbeeb3cb524f66270214ae024309aba8"
        },
        "date": 1618448040032,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 371,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 22940891,
            "range": "± 3813365",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 11779401,
            "range": "± 1804940",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 12356218,
            "range": "± 5925180",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 24577221,
            "range": "± 2455792",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 24348734,
            "range": "± 2287176",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12191,
            "range": "± 1803",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 448,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 519,
            "range": "± 91",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1583,
            "range": "± 319",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12121,
            "range": "± 1330",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 110727,
            "range": "± 22297",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 187429,
            "range": "± 20739",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 673468,
            "range": "± 84789",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 747216,
            "range": "± 76119",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 380,
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
          "id": "0e66dcc87d8eba2064c8dc9599abf2566a5c5405",
          "message": "implement delete_label - tests to follow",
          "timestamp": "2021-04-15T14:40:50+02:00",
          "tree_id": "c351b8c673520e9d575139e3535777a8411d2716",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/0e66dcc87d8eba2064c8dc9599abf2566a5c5405"
        },
        "date": 1618491011468,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 446,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 26082681,
            "range": "± 2817512",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 11295715,
            "range": "± 3011405",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 15315821,
            "range": "± 4416356",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 28888551,
            "range": "± 5148608",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 27816802,
            "range": "± 4344271",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13883,
            "range": "± 113",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 499,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 589,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1913,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12720,
            "range": "± 190",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 118812,
            "range": "± 2910",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 199846,
            "range": "± 994",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 877528,
            "range": "± 265692",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 981370,
            "range": "± 332648",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 423,
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
          "id": "dc5d1368bce30520f288be0fa6c05d0eccd20402",
          "message": "change result to false when deleting nonexistent directory label",
          "timestamp": "2021-04-15T15:14:21+02:00",
          "tree_id": "11cff17cca835c22edbc3cdf6fefef72b0ddc258",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/dc5d1368bce30520f288be0fa6c05d0eccd20402"
        },
        "date": 1618493008837,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 360,
            "range": "± 73",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 29503198,
            "range": "± 8827149",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12728686,
            "range": "± 4305070",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18008399,
            "range": "± 9884325",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 27200696,
            "range": "± 5792678",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 26257064,
            "range": "± 6330720",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11151,
            "range": "± 1296",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 375,
            "range": "± 107",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 450,
            "range": "± 93",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1572,
            "range": "± 261",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9617,
            "range": "± 2284",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 98661,
            "range": "± 15908",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 151333,
            "range": "± 12497",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 850394,
            "range": "± 280410",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 944573,
            "range": "± 282103",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 335,
            "range": "± 101",
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
          "id": "18998b337628e939f8d9fa833eed1752915925f8",
          "message": "label deletion tests",
          "timestamp": "2021-04-15T15:22:15+02:00",
          "tree_id": "4f64a40c4237f132fa7cbb5635ef192ae2c514bb",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/18998b337628e939f8d9fa833eed1752915925f8"
        },
        "date": 1618493457384,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 379,
            "range": "± 77",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 25310880,
            "range": "± 5640323",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 11315443,
            "range": "± 2733461",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 15441273,
            "range": "± 5380496",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 26740250,
            "range": "± 4031482",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 26114128,
            "range": "± 3380785",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11771,
            "range": "± 2396",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 436,
            "range": "± 83",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 488,
            "range": "± 104",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1630,
            "range": "± 356",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10519,
            "range": "± 2150",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 100470,
            "range": "± 18755",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 171036,
            "range": "± 30360",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 819437,
            "range": "± 188704",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 885695,
            "range": "± 231003",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 404,
            "range": "± 36",
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
          "id": "2d4fea1c9cc2b1821c84921ab775fed85c9862ce",
          "message": "add label deletion to high-level api",
          "timestamp": "2021-04-15T15:37:24+02:00",
          "tree_id": "aadc2f641067fd76353269628c84447cf2034179",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/2d4fea1c9cc2b1821c84921ab775fed85c9862ce"
        },
        "date": 1618494502984,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 469,
            "range": "± 118",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 36958466,
            "range": "± 9805616",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 19046591,
            "range": "± 8885199",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 23822854,
            "range": "± 10521130",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 37899104,
            "range": "± 12367379",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 36662216,
            "range": "± 9772176",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14781,
            "range": "± 1215",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 532,
            "range": "± 76",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 625,
            "range": "± 73",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2069,
            "range": "± 263",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13552,
            "range": "± 2517",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 124417,
            "range": "± 10292",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 199893,
            "range": "± 21424",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1237319,
            "range": "± 634972",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1352684,
            "range": "± 478244",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 465,
            "range": "± 39",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rrooij@users.noreply.github.com",
            "name": "rrooij",
            "username": "rrooij"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b85f042e5d19bf67bd2e5e447c4b8de3d6f97cdb",
          "message": "Merge pull request #68 from terminusdb/label_deletion\n\nLabel deletion",
          "timestamp": "2021-04-15T16:17:09+02:00",
          "tree_id": "aadc2f641067fd76353269628c84447cf2034179",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/b85f042e5d19bf67bd2e5e447c4b8de3d6f97cdb"
        },
        "date": 1618496799243,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 394,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 26950633,
            "range": "± 6607694",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12659284,
            "range": "± 5168907",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16729425,
            "range": "± 4848749",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 27851326,
            "range": "± 5488919",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28119510,
            "range": "± 5050102",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13799,
            "range": "± 2411",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 473,
            "range": "± 141",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 589,
            "range": "± 99",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1922,
            "range": "± 337",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11298,
            "range": "± 1845",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 107122,
            "range": "± 19056",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 176632,
            "range": "± 18666",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 958226,
            "range": "± 300400",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1068552,
            "range": "± 267833",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 410,
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
          "id": "595c70418c2e9fcabf0883c15197c59e288111f1",
          "message": "fix logarray benchmark tests",
          "timestamp": "2021-04-19T12:48:00+02:00",
          "tree_id": "1deddd8326240660f1fbc7ff1399c39ecfd5a5f1",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/595c70418c2e9fcabf0883c15197c59e288111f1"
        },
        "date": 1618829878341,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 455,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 29907376,
            "range": "± 7582608",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14075798,
            "range": "± 4148099",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20084957,
            "range": "± 9957813",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 32227487,
            "range": "± 6780235",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 32056105,
            "range": "± 6803171",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14909,
            "range": "± 91",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 562,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 661,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2076,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13473,
            "range": "± 60",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 126860,
            "range": "± 613",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 206162,
            "range": "± 588",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 697327,
            "range": "± 161172",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 850510,
            "range": "± 265739",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 481,
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
          "id": "499e019576d8bd433bfcf2fedf808d9ff7b23877",
          "message": "fix logarray benchmark tests",
          "timestamp": "2021-04-19T12:57:11+02:00",
          "tree_id": "1deddd8326240660f1fbc7ff1399c39ecfd5a5f1",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/499e019576d8bd433bfcf2fedf808d9ff7b23877"
        },
        "date": 1618830501485,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 454,
            "range": "± 137",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 41275567,
            "range": "± 12123002",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 22899313,
            "range": "± 6182678",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 30958298,
            "range": "± 11195708",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 44020452,
            "range": "± 6509989",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 41228637,
            "range": "± 7322219",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13231,
            "range": "± 3192",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 506,
            "range": "± 106",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 646,
            "range": "± 89",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1835,
            "range": "± 400",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13003,
            "range": "± 2337",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 114726,
            "range": "± 25663",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 182725,
            "range": "± 38099",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1291884,
            "range": "± 412303",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1320632,
            "range": "± 519428",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 441,
            "range": "± 66",
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
          "id": "6da1ea6fa373e68dce6a0310aab4f41d552171b3",
          "message": "derive Clone on all the high level api types",
          "timestamp": "2021-04-30T13:02:25+02:00",
          "tree_id": "06d801ba4679fd8221370377521740c81168739e",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/6da1ea6fa373e68dce6a0310aab4f41d552171b3"
        },
        "date": 1619781048563,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 316,
            "range": "± 50",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 21474183,
            "range": "± 2656464",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 11588895,
            "range": "± 2962997",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 14570348,
            "range": "± 2231103",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 25307816,
            "range": "± 2466535",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 25673661,
            "range": "± 1906297",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10609,
            "range": "± 175",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 401,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 472,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1495,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9485,
            "range": "± 164",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 90096,
            "range": "± 1678",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 145958,
            "range": "± 387",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 664464,
            "range": "± 160782",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 740583,
            "range": "± 154608",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 391,
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
          "id": "8969884da308a138f433da3784ee9ac9dc5d2812",
          "message": "raise version to 0.19.0",
          "timestamp": "2021-05-06T16:05:02+02:00",
          "tree_id": "afb7b79ac686aef3a6e480e018ca78052c089afe",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/8969884da308a138f433da3784ee9ac9dc5d2812"
        },
        "date": 1620310613404,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 401,
            "range": "± 72",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 33990767,
            "range": "± 5589450",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15979373,
            "range": "± 5636665",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 24054361,
            "range": "± 9079297",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 34077413,
            "range": "± 8050515",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 32558288,
            "range": "± 4787172",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13365,
            "range": "± 1547",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 476,
            "range": "± 151",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 560,
            "range": "± 120",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1897,
            "range": "± 228",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13153,
            "range": "± 1767",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 116772,
            "range": "± 26253",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 184389,
            "range": "± 29279",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 985967,
            "range": "± 174113",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1054844,
            "range": "± 187669",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 412,
            "range": "± 103",
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
          "id": "c99d53287a0b4c306afc897b56786461d97c55e1",
          "message": "make wavelet lookup work properly on empty wavelet trees",
          "timestamp": "2021-05-11T16:48:33+02:00",
          "tree_id": "25ce9df2967d84093b7fd82cb0c09db19bb32e71",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/c99d53287a0b4c306afc897b56786461d97c55e1"
        },
        "date": 1620745007841,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 428,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 27347165,
            "range": "± 4488391",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12869666,
            "range": "± 3733905",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19229039,
            "range": "± 4152809",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30450558,
            "range": "± 4358895",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 29824171,
            "range": "± 2852358",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14214,
            "range": "± 953",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 532,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 625,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1995,
            "range": "± 123",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12883,
            "range": "± 451",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 118865,
            "range": "± 9334",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 195747,
            "range": "± 8417",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 701722,
            "range": "± 204959",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 788758,
            "range": "± 206906",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 462,
            "range": "± 47",
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
          "id": "108c2aa35718b3d68a8981a2a01ebb31632de712",
          "message": "raise crate version to 0.19.1",
          "timestamp": "2021-05-12T00:37:50+02:00",
          "tree_id": "cf2fcd96fce606ad9af4e412166a107a3a77248f",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/108c2aa35718b3d68a8981a2a01ebb31632de712"
        },
        "date": 1620773235466,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 381,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 40054302,
            "range": "± 4550566",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 23363245,
            "range": "± 6788488",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 29975173,
            "range": "± 7637633",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 40915389,
            "range": "± 7501109",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 38178577,
            "range": "± 6415768",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11927,
            "range": "± 1283",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 467,
            "range": "± 60",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 552,
            "range": "± 58",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1761,
            "range": "± 225",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10928,
            "range": "± 1897",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 103815,
            "range": "± 11181",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 166318,
            "range": "± 22145",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1403970,
            "range": "± 446498",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1420273,
            "range": "± 416761",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 390,
            "range": "± 44",
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
          "id": "108c2aa35718b3d68a8981a2a01ebb31632de712",
          "message": "raise crate version to 0.19.1",
          "timestamp": "2021-05-12T00:37:50+02:00",
          "tree_id": "cf2fcd96fce606ad9af4e412166a107a3a77248f",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/108c2aa35718b3d68a8981a2a01ebb31632de712"
        },
        "date": 1620773273994,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 409,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 34781253,
            "range": "± 7203534",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15604992,
            "range": "± 7684917",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 21076639,
            "range": "± 9690658",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 33341458,
            "range": "± 4913180",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 33389279,
            "range": "± 4533000",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13787,
            "range": "± 1693",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 504,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 588,
            "range": "± 68",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1893,
            "range": "± 222",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12720,
            "range": "± 1331",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 118782,
            "range": "± 12161",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 190501,
            "range": "± 26361",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 944099,
            "range": "± 196963",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1030790,
            "range": "± 161370",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 419,
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
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "aee8eafc47e5a51b57511e9709442bb124d81894",
          "message": "Merge pull request #76 from spl/refactor-internal-layer\n\nRefactor InternalLayer",
          "timestamp": "2021-06-22T16:03:29+02:00",
          "tree_id": "5d7d2729fad2d5c8d900449581665b15f6d95dfe",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/aee8eafc47e5a51b57511e9709442bb124d81894"
        },
        "date": 1624371286878,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 443,
            "range": "± 53",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 44913451,
            "range": "± 7429480",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 24333433,
            "range": "± 7478695",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 35156004,
            "range": "± 11326004",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 51425703,
            "range": "± 13169523",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 48090321,
            "range": "± 9915103",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14451,
            "range": "± 1539",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 582,
            "range": "± 107",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 654,
            "range": "± 126",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2109,
            "range": "± 280",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13382,
            "range": "± 2375",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 124202,
            "range": "± 13375",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 138610,
            "range": "± 21148",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1383012,
            "range": "± 920208",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1373357,
            "range": "± 346001",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 491,
            "range": "± 79",
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
          "id": "aa587e7662b334969dff134a224415f0c43bbff3",
          "message": "Merge pull request #77 from spl/store-delete\n\nAdd Store::delete for deleting labels",
          "timestamp": "2021-07-01T11:41:52+02:00",
          "tree_id": "968585a2e519eabea5ab51a25f96b3f606b77fc4",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/aa587e7662b334969dff134a224415f0c43bbff3"
        },
        "date": 1625133169039,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 454,
            "range": "± 73",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 37401574,
            "range": "± 14301145",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 20694262,
            "range": "± 11858020",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 27275455,
            "range": "± 8524411",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 41142170,
            "range": "± 10013536",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 40106304,
            "range": "± 9884930",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14570,
            "range": "± 2224",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 572,
            "range": "± 82",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 662,
            "range": "± 113",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2042,
            "range": "± 382",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12755,
            "range": "± 2285",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 125674,
            "range": "± 22091",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 132762,
            "range": "± 10485",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1164319,
            "range": "± 527161",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1132311,
            "range": "± 618280",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 514,
            "range": "± 112",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "committer": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "distinct": true,
          "id": "951c2696d514d030b5ad4c74a12c33ce5a67cb59",
          "message": "Raise crate version from 0.19.1 to 0.19.2",
          "timestamp": "2021-07-01T13:16:49+02:00",
          "tree_id": "5dd1d2bdcc13ead0b88d3bf0837aeedee7b937cb",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/951c2696d514d030b5ad4c74a12c33ce5a67cb59"
        },
        "date": 1625138902606,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 390,
            "range": "± 98",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 37026327,
            "range": "± 14928374",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 20462701,
            "range": "± 6904016",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 27228218,
            "range": "± 9928261",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 34246513,
            "range": "± 5756327",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 33354989,
            "range": "± 4348200",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12239,
            "range": "± 1183",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 460,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 561,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1770,
            "range": "± 308",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11147,
            "range": "± 1172",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 108700,
            "range": "± 19218",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 112272,
            "range": "± 24813",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 903639,
            "range": "± 101692",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 947858,
            "range": "± 306389",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 396,
            "range": "± 50",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "committer": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "distinct": true,
          "id": "07e4662ef544fb65eef6b66a280f5798fee9fdb6",
          "message": "Exclude git and ci files and dirs from crate",
          "timestamp": "2021-07-01T13:36:02+02:00",
          "tree_id": "1fe33b6dd4109e6855d695930b2ea3b689d4499a",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/07e4662ef544fb65eef6b66a280f5798fee9fdb6"
        },
        "date": 1625139948630,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 335,
            "range": "± 68",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 19657301,
            "range": "± 940550",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 9968484,
            "range": "± 970752",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 14759238,
            "range": "± 3337173",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 23465833,
            "range": "± 1014961",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 24358153,
            "range": "± 1691800",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11725,
            "range": "± 139",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 468,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 544,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1708,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10663,
            "range": "± 116",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 98943,
            "range": "± 1394",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 109880,
            "range": "± 442",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 506425,
            "range": "± 42830",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 520298,
            "range": "± 38552",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 399,
            "range": "± 2",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "committer": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "distinct": true,
          "id": "07e4662ef544fb65eef6b66a280f5798fee9fdb6",
          "message": "Exclude git and ci files and dirs from crate",
          "timestamp": "2021-07-01T13:36:02+02:00",
          "tree_id": "1fe33b6dd4109e6855d695930b2ea3b689d4499a",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/07e4662ef544fb65eef6b66a280f5798fee9fdb6"
        },
        "date": 1625142893971,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 420,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 32191058,
            "range": "± 8003454",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16756778,
            "range": "± 5588942",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 23770399,
            "range": "± 5114004",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36744599,
            "range": "± 7443341",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 37624948,
            "range": "± 5302666",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13967,
            "range": "± 1097",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 549,
            "range": "± 149",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 641,
            "range": "± 93",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2036,
            "range": "± 135",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12552,
            "range": "± 840",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 118309,
            "range": "± 10389",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 131931,
            "range": "± 10932",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 867615,
            "range": "± 187266",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 880766,
            "range": "± 320590",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 438,
            "range": "± 60",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "committer": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "distinct": true,
          "id": "8d42b914ddbc224327a959ed6005e56d3554a959",
          "message": "Change master to main in docs",
          "timestamp": "2021-07-02T04:20:27+02:00",
          "tree_id": "d77929750518f8916b4a365252200157dd22a695",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/8d42b914ddbc224327a959ed6005e56d3554a959"
        },
        "date": 1625192893533,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 340,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 20225815,
            "range": "± 2175509",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 9825997,
            "range": "± 3992749",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 14968251,
            "range": "± 5278933",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 24757868,
            "range": "± 1844016",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 25548751,
            "range": "± 1466029",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11668,
            "range": "± 303",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 457,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 532,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1697,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10537,
            "range": "± 155",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 98680,
            "range": "± 1177",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 109747,
            "range": "± 326",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 551039,
            "range": "± 72293",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 549651,
            "range": "± 36550",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 385,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "committer": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "distinct": true,
          "id": "fc02bb1b3b647408a472d30dca113a9cc6fae7b8",
          "message": "Update version on README.md",
          "timestamp": "2021-07-02T04:22:37+02:00",
          "tree_id": "1aa4f0a3fa83846c0f2ca04ccd68f56e907ad855",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/fc02bb1b3b647408a472d30dca113a9cc6fae7b8"
        },
        "date": 1625193046549,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 366,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 28118778,
            "range": "± 6887450",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16396024,
            "range": "± 5130485",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16517194,
            "range": "± 4668669",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 25760262,
            "range": "± 9654432",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 32144096,
            "range": "± 5721539",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11686,
            "range": "± 166",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 470,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 547,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1700,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10452,
            "range": "± 122",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 98756,
            "range": "± 1181",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 109508,
            "range": "± 437",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 609930,
            "range": "± 285694",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 614847,
            "range": "± 50668",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 396,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rrooij@users.noreply.github.com",
            "name": "Robin de Rooij",
            "username": "rrooij"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4cd6e3c4b0f38ab3318b8cdb0fb32578ffbe7a0c",
          "message": "Merge pull request #79 from spl/workflow-separation\n\nWorkflow separation and (hopefully) improvement",
          "timestamp": "2021-07-02T16:02:55+02:00",
          "tree_id": "ba9a0e50e248508f7ccdec3440bf883d5c6ef7d5",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/4cd6e3c4b0f38ab3318b8cdb0fb32578ffbe7a0c"
        },
        "date": 1625234972931,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 335,
            "range": "± 42",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 24116647,
            "range": "± 4812611",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12021602,
            "range": "± 3169553",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 17108422,
            "range": "± 5787867",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 27556604,
            "range": "± 3499126",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 27730816,
            "range": "± 3724150",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11701,
            "range": "± 155",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 461,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 536,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1699,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10621,
            "range": "± 135",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 98998,
            "range": "± 1210",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 109538,
            "range": "± 362",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 702899,
            "range": "± 242258",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 803539,
            "range": "± 304910",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 385,
            "range": "± 2",
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
          "id": "ab351422a7131893969879a34e60f2745e077e3f",
          "message": "sync.rs: make task_sync public\n\nRequired if we want terminus-store-prolog to be able\nto use the task_sync function.",
          "timestamp": "2021-07-06T14:49:46+02:00",
          "tree_id": "e8971a099e9bd5d3354a66ec3cbbfd36fe707efb",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/ab351422a7131893969879a34e60f2745e077e3f"
        },
        "date": 1625576276852,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 389,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 24627978,
            "range": "± 3333238",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12286829,
            "range": "± 3147909",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16938787,
            "range": "± 2129752",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 28105742,
            "range": "± 5243614",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 27499570,
            "range": "± 2568649",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11588,
            "range": "± 140",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 475,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 544,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1674,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10487,
            "range": "± 106",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 97485,
            "range": "± 1334",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 110348,
            "range": "± 541",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 645865,
            "range": "± 120885",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 661008,
            "range": "± 162434",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 394,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "committer": {
            "email": "sean@terminusdb.com",
            "name": "Sean Leather",
            "username": "spl"
          },
          "distinct": true,
          "id": "3e34e7e33f8a87b6e589f820ddcda5a1404f4623",
          "message": "Remove duplication in test",
          "timestamp": "2021-07-13T13:21:15+02:00",
          "tree_id": "0c9dee755424bc94a1ff0cee69be18860b4cfddd",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/3e34e7e33f8a87b6e589f820ddcda5a1404f4623"
        },
        "date": 1626176025928,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 363,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 28264165,
            "range": "± 5564433",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14767901,
            "range": "± 4462325",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19450215,
            "range": "± 3541715",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31127625,
            "range": "± 7630576",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 29937198,
            "range": "± 4696889",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11721,
            "range": "± 113",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 464,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 538,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1728,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10557,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 98971,
            "range": "± 1129",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 110351,
            "range": "± 425",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 811449,
            "range": "± 339372",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 849719,
            "range": "± 287063",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 391,
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
          "id": "452d8b93e25a29ae3dff5ff4e2280750be735280",
          "message": "export StringTriple and IdTriple from lib.rs",
          "timestamp": "2021-08-23T12:11:09+02:00",
          "tree_id": "7fa3d27e4e6ebc5370ff5cce69233607cf88bb63",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/452d8b93e25a29ae3dff5ff4e2280750be735280"
        },
        "date": 1629713977753,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 425,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 34841866,
            "range": "± 8132942",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 19044125,
            "range": "± 11050986",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 26056857,
            "range": "± 8610268",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 41167347,
            "range": "± 10468966",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 37720064,
            "range": "± 6131650",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13851,
            "range": "± 119",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 569,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 658,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1967,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12755,
            "range": "± 61",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 117263,
            "range": "± 809",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 131880,
            "range": "± 168",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1269832,
            "range": "± 535736",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1194917,
            "range": "± 568304",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 479,
            "range": "± 26",
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
          "id": "88175c9ece426d7e5154b0ddfaa52c739493355e",
          "message": "Some example code",
          "timestamp": "2021-08-23T13:15:07+02:00",
          "tree_id": "83c175dcc1c2b8d162160689963ae5d3f20a94c9",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/88175c9ece426d7e5154b0ddfaa52c739493355e"
        },
        "date": 1629717790032,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 328,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 31585555,
            "range": "± 5761654",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 17334733,
            "range": "± 6871015",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 23397649,
            "range": "± 4867549",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 33507880,
            "range": "± 4470001",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31924410,
            "range": "± 3500137",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10636,
            "range": "± 2852",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 425,
            "range": "± 74",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 504,
            "range": "± 89",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1510,
            "range": "± 221",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10044,
            "range": "± 1430",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 93622,
            "range": "± 17691",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 100644,
            "range": "± 12287",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 877514,
            "range": "± 102855",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 913517,
            "range": "± 128366",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 375,
            "range": "± 89",
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
          "id": "10a6542ed5177d322043568542286dd15e1cc0dc",
          "message": "raise crate version to 0.19.3",
          "timestamp": "2021-08-23T13:16:28+02:00",
          "tree_id": "67ad0d1abc138924950b81db23cde9f20f8bb9c9",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/10a6542ed5177d322043568542286dd15e1cc0dc"
        },
        "date": 1629718008869,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 410,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 28773511,
            "range": "± 3337225",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12986197,
            "range": "± 7545504",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19352064,
            "range": "± 3492119",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31798189,
            "range": "± 3966805",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31465475,
            "range": "± 3508021",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13613,
            "range": "± 1092",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 523,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 614,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1945,
            "range": "± 80",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12347,
            "range": "± 685",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 114714,
            "range": "± 11301",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 128973,
            "range": "± 9331",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 732821,
            "range": "± 138258",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 803781,
            "range": "± 177693",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 469,
            "range": "± 6",
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
          "id": "5a2d6baf72ac20440e40362ac46b7faa50c3348b",
          "message": "mention examples in readme",
          "timestamp": "2021-08-23T13:19:14+02:00",
          "tree_id": "40143d2e29cf77e41cb271965874c2b3e4b00909",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/5a2d6baf72ac20440e40362ac46b7faa50c3348b"
        },
        "date": 1629718091448,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 421,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 30802700,
            "range": "± 11303917",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16444226,
            "range": "± 4908142",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 21739198,
            "range": "± 5748042",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36705642,
            "range": "± 7199842",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 35801835,
            "range": "± 5007364",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13786,
            "range": "± 1149",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 550,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 640,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1955,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12866,
            "range": "± 486",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 118493,
            "range": "± 7971",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 133601,
            "range": "± 6353",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1073722,
            "range": "± 399942",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 852466,
            "range": "± 354078",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 460,
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
          "id": "ce21d665828f36257e8cd8473e87534f4a8980b8",
          "message": "reformat example that was malformatted",
          "timestamp": "2021-08-23T13:22:51+02:00",
          "tree_id": "a42a3a08920ad7d2520a798b2aef3ee9e1b71061",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/ce21d665828f36257e8cd8473e87534f4a8980b8"
        },
        "date": 1629718421893,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 454,
            "range": "± 133",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 41511190,
            "range": "± 23816014",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 23184764,
            "range": "± 8810769",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 28306166,
            "range": "± 10138483",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 42886134,
            "range": "± 14043333",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 40176996,
            "range": "± 12087622",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14524,
            "range": "± 2276",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 611,
            "range": "± 86",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 697,
            "range": "± 76",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2163,
            "range": "± 310",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13646,
            "range": "± 1811",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 127834,
            "range": "± 23824",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 135977,
            "range": "± 16400",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1120767,
            "range": "± 186623",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1246694,
            "range": "± 575718",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 527,
            "range": "± 87",
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
          "id": "905d56715738cb412b045fa4889a930d67339441",
          "message": "when rolling up a previously rolled up layer, use predicate map correctly",
          "timestamp": "2021-10-01T16:30:49+02:00",
          "tree_id": "3fb53721aa618bbd74053b0145ae38c54d84e225",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/905d56715738cb412b045fa4889a930d67339441"
        },
        "date": 1633099162005,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 409,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 30740960,
            "range": "± 15800813",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 17143205,
            "range": "± 4842098",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 24399965,
            "range": "± 5444898",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36601598,
            "range": "± 5374470",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 37059115,
            "range": "± 8985907",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13206,
            "range": "± 1846",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 527,
            "range": "± 169",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 602,
            "range": "± 219",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1865,
            "range": "± 392",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11597,
            "range": "± 2642",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 109592,
            "range": "± 22789",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 122264,
            "range": "± 22574",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1002247,
            "range": "± 724004",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 997787,
            "range": "± 331556",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 438,
            "range": "± 128",
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
          "id": "84280ffe30b5c07932389333714aa25487262c50",
          "message": "Raise version to 0.19.4",
          "timestamp": "2021-10-01T16:34:46+02:00",
          "tree_id": "36de0c010e2843a02c0489d68fd2346acb4731af",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/84280ffe30b5c07932389333714aa25487262c50"
        },
        "date": 1633099331118,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 372,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 34133985,
            "range": "± 9115851",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 23579228,
            "range": "± 13730654",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 26462852,
            "range": "± 12556869",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36956815,
            "range": "± 12894222",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 37626843,
            "range": "± 10364070",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10446,
            "range": "± 122",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 463,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 518,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1506,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9097,
            "range": "± 77",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 83339,
            "range": "± 785",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 98894,
            "range": "± 1831",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1596792,
            "range": "± 792135",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1338605,
            "range": "± 729416",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 386,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fec3a33f63ccb15843ee1f8ba5d30494b17fdb25",
          "message": "Merge pull request #86 from terminusdb/checked_label_update\n\ncheck outcome of label update to ensure it was succesful",
          "timestamp": "2021-10-19T13:51:34+02:00",
          "tree_id": "ed85e06184b52e5ebc61d0e4847da782fbd09298",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/fec3a33f63ccb15843ee1f8ba5d30494b17fdb25"
        },
        "date": 1634644778211,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 356,
            "range": "± 103",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 36821651,
            "range": "± 5053244",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 19429776,
            "range": "± 4621728",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 25423683,
            "range": "± 5300829",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 37250976,
            "range": "± 9781931",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 36333476,
            "range": "± 5957121",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10025,
            "range": "± 2833",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 451,
            "range": "± 110",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 513,
            "range": "± 101",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1624,
            "range": "± 263",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9598,
            "range": "± 1735",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 92248,
            "range": "± 15890",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 101840,
            "range": "± 21508",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 981497,
            "range": "± 328861",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 968174,
            "range": "± 177638",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 421,
            "range": "± 102",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "41a3bd48a95b07d9ad0a1e2be68e7abfe9acee49",
          "message": "Merge pull request #87 from terminusdb/raise_version_to_0.19.5\n\nRaise version to 0.19.5",
          "timestamp": "2021-10-19T13:54:54+02:00",
          "tree_id": "1ef584c1e987a80ce6ea25c7230025f3626df80c",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/41a3bd48a95b07d9ad0a1e2be68e7abfe9acee49"
        },
        "date": 1634644949229,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 412,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 27234520,
            "range": "± 4271208",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12938778,
            "range": "± 1908762",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19122032,
            "range": "± 3434053",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 33698575,
            "range": "± 6171752",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 33873506,
            "range": "± 6475506",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12371,
            "range": "± 2037",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 561,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 641,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1823,
            "range": "± 92",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11248,
            "range": "± 1338",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 99102,
            "range": "± 9943",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 108633,
            "range": "± 31920",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 793339,
            "range": "± 147473",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 805957,
            "range": "± 423193",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 467,
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
          "id": "4529f1bf9286697b2b18a9e4e9b9bd6d9fcc82be",
          "message": "Rewrite more traits with futures using async_trait",
          "timestamp": "2021-10-21T13:46:32+02:00",
          "tree_id": "e17b65a12f957f550fdc4965fee1c4cce7e36235",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/4529f1bf9286697b2b18a9e4e9b9bd6d9fcc82be"
        },
        "date": 1634817193705,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 339,
            "range": "± 72",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 21537745,
            "range": "± 4751405",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 10531125,
            "range": "± 2528387",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 15101542,
            "range": "± 2689251",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 25510654,
            "range": "± 3131691",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 25562875,
            "range": "± 2463262",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10382,
            "range": "± 129",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 460,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 515,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1502,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9289,
            "range": "± 122",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 84261,
            "range": "± 1256",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 91197,
            "range": "± 729",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 645740,
            "range": "± 276241",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 639251,
            "range": "± 177347",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 383,
            "range": "± 1",
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
          "id": "b6956b20f4b6a28fbbe4f4e4ece8d45203d60a84",
          "message": "Implement a way to compare layers which fails on delta rollups",
          "timestamp": "2022-02-21T15:28:52+01:00",
          "tree_id": "3ba007203d69f2f64ed8fdb5ce5a2fec34db82be",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/b6956b20f4b6a28fbbe4f4e4ece8d45203d60a84"
        },
        "date": 1645454280375,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 439,
            "range": "± 46",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 41797910,
            "range": "± 9317477",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 24285030,
            "range": "± 7579274",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 29519604,
            "range": "± 7978242",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 41397289,
            "range": "± 7066213",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 42466335,
            "range": "± 8268785",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13691,
            "range": "± 84",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 558,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 653,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1985,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12296,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 110037,
            "range": "± 506",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 116775,
            "range": "± 576",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1257066,
            "range": "± 500553",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1601400,
            "range": "± 738710",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 479,
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
          "id": "cbe7b93d4936132cb8e89326a4b017f282672eea",
          "message": "raise version to 0.19.6",
          "timestamp": "2022-02-21T15:29:35+01:00",
          "tree_id": "96e3a312bcf5328029baf3023e374151d2aa1a51",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/cbe7b93d4936132cb8e89326a4b017f282672eea"
        },
        "date": 1645454310788,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 385,
            "range": "± 53",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 32397036,
            "range": "± 6916738",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15919945,
            "range": "± 4659909",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 23622831,
            "range": "± 7741222",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 36910209,
            "range": "± 5936822",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 36092780,
            "range": "± 4805967",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12698,
            "range": "± 380",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 530,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 614,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1832,
            "range": "± 81",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11451,
            "range": "± 298",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 103480,
            "range": "± 6021",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 111584,
            "range": "± 5061",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 995402,
            "range": "± 435716",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 995830,
            "range": "± 483998",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 456,
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
          "id": "38a05199462ed9e9d5b5069105f2771064e84376",
          "message": "Eliminate another location where a shared lock is acquired",
          "timestamp": "2022-05-19T16:04:01+02:00",
          "tree_id": "5ee15b04228d6049ff916fd8a4a74b0a03e2aadc",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/38a05199462ed9e9d5b5069105f2771064e84376"
        },
        "date": 1652969481078,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 277,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 24423153,
            "range": "± 11182983",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13710975,
            "range": "± 12412485",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16606192,
            "range": "± 14039412",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 27084791,
            "range": "± 28063646",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30018097,
            "range": "± 32913635",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9404,
            "range": "± 97",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 363,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 446,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1388,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 8677,
            "range": "± 89",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 85349,
            "range": "± 1333",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 94979,
            "range": "± 1522",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 667790,
            "range": "± 219592",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 751281,
            "range": "± 698499",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 294,
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
          "id": "a287e9af857ca5216c833841cf0d489c5eee93c1",
          "message": "Raise version to 0.19.7",
          "timestamp": "2022-05-19T16:05:27+02:00",
          "tree_id": "05388cec4cf572e96b8418982dee3174a45b2c8f",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/a287e9af857ca5216c833841cf0d489c5eee93c1"
        },
        "date": 1652969848563,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 275,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 29841827,
            "range": "± 3226969",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16624336,
            "range": "± 3376328",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22444698,
            "range": "± 4827783",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 35273039,
            "range": "± 6141234",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 35111070,
            "range": "± 5052875",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9330,
            "range": "± 294",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 351,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 426,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1388,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 8662,
            "range": "± 243",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 84687,
            "range": "± 1305",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 92348,
            "range": "± 1093",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1005258,
            "range": "± 334893",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1045447,
            "range": "± 286994",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 290,
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
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4eed43adc2cffdd8c8d3963e7697c8a4276559fd",
          "message": "Speed improvement in LogArray::entry for non-boundary cases (#95)\n\nIn a logarray, an entry may be entirely within one aligned u64, or it may span\r\ntwo. In the existing code, we always retrieve two u64's, but will only use one\r\nif the entry doesn't span both. This change will only retrieve that second u64\r\nwhen it is actually needed.\r\n\r\nIt's a small change, and the speed improvement isn't huge, but LogArray::entry\r\nappears to be the most called function in TerminusDB by a large margin, so even\r\nsmall improvements matter a little.",
          "timestamp": "2022-05-30T11:45:09+02:00",
          "tree_id": "796e98e613754af1bcaa2457451a2a1cfefa26e5",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/4eed43adc2cffdd8c8d3963e7697c8a4276559fd"
        },
        "date": 1653904351980,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 333,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 25686068,
            "range": "± 4400328",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12039369,
            "range": "± 2749435",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19345804,
            "range": "± 10689541",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 31147560,
            "range": "± 9961022",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28945936,
            "range": "± 7477590",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10665,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 341,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 415,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1447,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9284,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 86568,
            "range": "± 320",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 93324,
            "range": "± 182",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 711597,
            "range": "± 169583",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 709323,
            "range": "± 129505",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 275,
            "range": "± 1",
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
          "id": "c092e24a3d786c6008c04011f72b92442470a8db",
          "message": "Add methods for dealing with id to object conversion when object type is known (#94)\n\nFixes #93",
          "timestamp": "2022-05-30T12:03:05+02:00",
          "tree_id": "7ef6db989c0a4b8e28d948c887391c9f9397f694",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/c092e24a3d786c6008c04011f72b92442470a8db"
        },
        "date": 1653905450436,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 279,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 32885158,
            "range": "± 9374026",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 20102493,
            "range": "± 6519012",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 27176445,
            "range": "± 20759852",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 38231814,
            "range": "± 29417051",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 39019398,
            "range": "± 29058226",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9935,
            "range": "± 108",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 356,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 451,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1462,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9077,
            "range": "± 285",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 87079,
            "range": "± 1243",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 92164,
            "range": "± 906",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 754268,
            "range": "± 275610",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1050295,
            "range": "± 391753",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 285,
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
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3e9b1675dfa5ac42684f812464e8c4686a9b7598",
          "message": "Optimizations to BitIndex (#96)\n\n1. when selecting from a range, only the blocks and superblocks in that range\r\n   are considered.\r\n2. Selecting from the full index is now implemented through range selection\r\n   rather than the other way around.\r\n3. When the range from which we're selecting is entirely in the same cache line,\r\n   a linear scan is performed rather than a binary search.\r\n4. Some asserts are now debug_asserts and therefore no longer happen in\r\n   production code.",
          "timestamp": "2022-05-31T13:09:34+02:00",
          "tree_id": "77b6c9710034607ef4c2e703a1b166b9c92b14b7",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/3e9b1675dfa5ac42684f812464e8c4686a9b7598"
        },
        "date": 1653995814175,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 334,
            "range": "± 68",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 27621694,
            "range": "± 3380760",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15020764,
            "range": "± 4903262",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 21215443,
            "range": "± 4683392",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 32097323,
            "range": "± 3774413",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 31023725,
            "range": "± 3925598",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10437,
            "range": "± 51",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 346,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 416,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1410,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 8933,
            "range": "± 90",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 83764,
            "range": "± 315",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 91024,
            "range": "± 360",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 940067,
            "range": "± 295310",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1009317,
            "range": "± 349517",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 276,
            "range": "± 0",
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
          "id": "20af2f9e8623ea8ed2af453a3501e1e1e74ddfb9",
          "message": "Pre-allocate vectors instead of growing them dynamically (#97)\n\nVarious places in the code were allocating vectors that'd have to be grown at a\r\nlater point, which could incur a full move of the existing array data to a new\r\nlocation in memory, incurring quite some overhead. This is now avoided by\r\nensuring vectors are pre-allocated with the right size.",
          "timestamp": "2022-05-31T13:27:31+02:00",
          "tree_id": "2b659d7bb98cbf3b0b50f0b7b2a43fd26b518d17",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/20af2f9e8623ea8ed2af453a3501e1e1e74ddfb9"
        },
        "date": 1653996864828,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 296,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 29842493,
            "range": "± 9168771",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15852523,
            "range": "± 10048037",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 21187771,
            "range": "± 5630308",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 35043570,
            "range": "± 10071820",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 33336691,
            "range": "± 9068433",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10483,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 343,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 414,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1422,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 8962,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 83987,
            "range": "± 385",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 90867,
            "range": "± 336",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 964572,
            "range": "± 384433",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1080083,
            "range": "± 636867",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 276,
            "range": "± 0",
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
          "id": "3daa90361c48afcc31c621234dfbd0a01aa0ad70",
          "message": "Introduce method for quickly looking up a single triple when subject and predicate are known (#98)\n\nThere are some cases where we very quickly want to look up a single triple based\r\non a subject and a predicate. A major place where this comes up is when type\r\nchecking, as we always expect just a single triple to match `t(id, rdf:type,\r\nType)`. Other examples are list cells where we always expect a single\r\n`rdf:first` and `rdf:rest`, or arrays where we always expect a single\r\n`sys:index` and `sys:value`.\r\n\r\nAvoiding setting up a full iterator in those cases makes code go faster. So even\r\nthough it's a bit of a special case, having a function just for this makes\r\nsense.",
          "timestamp": "2022-06-01T13:20:36+02:00",
          "tree_id": "f35cc583c1bb1bf4a83dea7e5164f142eaea7d63",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/3daa90361c48afcc31c621234dfbd0a01aa0ad70"
        },
        "date": 1654083036508,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 376,
            "range": "± 65",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 42713843,
            "range": "± 5519852",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 24424755,
            "range": "± 7078058",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 32658910,
            "range": "± 10759506",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 47873907,
            "range": "± 12543002",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 43596120,
            "range": "± 8076537",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13119,
            "range": "± 1962",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 434,
            "range": "± 65",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 538,
            "range": "± 182",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1794,
            "range": "± 250",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12479,
            "range": "± 1998",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 111923,
            "range": "± 21542",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 114342,
            "range": "± 18678",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1468678,
            "range": "± 632695",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1560404,
            "range": "± 586295",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 346,
            "range": "± 40",
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
          "id": "a02053b361f2ffe814d919158cf74f1d619eabb6",
          "message": "Raise version to 0.19.8",
          "timestamp": "2022-06-01T14:15:33+02:00",
          "tree_id": "e79c810fa58e63b9697cf175752a2260b3050a32",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/a02053b361f2ffe814d919158cf74f1d619eabb6"
        },
        "date": 1654086179544,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 319,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 24748225,
            "range": "± 2425770",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12153731,
            "range": "± 2182295",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18416533,
            "range": "± 5305779",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30521072,
            "range": "± 6023552",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28341595,
            "range": "± 5105683",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10400,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 340,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 406,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1397,
            "range": "± 26",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 8924,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 83280,
            "range": "± 17796",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 90777,
            "range": "± 421",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 769458,
            "range": "± 237388",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 705263,
            "range": "± 59255",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 276,
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
          "id": "93a741e09a3293bcdd71d1a7729d7952b376beac",
          "message": "Raise version to 0.19.9",
          "timestamp": "2022-06-13T13:06:01+02:00",
          "tree_id": "fe08fb2a4c0cc3dce8117e313d84030dd239848a",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/93a741e09a3293bcdd71d1a7729d7952b376beac"
        },
        "date": 1655118843813,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 302,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 26154510,
            "range": "± 6276394",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12585639,
            "range": "± 2986180",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 16861183,
            "range": "± 2946563",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 28829224,
            "range": "± 4845678",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28238948,
            "range": "± 2992331",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10406,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 349,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 420,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1398,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 8908,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 83359,
            "range": "± 270",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 91433,
            "range": "± 245",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 659721,
            "range": "± 73906",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 679289,
            "range": "± 92620",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 278,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rrooij@users.noreply.github.com",
            "name": "Robin de Rooij",
            "username": "rrooij"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a3ee5f3541ea57c214a4f2e36e4ec5326336461a",
          "message": "Enable arm64 testing in CI (#100)\n\n* Try to run tests on arm64\r\n\r\n* Woops\r\n\r\n* Run apt non-interactively\r\n\r\n* Remove cargo package\r\n\r\n* Set env source in current shell\r\n\r\n* Use 4g at max\r\n\r\n* ci: try bullseye\r\n\r\n* Install git\r\n\r\n* Initialize cargo registry\r\n\r\n* Ignore error\r\n\r\n* Just want to know the home dir\r\n\r\n* ci: check where cargo is installed\r\n\r\n* ci: check if the volume is mounted correctly\r\n\r\n* Trying the git cli instead\r\n\r\n* ci: update with stuff\r\n\r\n* Remove arm64 on every commit but do it nightly\r\n\r\n* Actually commit the YAML\r\n\r\n* Fix weird indentation",
          "timestamp": "2022-11-07T10:25:46+01:00",
          "tree_id": "30e003292ac6aa6cec7537571f15cf964906099e",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/a3ee5f3541ea57c214a4f2e36e4ec5326336461a"
        },
        "date": 1667813558896,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 296,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 21736541,
            "range": "± 769352",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 10520391,
            "range": "± 2165539",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 14457341,
            "range": "± 5180415",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 25404217,
            "range": "± 1262324",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 26318496,
            "range": "± 1877089",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9788,
            "range": "± 264",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 319,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 385,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1382,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9175,
            "range": "± 173",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 88656,
            "range": "± 2915",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 107902,
            "range": "± 7286",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 534665,
            "range": "± 108870",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 548425,
            "range": "± 88788",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 261,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rrooij@users.noreply.github.com",
            "name": "Robin de Rooij",
            "username": "rrooij"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "17d34cd6ffb598b106415d15e63af61f9f968d20",
          "message": "Clippy comments (#103)\n\n* ci: run clippy\r\n\r\n* Run clippy separately\r\n\r\nWe don't want it running three times!\r\n\r\n* disable clippy erroring on arc ptr comparison in tests\r\n\r\nCo-authored-by: Matthijs van Otterdijk <matthijs@terminusdb.com>",
          "timestamp": "2022-12-05T12:14:16+01:00",
          "tree_id": "0161da8c9f4c87bfce1456e15b1833920555c81e",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/17d34cd6ffb598b106415d15e63af61f9f968d20"
        },
        "date": 1670239264997,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 335,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 28677167,
            "range": "± 3319391",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16915963,
            "range": "± 4062149",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22018625,
            "range": "± 4283623",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 33650872,
            "range": "± 3646407",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 33637076,
            "range": "± 4081755",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10846,
            "range": "± 98",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 408,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 470,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1582,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9977,
            "range": "± 33",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 91978,
            "range": "± 309",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 102850,
            "range": "± 995",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 880712,
            "range": "± 213196",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 948721,
            "range": "± 285560",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 345,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rrooij@users.noreply.github.com",
            "name": "Robin de Rooij",
            "username": "rrooij"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e4e7503429868d0d9f42b0cf0688f67fd01ca3fb",
          "message": "ci: rustup for GNU in Windows (#106)\n\nNeeded for GMP support",
          "timestamp": "2022-12-07T12:25:06+01:00",
          "tree_id": "6c30fcec400050240f096f603c48b40182660c50",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/e4e7503429868d0d9f42b0cf0688f67fd01ca3fb"
        },
        "date": 1670412879952,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 392,
            "range": "± 72",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 47072500,
            "range": "± 8514660",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 30744969,
            "range": "± 8573271",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 36967119,
            "range": "± 11038265",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 54062266,
            "range": "± 13947494",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 51238367,
            "range": "± 8603070",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13703,
            "range": "± 2286",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 510,
            "range": "± 118",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 587,
            "range": "± 139",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1952,
            "range": "± 494",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12201,
            "range": "± 1833",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 112634,
            "range": "± 26919",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 121622,
            "range": "± 30838",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1576487,
            "range": "± 781283",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1536752,
            "range": "± 510613",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 425,
            "range": "± 97",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ef3e544450c2bfae2e883ac23506cd588c7829a1",
          "message": "Merge pull request #109 from terminusdb/conversion_fixes\n\nfix label list retrieval for the directory store",
          "timestamp": "2023-01-04T10:21:19+01:00",
          "tree_id": "c59d9f2437eb25646324e9a5e9b081bdd3b47715",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/ef3e544450c2bfae2e883ac23506cd588c7829a1"
        },
        "date": 1672824404283,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 281,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 21659384,
            "range": "± 1587289",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 10186616,
            "range": "± 2898797",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 15419655,
            "range": "± 6796119",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 25494824,
            "range": "± 1690267",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 26768948,
            "range": "± 2030709",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10499,
            "range": "± 103",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 432,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 507,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1543,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9983,
            "range": "± 141",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 95631,
            "range": "± 888",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 104242,
            "range": "± 3462",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 562372,
            "range": "± 107408",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 566190,
            "range": "± 56645",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 371,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "695272d064a488de5541c7e4ef114bc846ce288a",
          "message": "Merge pull request #110 from terminusdb/raise_version_to_v0.19.10\n\nRaise version to v0.19.10",
          "timestamp": "2023-01-04T10:30:01+01:00",
          "tree_id": "c8718cf74a9ae5b0cc8bf616386abb2a172ecdc2",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/695272d064a488de5541c7e4ef114bc846ce288a"
        },
        "date": 1672825034883,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 369,
            "range": "± 43",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 40159320,
            "range": "± 7812860",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 25844520,
            "range": "± 8906994",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 28665730,
            "range": "± 8933545",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 42417290,
            "range": "± 5531849",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 41187560,
            "range": "± 14982720",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13799,
            "range": "± 1830",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 481,
            "range": "± 99",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 576,
            "range": "± 251",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1912,
            "range": "± 213",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12277,
            "range": "± 849",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 114611,
            "range": "± 39441",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 125316,
            "range": "± 30232",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1235696,
            "range": "± 396228",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1238126,
            "range": "± 218703",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 421,
            "range": "± 171",
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
          "id": "2d1d164e90e0b339ee737c31287dfefbaf0d1a86",
          "message": "fix pfc dict streaming (#111)\n\n* fix pfc dict streaming\r\n\r\n* reformat code\r\n\r\n* move comments\r\n\r\n* more comment",
          "timestamp": "2023-01-05T15:09:47+01:00",
          "tree_id": "3fd5201e548294c65b04139be37cc5ae518612ef",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/2d1d164e90e0b339ee737c31287dfefbaf0d1a86"
        },
        "date": 1672928131809,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 319,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 26081630,
            "range": "± 2402963",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12860877,
            "range": "± 2204988",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18007459,
            "range": "± 3885260",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30029294,
            "range": "± 6297891",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 28976182,
            "range": "± 3820746",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11433,
            "range": "± 80",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 422,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 495,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1634,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10526,
            "range": "± 50",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 97743,
            "range": "± 369",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 102574,
            "range": "± 319",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 693928,
            "range": "± 172605",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 678639,
            "range": "± 187990",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 360,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5f6dfa88ac777f7b31685f80c3c2a50cff441435",
          "message": "Merge pull request #112 from terminusdb/raise_version_to_0.19.11\n\nRaise version to v0.19.11",
          "timestamp": "2023-01-05T15:23:37+01:00",
          "tree_id": "af9e21d4df5c35f685e1adf53304f8a191a83122",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/5f6dfa88ac777f7b31685f80c3c2a50cff441435"
        },
        "date": 1672928949928,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 285,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 25805106,
            "range": "± 11322861",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 11508030,
            "range": "± 3443011",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 20614392,
            "range": "± 8607797",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 30340736,
            "range": "± 12737716",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30638782,
            "range": "± 12796245",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9977,
            "range": "± 188",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 413,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 492,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1493,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9591,
            "range": "± 127",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 92101,
            "range": "± 2667",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 100966,
            "range": "± 1513",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1004562,
            "range": "± 600792",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1061643,
            "range": "± 671266",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 356,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "76c56d077a8f9d2e054376d50ac9a57c2b2d4354",
          "message": "Merge pull request #113 from terminusdb/archive_format\n\nArchive format",
          "timestamp": "2023-01-13T12:43:03+01:00",
          "tree_id": "795e9785f9a227f1dd340683eb6bc4865b80be37",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/76c56d077a8f9d2e054376d50ac9a57c2b2d4354"
        },
        "date": 1673610745796,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 390,
            "range": "± 62",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 34765123,
            "range": "± 7465373",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 24701419,
            "range": "± 8555627",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 31145047,
            "range": "± 8097126",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 40056411,
            "range": "± 6735106",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 36970713,
            "range": "± 7963220",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12325,
            "range": "± 3003",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 422,
            "range": "± 156",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 503,
            "range": "± 73",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1682,
            "range": "± 423",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10736,
            "range": "± 2621",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 103579,
            "range": "± 44898",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 110880,
            "range": "± 19141",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1115514,
            "range": "± 254110",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1125011,
            "range": "± 417445",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 350,
            "range": "± 78",
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
          "id": "82cf4a192b9113f9bdcddc1f9fb8024a2757197f",
          "message": "Call finalize on layers after unpacking them (#114)",
          "timestamp": "2023-01-13T15:43:51+01:00",
          "tree_id": "d4ab14e23df5fcbffd9fd7993c1394de99737051",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/82cf4a192b9113f9bdcddc1f9fb8024a2757197f"
        },
        "date": 1673621466986,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 315,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 22032653,
            "range": "± 1118958",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 12237005,
            "range": "± 3059365",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 17472666,
            "range": "± 4883706",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 26205485,
            "range": "± 1772717",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 23639388,
            "range": "± 2423618",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9874,
            "range": "± 66",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 409,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 480,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1479,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9421,
            "range": "± 42",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 91389,
            "range": "± 604",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 101300,
            "range": "± 1521",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 606797,
            "range": "± 71651",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 621532,
            "range": "± 65079",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 357,
            "range": "± 4",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "72d20d4a14a991b7aa042311c36b44ddec8bbf27",
          "message": "Merge pull request #115 from terminusdb/increase_version\n\nIncrease version",
          "timestamp": "2023-01-13T15:48:01+01:00",
          "tree_id": "526e068d91303f06f198210dec72811c22afe974",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/72d20d4a14a991b7aa042311c36b44ddec8bbf27"
        },
        "date": 1673621722138,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 312,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 30373448,
            "range": "± 9702522",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 21416572,
            "range": "± 8185909",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 23968742,
            "range": "± 8463884",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 32771127,
            "range": "± 6594923",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 29986570,
            "range": "± 8921238",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9853,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 403,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 469,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1474,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9469,
            "range": "± 100",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 91009,
            "range": "± 703",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 99976,
            "range": "± 850",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 715074,
            "range": "± 304338",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 845465,
            "range": "± 478531",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 354,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "74c37e98392d58f025e3e86c1e7cbaae2f1073a2",
          "message": "Fix marshalling of dates in and out, add tests (#116)",
          "timestamp": "2023-01-16T10:26:04+01:00",
          "tree_id": "882757b3a049bb7f36ac82ed333615106fb2dc4b",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/74c37e98392d58f025e3e86c1e7cbaae2f1073a2"
        },
        "date": 1673861609725,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 337,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 28578486,
            "range": "± 6101788",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16388324,
            "range": "± 6843117",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22906891,
            "range": "± 8327633",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 33983244,
            "range": "± 6500739",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 30790448,
            "range": "± 8545618",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11210,
            "range": "± 62",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 400,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 468,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1591,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10320,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 96307,
            "range": "± 342",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 102456,
            "range": "± 326",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 899758,
            "range": "± 455142",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 835551,
            "range": "± 344038",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 337,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0c30dfd1d17b250e60f563debd371d5a347c44eb",
          "message": "Add rc3 (#117)",
          "timestamp": "2023-01-16T10:28:58+01:00",
          "tree_id": "1953b4374acddf91f2efaf596979af9df45db81d",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/0c30dfd1d17b250e60f563debd371d5a347c44eb"
        },
        "date": 1673861825347,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 350,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 24792581,
            "range": "± 3617686",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 15526129,
            "range": "± 5360676",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19822775,
            "range": "± 3895769",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 28798586,
            "range": "± 3019000",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 27580852,
            "range": "± 5578868",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11099,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 399,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 468,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1577,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10026,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 92827,
            "range": "± 346",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 102366,
            "range": "± 737",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 755045,
            "range": "± 167901",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 715301,
            "range": "± 203759",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 342,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "70e294ab36b0d1bdec75895e26fba46cc72220e3",
          "message": "Merge pull request #118 from terminusdb/fix_durations\n\nFix durations",
          "timestamp": "2023-01-17T18:48:03+01:00",
          "tree_id": "d8345ead9dd0f83863d4fa9b446bc77243d6f4c1",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/70e294ab36b0d1bdec75895e26fba46cc72220e3"
        },
        "date": 1673978308863,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 410,
            "range": "± 67",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 41717082,
            "range": "± 5938565",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 28119918,
            "range": "± 6778845",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 36422889,
            "range": "± 7092655",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 50537170,
            "range": "± 7213091",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 44625243,
            "range": "± 8318045",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13240,
            "range": "± 1490",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 511,
            "range": "± 59",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 598,
            "range": "± 125",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1935,
            "range": "± 315",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12018,
            "range": "± 1353",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 111279,
            "range": "± 13161",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 123019,
            "range": "± 11402",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1355008,
            "range": "± 286506",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1328984,
            "range": "± 232098",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 435,
            "range": "± 71",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3ffdce42a34c6ea86934e5673b60b8febdb00902",
          "message": "Merge pull request #119 from terminusdb/raise_version_to_0.20.0-rc4\n\nRaise version to 0.20.0-rc4",
          "timestamp": "2023-01-17T18:48:22+01:00",
          "tree_id": "fb5349fd141d49bc76e1a48942df1d096b141088",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/3ffdce42a34c6ea86934e5673b60b8febdb00902"
        },
        "date": 1673978372196,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 404,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 35324933,
            "range": "± 10065752",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 22232495,
            "range": "± 8133203",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 29310798,
            "range": "± 10395437",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 44675296,
            "range": "± 11804642",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 40383591,
            "range": "± 9004026",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13537,
            "range": "± 1023",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 493,
            "range": "± 80",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 575,
            "range": "± 60",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1939,
            "range": "± 193",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12487,
            "range": "± 871",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 114626,
            "range": "± 8774",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 123005,
            "range": "± 5926",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1112951,
            "range": "± 532037",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 872137,
            "range": "± 406097",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 418,
            "range": "± 8",
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
          "id": "18a11dc73bdfbe82eabb814970f6e9024a5bcad2",
          "message": "Raise version to v0.20.0 (#121)",
          "timestamp": "2023-01-25T15:27:57+01:00",
          "tree_id": "62972f913d961126d70d1f1af06e486511bbd1c0",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/18a11dc73bdfbe82eabb814970f6e9024a5bcad2"
        },
        "date": 1674657515169,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 418,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 44081486,
            "range": "± 5766141",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 27845978,
            "range": "± 5300214",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 38657973,
            "range": "± 8977954",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 50527798,
            "range": "± 8657748",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 46571867,
            "range": "± 7805951",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 14204,
            "range": "± 712",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 551,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 644,
            "range": "± 84",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 2039,
            "range": "± 250",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 13035,
            "range": "± 1479",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 122288,
            "range": "± 17904",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 131210,
            "range": "± 15250",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1419832,
            "range": "± 287318",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1454597,
            "range": "± 527531",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 463,
            "range": "± 38",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1355f5919dc5f07da45b1631c284b361384522c9",
          "message": "Merge pull request #122 from terminusdb/update_readme\n\nupdate readme with upgrade instructions",
          "timestamp": "2023-01-25T15:38:13+01:00",
          "tree_id": "3a0e816415fab797637ce91643557b3bb9da6f71",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/1355f5919dc5f07da45b1631c284b361384522c9"
        },
        "date": 1674657952483,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 308,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 27005022,
            "range": "± 6822765",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 18837530,
            "range": "± 7280326",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 21991903,
            "range": "± 6303988",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 27077083,
            "range": "± 8466581",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 26627555,
            "range": "± 6068675",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 10455,
            "range": "± 184",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 426,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 492,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1538,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9813,
            "range": "± 112",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 95246,
            "range": "± 1817",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 102410,
            "range": "± 1917",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 789107,
            "range": "± 408258",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 756106,
            "range": "± 151603",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 373,
            "range": "± 1",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f916464afa496568c2d921aedf66e3eda800d3d6",
          "message": "Merge pull request #125 from terminusdb/nolock_label_store\n\na caching label store",
          "timestamp": "2023-01-31T11:17:55+01:00",
          "tree_id": "d46c8d69f844d8998c80c97f722efffcde35f34d",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/f916464afa496568c2d921aedf66e3eda800d3d6"
        },
        "date": 1675160870407,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 392,
            "range": "± 74",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 36150972,
            "range": "± 10018095",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 24642181,
            "range": "± 6364595",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 36450962,
            "range": "± 13336280",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 39831279,
            "range": "± 9012733",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 37801884,
            "range": "± 8497408",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13357,
            "range": "± 668",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 474,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 572,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1899,
            "range": "± 246",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12374,
            "range": "± 931",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 115389,
            "range": "± 5336",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 124570,
            "range": "± 17267",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1212130,
            "range": "± 377334",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1344906,
            "range": "± 648401",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 399,
            "range": "± 57",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "likelion@gmail.com",
            "name": "Leonid Mokrushin",
            "username": "likelion"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1f776ab75111e556ad08b57e42110aa55935cffd",
          "message": "Added a method to enumerate names of all existing databases (#124)\n\n* Added a method to enumerate names of all existing databases\r\n\r\n* Added database listing unit test\r\n\r\n---------\r\n\r\nCo-authored-by: Matthijs van Otterdijk <matthijs@terminusdb.com>",
          "timestamp": "2023-03-15T17:28:02+01:00",
          "tree_id": "4190064be852c64838764af5dc1cfffa6c3c9d75",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/1f776ab75111e556ad08b57e42110aa55935cffd"
        },
        "date": 1678898129561,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 319,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 22750488,
            "range": "± 3878721",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 16207445,
            "range": "± 3403831",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 22078598,
            "range": "± 5125927",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 27634175,
            "range": "± 4293721",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 26950095,
            "range": "± 4601904",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9657,
            "range": "± 314",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 415,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 484,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1492,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9483,
            "range": "± 322",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 91939,
            "range": "± 1315",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 101854,
            "range": "± 1572",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 670222,
            "range": "± 308815",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 861473,
            "range": "± 241806",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 367,
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
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a5f6d868cc35768b4cdf65c143a336ffe7ea4154",
          "message": "Raise version to 0.20.1 (#127)",
          "timestamp": "2023-03-15T17:45:02+01:00",
          "tree_id": "4db62119f13e6e437b1cde03a88cca5a1be569d0",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/a5f6d868cc35768b4cdf65c143a336ffe7ea4154"
        },
        "date": 1678899127702,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 307,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 22485905,
            "range": "± 3635378",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 14892830,
            "range": "± 2528737",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 19818950,
            "range": "± 4780059",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 25960548,
            "range": "± 3715689",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 24981310,
            "range": "± 3927170",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9778,
            "range": "± 248",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 410,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 477,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1457,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9312,
            "range": "± 150",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 92929,
            "range": "± 1737",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 102022,
            "range": "± 1634",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 766093,
            "range": "± 156371",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 764888,
            "range": "± 265580",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 360,
            "range": "± 2",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "150a55ef262f065dd3cf0601882c8648c918a08c",
          "message": "Merge pull request #128 from terminusdb/layer_load_store_backend\n\nLayer load store backend with lru caching",
          "timestamp": "2023-03-24T14:11:45+01:00",
          "tree_id": "bced1f60aa03f90706f4ce76fe63f655f153922e",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/150a55ef262f065dd3cf0601882c8648c918a08c"
        },
        "date": 1679664015536,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 406,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 28239584,
            "range": "± 4240946",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 21337414,
            "range": "± 3955366",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 27184110,
            "range": "± 4469980",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 35780547,
            "range": "± 10408686",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 32881413,
            "range": "± 7051831",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13119,
            "range": "± 616",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 482,
            "range": "± 59",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 578,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1757,
            "range": "± 133",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11741,
            "range": "± 251",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 110421,
            "range": "± 3799",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 123228,
            "range": "± 4040",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 958080,
            "range": "± 175453",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 978657,
            "range": "± 224322",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 423,
            "range": "± 18",
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
          "id": "a66600996c329007a81a6197f2ea02ee81e71c8e",
          "message": "Squash and squash_upto implemented more lowlevel (#129)\n\n* starting to implement squash upto\r\n\r\n* reimplement squash to use internal builders\r\n\r\n* untested squash_upto\r\n\r\n* working rollup upto\r\n\r\n* sort triples before squashing them\r\n\r\n* fix squashes of rollup values + tests\r\n\r\n* also fix rollup squash in squash_upto\r\n\r\n* reformat\r\n\r\n---------\r\n\r\nCo-authored-by: Gavin Mendel-Gleason <gavin@terminusdb.com>",
          "timestamp": "2023-03-24T14:55:37+01:00",
          "tree_id": "729c29aee33d27e8137d5fa75e47661df116dc2a",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/a66600996c329007a81a6197f2ea02ee81e71c8e"
        },
        "date": 1679666563997,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 311,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 20131851,
            "range": "± 1912483",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 13694728,
            "range": "± 2161066",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 18668616,
            "range": "± 3890417",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 24259706,
            "range": "± 2151769",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 23473053,
            "range": "± 4261356",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9889,
            "range": "± 194",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 416,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 483,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1391,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9381,
            "range": "± 121",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 92720,
            "range": "± 2667",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 101899,
            "range": "± 1536",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 697735,
            "range": "± 193800",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 703046,
            "range": "± 122469",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 361,
            "range": "± 1",
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
          "id": "dff58eb619165d37c71cabe1c02934b1876fc600",
          "message": "delete unused dict entries in a squash (#131)\n\n* delete unused dict entries in a squash\r\n\r\n* reorder operations for slightly better memory use\r\n\r\n* various optimizations\r\n\r\n* further optimizations\r\n\r\n* remove superfluous prints\r\n\r\n* refactor squash_upto for more predictable memory use\r\n\r\n* tests and fixes for squash\r\n\r\n* fix test\r\n\r\n* remove unnecessary reserves\r\n\r\n* skip over removals when preserving dict entries in squash upto\r\n\r\n* keep track of triple change count correctly",
          "timestamp": "2023-04-03T18:18:20+02:00",
          "tree_id": "bc3c3b7db4cb99131fac8826d4ba4fcfb5a142be",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/dff58eb619165d37c71cabe1c02934b1876fc600"
        },
        "date": 1680539233142,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 382,
            "range": "± 123",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 37589908,
            "range": "± 11072244",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 28095248,
            "range": "± 9302819",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 36166859,
            "range": "± 11168602",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 44611709,
            "range": "± 11100449",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 39793842,
            "range": "± 11622814",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13128,
            "range": "± 688",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 492,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 579,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1781,
            "range": "± 103",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11889,
            "range": "± 1075",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 113055,
            "range": "± 6235",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 122093,
            "range": "± 3841",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1293826,
            "range": "± 603712",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1212410,
            "range": "± 517419",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 424,
            "range": "± 97",
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
          "id": "f2196ba9a4689efea1698824dfca0187815d7c49",
          "message": "use less memory in builders (#132)\n\n* complete builder rewrite for better memory behavior\r\n\r\n* dedup while adding triples to the simple builder\r\n\r\n* use archive store for the builder benchmarks\r\n\r\n* change loop strategy for iterator",
          "timestamp": "2023-04-05T11:15:45+02:00",
          "tree_id": "6202d22f5d167b84557e2adf68568be51a4fe7f1",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/f2196ba9a4689efea1698824dfca0187815d7c49"
        },
        "date": 1680686571752,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 1057,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 10607274,
            "range": "± 868752",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 1410070,
            "range": "± 747438",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 1456064,
            "range": "± 600655",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 10774338,
            "range": "± 1135968",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 6945204,
            "range": "± 903797",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11101,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 409,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 478,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1488,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9936,
            "range": "± 42",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 94101,
            "range": "± 446",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 101903,
            "range": "± 254",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 964109,
            "range": "± 451521",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 990934,
            "range": "± 405420",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 348,
            "range": "± 1",
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
          "id": "1b97b795c84b0d17975cd332c5c7a4ba42a3e369",
          "message": "Raise version to 0.21.0 (#133)",
          "timestamp": "2023-04-12T12:03:16+02:00",
          "tree_id": "7c67d8ccecf7212625abc05b2780f75b9392f836",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/1b97b795c84b0d17975cd332c5c7a4ba42a3e369"
        },
        "date": 1681294289381,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 1131,
            "range": "± 137",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 11973262,
            "range": "± 1275282",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 1274762,
            "range": "± 525125",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 1417692,
            "range": "± 559856",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 12023710,
            "range": "± 1439197",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 7713052,
            "range": "± 648336",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13163,
            "range": "± 430",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 492,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 582,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1785,
            "range": "± 64",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11749,
            "range": "± 310",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 111285,
            "range": "± 2972",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 122431,
            "range": "± 2755",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 916020,
            "range": "± 169654",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 893144,
            "range": "± 196601",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 419,
            "range": "± 67",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cf16308249032f6dfdb74a1eca13de896c650b0a",
          "message": "Merge pull request #135 from terminusdb/graph_merge\n\nbase graph merge",
          "timestamp": "2023-06-26T13:38:09+02:00",
          "tree_id": "c37b2aeeb9152e1a4880e0d24a741cf56ef0020f",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/cf16308249032f6dfdb74a1eca13de896c650b0a"
        },
        "date": 1687780091989,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 1333,
            "range": "± 314",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 12537705,
            "range": "± 1703680",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 2042526,
            "range": "± 539954",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 2018022,
            "range": "± 866821",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 12593335,
            "range": "± 2007203",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 8532070,
            "range": "± 1295658",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12992,
            "range": "± 1456",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 524,
            "range": "± 56",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 616,
            "range": "± 77",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1911,
            "range": "± 120",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12621,
            "range": "± 1790",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 118614,
            "range": "± 13248",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 130250,
            "range": "± 10750",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1327683,
            "range": "± 312188",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1307972,
            "range": "± 306905",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 451,
            "range": "± 38",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "87fe839fbc0c367601328dc9d5b486fda3fa396a",
          "message": "Merge pull request #138 from terminusdb/sync_directories\n\nEnsure that all metadata about new layers is written to the fs",
          "timestamp": "2023-07-17T16:36:06+02:00",
          "tree_id": "569c06b0eb2eb206f88053857573de72b6489d38",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/87fe839fbc0c367601328dc9d5b486fda3fa396a"
        },
        "date": 1689605128061,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 1224,
            "range": "± 350",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 13151418,
            "range": "± 1870570",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 2223968,
            "range": "± 718687",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 2260255,
            "range": "± 1203193",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 12951984,
            "range": "± 1764361",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 8053540,
            "range": "± 1731086",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12741,
            "range": "± 2070",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 475,
            "range": "± 118",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 525,
            "range": "± 220",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1718,
            "range": "± 416",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12385,
            "range": "± 2475",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 114280,
            "range": "± 33184",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 121367,
            "range": "± 39080",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1298286,
            "range": "± 627735",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1394958,
            "range": "± 853239",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 427,
            "range": "± 166",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9813a7ae6e9bed1549b5308ee27e3acd804dab3f",
          "message": "Add scientific notation to terminus store (#136)\n\n* Add scientific notation to terminus store\r\n\r\n* Remove extraneous eprintln!s\r\n\r\n* Fix big float to accept +",
          "timestamp": "2023-07-17T20:56:44+02:00",
          "tree_id": "f4924667782db6a9d1503d64f348f2b9df4622db",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/9813a7ae6e9bed1549b5308ee27e3acd804dab3f"
        },
        "date": 1689620648424,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 974,
            "range": "± 193",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 9943553,
            "range": "± 759030",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 905951,
            "range": "± 411680",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 1350188,
            "range": "± 458206",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 10227132,
            "range": "± 801015",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 6651604,
            "range": "± 617301",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 9655,
            "range": "± 125",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 404,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 464,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1388,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 9341,
            "range": "± 183",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 90476,
            "range": "± 1713",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 99478,
            "range": "± 2136",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 805371,
            "range": "± 237161",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 875117,
            "range": "± 273027",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 357,
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
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "87a195c25f02930893ee07bb381ff83a53c4d885",
          "message": "raise version to 0.21.1 (#139)",
          "timestamp": "2023-07-18T10:24:24+02:00",
          "tree_id": "9b5a679586fe212a8196a026548924284c4a78c8",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/87a195c25f02930893ee07bb381ff83a53c4d885"
        },
        "date": 1689669195760,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 1125,
            "range": "± 159",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 13133968,
            "range": "± 2880702",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 2170053,
            "range": "± 1164517",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 2375520,
            "range": "± 1334198",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 13501706,
            "range": "± 3273768",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 8672635,
            "range": "± 1533894",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 13027,
            "range": "± 462",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 489,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 569,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1792,
            "range": "± 122",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 12057,
            "range": "± 753",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 109950,
            "range": "± 8410",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 122352,
            "range": "± 9032",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1447634,
            "range": "± 696615",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1439139,
            "range": "± 724563",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 415,
            "range": "± 11",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "gavin@terminusdb.com",
            "name": "Gavin Mendel-Gleason",
            "username": "GavinMendelGleason"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "03fa9a4583a08a2fa80ea4a383e3404c76ec6258",
          "message": "Merge pull request #140 from terminusdb/rollup_file_synchronization\n\nuse locks and syncs for rollup file synchronization",
          "timestamp": "2023-07-19T16:39:05+02:00",
          "tree_id": "12f30b9ed54b6549ed9f1bb877273d19cb6234e9",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/03fa9a4583a08a2fa80ea4a383e3404c76ec6258"
        },
        "date": 1689777995729,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 985,
            "range": "± 53",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 10661920,
            "range": "± 385954",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 1124445,
            "range": "± 319163",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 1152770,
            "range": "± 225939",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 10755100,
            "range": "± 757175",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 6854479,
            "range": "± 614406",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 11003,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 403,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 469,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1538,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 10152,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 93146,
            "range": "± 706",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 102862,
            "range": "± 184",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 681377,
            "range": "± 98219",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 674023,
            "range": "± 152429",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 340,
            "range": "± 1",
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
          "id": "f807c741bfdbb59cf156319e5e1026d5082c8935",
          "message": "More buf builders (#144)\n\n* bitarray bufbuilder\r\n\r\n* adjacency list buf builder\r\n\r\n* resolve warnings in test compilation\r\n\r\n* removed unnecessary lifetimes\r\n\r\n* conform to clippy expectations in ci",
          "timestamp": "2023-09-15T11:31:32+02:00",
          "tree_id": "f058e4e415e7ce428dc098b04c14df1fb827de69",
          "url": "https://github.com/terminusdb/terminusdb-store/commit/f807c741bfdbb59cf156319e5e1026d5082c8935"
        },
        "date": 1694770828603,
        "tool": "cargo",
        "benches": [
          {
            "name": "bench_add_string_triple",
            "value": 1193,
            "range": "± 167",
            "unit": "ns/iter"
          },
          {
            "name": "build_base_layer_1000",
            "value": 11291924,
            "range": "± 1294431",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_base_layer",
            "value": 1904254,
            "range": "± 297316",
            "unit": "ns/iter"
          },
          {
            "name": "build_empty_child_layer_on_empty_base_layer",
            "value": 1849132,
            "range": "± 510796",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_empty_base_layer",
            "value": 11858312,
            "range": "± 1842037",
            "unit": "ns/iter"
          },
          {
            "name": "build_nonempty_child_layer_on_nonempty_base_layer",
            "value": 7682910,
            "range": "± 731443",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w10_1000",
            "value": 12011,
            "range": "± 1460",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1",
            "value": 439,
            "range": "± 56",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10",
            "value": 522,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_100",
            "value": 1634,
            "range": "± 169",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_1000",
            "value": 11122,
            "range": "± 1888",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000",
            "value": 102393,
            "range": "± 8039",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_as_stream",
            "value": 111883,
            "range": "± 10761",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent",
            "value": 1337633,
            "range": "± 278748",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_10000_persistent_as_stream",
            "value": 1144657,
            "range": "± 88193",
            "unit": "ns/iter"
          },
          {
            "name": "logarray_w5_empty",
            "value": 363,
            "range": "± 35",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}