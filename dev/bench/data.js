window.BENCHMARK_DATA = {
  "lastUpdate": 1617172789926,
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
      }
    ]
  }
}