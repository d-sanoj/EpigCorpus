# Phase 0 environment capture

captured: 2026-08-20T18:42:39Z
host_os: Darwin Macbook-Pro-M5.local 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:16:20 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8142 arm64

## git
commit: 537c15948ace83b8025c5af5cc1b428dd151d317
branch: main
dirty (tracked files): 0

## python
python: 3.13.13 (main, Apr 14 2026, 14:32:41) [Clang 22.1.3 ]
impl: CPython
exe: /Users/sanoj/Documents/Projects/EpigCorpus/.venv/bin/python

## packages (uv.lock is authoritative; installed set below)
    altair==6.1.0
    anyio==4.13.0
    attrs==26.1.0
    blinker==1.9.0
    branca==0.8.2
    cachetools==7.1.4
    certifi==2026.2.25
    charset-normalizer==3.4.6
    click==8.4.1
    contourpy==1.3.3
    cycler==0.12.1
    folium==0.20.0
    fonttools==4.63.0
    geopandas==1.1.3
    gitdb==4.0.12
    GitPython==3.1.50
    h11==0.16.0
    httptools==0.8.0
    idna==3.11
    iniconfig==2.3.0
    itsdangerous==2.2.0
    Jinja2==3.1.6
    jsonschema==4.26.0
    jsonschema-specifications==2025.9.1
    kiwisolver==1.5.0
    MarkupSafe==3.0.3
    matplotlib==3.10.9
    narwhals==2.22.0
    numpy==2.4.3
    packaging==26.2
    pandas==3.0.1
    pillow==12.2.0
    pluggy==1.6.0
    protobuf==7.35.0
    pyarrow==24.0.0
    pydeck==0.9.2
    Pygments==2.21.0
    pyogrio==0.12.1
    pyparsing==3.3.2
    pyproj==3.7.2
    pytest==9.1.1
    python-dateutil==2.9.0.post0
    python-multipart==0.0.30
    referencing==0.37.0
    requests==2.33.0
    rpds-py==2026.5.1
    ruff==0.16.3
    shapely==2.1.2
    six==1.17.0
    smmap==5.0.3
    starlette==1.2.1
    streamlit==1.58.0
    tenacity==9.1.4
    toml==0.10.2
    typing_extensions==4.15.0
    urllib3==2.6.3
    uvicorn==0.48.0
    websockets==16.0
    xyzservices==2026.3.0


---

## Phase 5 environment addition — recorded 2026-08-20T20:59:54Z

The ML stack was absent at the end of Phase 4 (numpy and pandas only) and
was installed as a separate `ml` dependency group so the scraper and the
Streamlit map keep their light dependency set.

```
ml = [
    "protobuf>=7.35.0",
    "scikit-learn>=1.9.0",
    "sentencepiece>=0.2.2",
    "tokenizers>=0.22.2",
    "torch>=2.13.0",
    "transformers>=5.15.1",
]
```

Resolved versions:

    torch==2.13.0
    transformers==5.15.1
    tokenizers==0.22.2
    scikit-learn==1.9.0
    sentencepiece==0.2.2
    protobuf==7.35.0
    numpy==2.4.3
    pandas==3.0.1

    accelerator: MPS available=True  CUDA available=False  torch threads=4
    platform: macOS-26.6.2-arm64-arm-64bit-Mach-O  arm

Model host reachability, checked before building around it as the brief
requires: pypi.org 200, huggingface.co 200, github.com 200. **The brief's
fallback condition (host unreachable) is NOT triggered**, so the
from-scratch transformer path is not taken.

| model | role | params | licence | loads without remote code |
| --- | --- | --- | --- | --- |
| `bowphs/LaBerta` | M3 | 125,978,112 | apache-2.0 | yes, verified |
| `google-bert/bert-base-multilingual-cased` | M3 control | 177,853,440 | apache-2.0 | yes, verified |
| `latincy/latin-bert` | rejected | ~110M | apache-2.0 | **no** — needs `trust_remote_code=True` |

