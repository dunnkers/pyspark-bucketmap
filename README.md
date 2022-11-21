# pyspark-bucketmap
[![build status](https://github.com/dunnkers/pyspark-bucketmap/actions/workflows/python-app.yml/badge.svg)](https://github.com/dunnkers/pyspark-bucketmap/actions/workflows/python-app.yml) [![pypi badge](https://img.shields.io/pypi/v/pyspark-bucketmap.svg?maxAge=3600)](https://pypi.org/project/pyspark-bucketmap/) [![Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![Open in Remote - Containers](https://img.shields.io/static/v1?label=Remote%20-%20Containers&message=Open&color=blue&logo=visualstudiocode)](https://vscode.dev/redirect?url=vscode://ms-vscode-remote.remote-containers/cloneInVolume?url=https://github.com/dunnkers/pyspark-bucketmap)

`pyspark-bucketmap` is a tiny module for **pyspark** which allows you to bucketize DataFrame rows and map their values easily.

## Install

```shell
pip install pyspark-bucketmap
```

## Usage

```python
from pyspark.sql import Row

people = spark.createDataFrame(
    [
        Row(age=12, name="Damian"),
        Row(age=15, name="Jake"),
        Row(age=18, name="Dominic"),
        Row(age=20, name="John"),
        Row(age=27, name="Jerry"),
        Row(age=101, name="Jerry's Grandpa"),
    ]
)
people
```

Now, what we would like to do, is map each person's age to an age category.

|age range|life phase|
|-|-|
|0 to 12|Child|
|12 to 18|Teenager|
|18 to 25|Young adulthood|
|25 to 70|Adult|
|70 and beyond|Elderly|

We can use `pyspark-bucketmap` for this. First, define the splits and mappings:

```python
from typing import List

splits: List[float] = [-float("inf"), 0, 12, 18, 25, 70, float("inf")]
mapping: Dict[int, Column] = {
    0: lit("Not yet born"),
    1: lit("Child"),
    2: lit("Teenager"),
    3: lit("Young adulthood"),
    4: lit("Adult"),
    5: lit("Elderly"),
}
```

Then, apply `BucketMap.transform(df)`:

```python
from pyspark_bucketmap import BucketMap
from typing import List, Dict

bucket_mapper = BucketMap(
    splits=splits, mapping=mapping, inputCol="age", outputCol="phase"
)
phases_actual: DataFrame = bucket_mapper.transform(people).select("name", "phase")
phases_actual.show()
```

|           name|          phase|
|-|-|
|         Damian|       Teenager|
|           Jake|       Teenager|
|        Dominic|Young adulthood|
|           John|Young adulthood|
|          Jerry|          Adult|
|Jerry's Grandpa|        Elderly|

Success!

✨

## API

Module `pyspark_bucketmap`:

```python
from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame
from pyspark.sql.column import Column

class BucketMap(Bucketizer):
    mapping: Dict[int, Column]

    def __init__(self, mapping: Dict[int, Column], *args, **kwargs):
        ...

    def transform(self, dataset: DataFrame, params: Optional[Any] = None) -> DataFrame:
        ...
```

## Contributing
Under the hood, uses a combination of pyspark's `Bucketizer` and `pyspark.sql.functions.create_map`. The code is 42 lines and exists 1 in file: `pyspark_bucketmap.py`. To contribute, follow your preferred setup option below.

## Option A: using a Devcontainer (VSCode only)
If you happen to use VSCode as your editor, you can open `pyspark-bucketmap` in a [**Devcontainer**](https://code.visualstudio.com/docs/remote/containers). Devcontainers allow you to develop _inside_ a Docker container - which means all dependencies and packages are automatically set up for you. First, make sure you have the [Remote Development extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack) installed.


Then, you can do two things.

1. Click the following button:

    [![Open in Remote - Containers](https://img.shields.io/static/v1?label=Remote%20-%20Containers&message=Open&color=blue&logo=visualstudiocode)](https://vscode.dev/redirect?url=vscode://ms-vscode-remote.remote-containers/cloneInVolume?url=https://github.com/dunnkers/pyspark-bucketmap)

1. Or, clone and open up the repo in VSCode:

    ```shell
    git clone https://github.com/dunnkers/pyspark-bucketmap.git
    code pyspark-bucketmap
    ```

    (for this to work, make sure you activated VSCode's [`code`](https://code.visualstudio.com/docs/editor/command-line) CLI)

    Then, you should see the following notification:

    ![reopen in devcontainer](https://github.com/dunnkers/fseval/blob/master/website/static/img/contributing/reopen-in-devcontainer.png?raw=true)

Now you should have a fully working dev environment working 🙌🏻. You can run tests, debug code, etcetera. All dependencies are automatically installed for you.

🙌🏻

### Option B: installing the dependencies manually
Clone the repo and install the deps:

```shell
git clone https://github.com/dunnkers/pyspark-bucketmap.git
cd pyspark-bucketmap
pip install -r .devcontainer/requirements.txt
pip install -r .devcontainer/requirements-dev.txt
pip install .
```

Make sure you also have the following installed:
- Python 3.9
- OpenJDK version 11

Now, you should be able to run tests 🧪:

```shell
pytest .
```

🙌🏻

## About
Created by [Jeroen Overschie](https://jeroenoverschie.nl/) © 2022.
