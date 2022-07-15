from overrides import overrides
from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame
from typing import Dict, Optional, Any
from pyspark.sql import functions as sf
from pyspark.sql.column import Column
from itertools import chain


class BucketMap(Bucketizer):
    mapping: Dict[int, Column]

    def __init__(self, mapping: Dict[int, Column], *args, **kwargs):
        super(BucketMap, self).__init__(*args, **kwargs)

        n_splits: int = len(self.getSplits())
        n_buckets: int = n_splits - 1
        n_mappings: int = len(mapping)
        assert n_mappings == n_buckets, (
            "there must be exactly 1 mapping for each bucket ("
            + f"input was {n_splits} splits, making for {n_buckets} buckets. "
            + f"{n_buckets} mappings expected but {n_mappings} were given.)"
        )

        self.mapping = mapping

    @overrides
    def transform(self, dataset: DataFrame, params: Optional[Any] = None) -> DataFrame:
        # Run bucketizer
        bucketed: DataFrame = super().transform(dataset, params)
        buckets: Column = bucketed[self.getOutputCol()]

        # Map buckets to their desired values
        mapping: Dict[int, Column] = self.mapping
        range_map: chain = chain(*mapping.items())
        range_mapper: Column = sf.create_map([sf.lit(x) for x in range_map])
        with_ranges: DataFrame = bucketed.withColumn(
            self.getOutputCol(), range_mapper[buckets]
        )

        return with_ranges
