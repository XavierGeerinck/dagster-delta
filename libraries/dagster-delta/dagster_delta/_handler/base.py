import logging
from abc import abstractmethod
from typing import Any, Generic, Optional, TypeVar, Union, cast, Dict

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from dagster import (
    InputContext,
    MetadataValue,
    OutputContext,
    TableColumn,
    TableSchema,
)
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from deltalake import CommitProperties, DeltaTable, WriterProperties, write_deltalake
from deltalake.exceptions import TableNotFoundError
from deltalake.schema import Schema, _convert_pa_schema_to_delta
from deltalake.table import FilterLiteralType

from dagster_delta._handler.merge import merge_execute
from dagster_delta._handler.utils import (
    create_predicate,
    extract_date_format_from_partition_definition,
    partition_dimensions_to_dnf,
    read_table,
)
from dagster_delta.config import MergeConfig
from dagster_delta.io_manager.base import (
    TableConnection,
    _DeltaTableIOManagerResourceConfig,
)

T = TypeVar("T")
ArrowTypes = Union[pa.Table, pa.RecordBatchReader, ds.Dataset]


class DeltalakeBaseArrowTypeHandler(DbTypeHandler[T], Generic[T]):
    """Base TypeHandler implementation for arrow supported libraries used to handle deltalake IO."""

    @abstractmethod
    def from_arrow(self, obj: pa.RecordBatchReader, target_type: type) -> T:
        """Abstract method to convert arrow to target type"""
        pass

    @abstractmethod
    def to_arrow(self, obj: T) -> tuple[ArrowTypes, dict[str, Any]]:
        """Abstract method to convert type to arrow"""
        pass

    @abstractmethod
    def get_output_stats(self, obj: T) -> dict[str, MetadataValue]:
        """Abstract method to return output stats"""
        pass

    def _find_keys_in_metadata(
        self,
        context: OutputContext,
        keys=["merge_predicate", "merge_operations_config"],
    ) -> Dict[str, Any]:
        """Finds the keys in the metadata in the following order:

        It will find the merge_predicate or merge_operations_config in this order:
        1. Runtime metadata
        2. Definition metadata
        3. IO Manager config

        E.g., `merge_predicate` and `merge_operations_config`

        Args:
            context (OutputContext): The output context
            keys (list[str], optional): The keys to find in the metadata. Defaults to ["merge_predicate", "merge_operations_config"].

        Returns:
            dict[str, Any]: The metadata with the keys found
        """
        metadata_definition = context.definition_metadata or {}
        metadata_output = context.output_metadata or {}
        metadata_resource = context.resource_config or {}

        # Find each of the key in the definition or output metadata
        result = {}

        for key in keys:
            if key in metadata_output or {}:
                result[key] = metadata_output[key]
            elif key in metadata_definition or {}:
                result[key] = metadata_definition[key]
            elif key in context.resource_config or {}:
                result[key] = metadata_resource[key]
            else:
                result[key] = None

            # If it's a TextMetadataValue, cast it to string
            if isinstance(result[key], MetadataValue):
                if result[key].value is not None:
                    result[key] = str(result[key].value)
                else:
                    result[key] = None

        return result

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: T,
        connection: TableConnection,
    ):
        """Stores pyarrow types in Delta table."""
        logger = logging.getLogger()
        logger.setLevel("DEBUG")

        keys_from_metadata = self._find_keys_in_metadata(
            context, ["merge_predicate", "merge_operations_config"]
        )

        merge_predicate_from_metadata = keys_from_metadata.get("merge_predicate", None)
        merge_operations_config_from_metadata = keys_from_metadata.get(
            "merge_operations_config", None
        )

        definition_metadata = context.definition_metadata or {}
        additional_table_config = definition_metadata.get("table_configuration", {})
        if connection.table_config is not None:
            table_config = additional_table_config | connection.table_config
        else:
            table_config = additional_table_config
        resource_config = context.resource_config or {}
        object_stats = self.get_output_stats(obj)
        data, delta_params = self.to_arrow(obj=obj)
        delta_schema = Schema.from_pyarrow(_convert_pa_schema_to_delta(data.schema))
        resource_config = cast(_DeltaTableIOManagerResourceConfig, context.resource_config)
        engine = resource_config.get("writer_engine")
        save_mode = definition_metadata.get("mode")
        main_save_mode = resource_config.get("mode")
        custom_metadata = definition_metadata.get("custom_metadata") or resource_config.get(
            "custom_metadata",
        )
        schema_mode = definition_metadata.get("schema_mode") or resource_config.get(
            "schema_mode",
        )
        writer_properties = resource_config.get("writer_properties")
        writer_properties = (
            WriterProperties(**writer_properties) if writer_properties is not None else None  # type: ignore
        )

        commit_properties = definition_metadata.get("commit_properties") or resource_config.get(
            "commit_properties",
        )
        commit_properties = (
            CommitProperties(**commit_properties) if commit_properties is not None else None  # type: ignore
        )
        merge_config = resource_config.get("merge_config")

        date_format = extract_date_format_from_partition_definition(context)

        if save_mode is not None:
            logger.debug(
                "IO manager mode overridden with the asset metadata mode, %s -> %s",
                main_save_mode,
                save_mode,
            )
            main_save_mode = save_mode
        logger.debug("Writing with mode: `%s`", main_save_mode)

        merge_stats = None
        partition_filters = None
        partition_columns = None
        predicate = None

        if table_slice.partition_dimensions is not None:
            partition_filters = partition_dimensions_to_dnf(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=delta_schema,
                str_values=True,
                date_format=date_format,
            )
            if partition_filters is not None and engine == "rust":
                ## Convert partition_filter to predicate
                predicate = create_predicate(partition_filters)
                partition_filters = None
            else:
                predicate = None
            # TODO(): make robust and move to function
            partition_columns = [dim.partition_expr for dim in table_slice.partition_dimensions]

        if main_save_mode not in ["merge", "create_or_replace"]:
            if predicate is not None and engine == "rust":
                logger.debug("Using explicit partition predicate: \n%s", predicate)
            elif partition_filters is not None and engine == "pyarrow":
                logger.debug("Using explicit partition_filter: \n%s", partition_filters)
            write_deltalake(  # type: ignore
                table_or_uri=connection.table_uri,
                data=data,
                storage_options=connection.storage_options,
                mode=main_save_mode,
                partition_filters=partition_filters,
                predicate=predicate,
                partition_by=partition_columns,
                engine=engine,
                schema_mode=schema_mode,
                configuration=table_config,
                custom_metadata=custom_metadata,
                writer_properties=writer_properties,
                commit_properties=commit_properties,
                **delta_params,
            )
        elif main_save_mode == "create_or_replace":
            DeltaTable.create(
                table_uri=connection.table_uri,
                schema=_convert_pa_schema_to_delta(data.schema),
                mode="overwrite",
                partition_by=partition_columns,
                configuration=table_config,
                storage_options=connection.storage_options,
                custom_metadata=custom_metadata,
            )
        else:
            if merge_config is None:
                raise ValueError(
                    "Merge Configuration should be provided when `mode = WriterMode.merge`",
                )
            try:
                dt = DeltaTable(connection.table_uri, storage_options=connection.storage_options)
            except TableNotFoundError:
                logger.debug("Creating a DeltaTable first before merging.")
                dt = DeltaTable.create(
                    table_uri=connection.table_uri,
                    schema=_convert_pa_schema_to_delta(data.schema),
                    partition_by=partition_columns,
                    configuration=table_config,
                    storage_options=connection.storage_options,
                    custom_metadata=custom_metadata,
                )
            merge_stats = merge_execute(
                dt,
                data,
                MergeConfig.model_validate(merge_config),
                writer_properties=writer_properties,
                commit_properties=commit_properties,
                custom_metadata=custom_metadata,
                delta_params=delta_params,
                merge_predicate_from_metadata=merge_predicate_from_metadata,
                merge_operations_config=merge_operations_config_from_metadata,
                partition_filters=partition_filters,
            )

        dt = DeltaTable(connection.table_uri, storage_options=connection.storage_options)
        try:
            stats = _get_partition_stats(dt=dt, partition_filters=partition_filters)
        except Exception as e:
            context.log.warning(f"error while computing table stats: {e}")
            stats = {}

        output_metadata = {
            # "dagster/table_name": table_slice.table,
            "table_uri": MetadataValue.path(connection.table_uri),
            # "dagster/uri": MetadataValue.path(connection.table_uri),
            "dagster/column_schema": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn(name=name, type=str(dtype))
                        for name, dtype in zip(data.schema.names, data.schema.types)
                    ],
                ),
            ),
            "table_version": MetadataValue.int(dt.version()),
            **stats,
            **object_stats,
        }
        if merge_stats is not None:
            output_metadata["num_output_rows"] = MetadataValue.int(
                merge_stats.get("num_output_rows", 0),
            )
            output_metadata["merge_stats"] = MetadataValue.json(merge_stats)

        context.add_output_metadata(output_metadata)

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: TableConnection,
    ) -> T:
        """Loads the input as a pyarrow Table or RecordBatchReader."""
        parquet_read_options = None
        if context.resource_config is not None:
            parquet_read_options = context.resource_config.get("parquet_read_options", None)
            parquet_read_options = (
                ds.ParquetReadOptions(**parquet_read_options)
                if parquet_read_options is not None
                else None
            )

        dataset = read_table(table_slice, connection, parquet_read_options=parquet_read_options)

        if context.dagster_type.typing_type == ds.Dataset:
            if table_slice.columns is not None:
                raise ValueError("Cannot select columns when loading as Dataset.")
            return dataset

        scanner = dataset.scanner(columns=table_slice.columns)
        return self.from_arrow(scanner.to_reader(), context.dagster_type.typing_type)


def _get_partition_stats(
    dt: DeltaTable,
    partition_filters: Optional[list[FilterLiteralType]] = None,
) -> dict[str, Any]:
    """Gets the stats for a partition

    Args:
        dt (DeltaTable): DeltaTable object
        partition_filters (list[FilterLiteralType] | None, optional): filters to grabs stats with. Defaults to None.

    Returns:
        dict[str, MetadataValue]: Partition stats
    """
    files = pa.array(dt.files(partition_filters=partition_filters))
    files_table = pa.Table.from_arrays([files], names=["path"])
    actions_table = pa.Table.from_batches([dt.get_add_actions(flatten=True)])
    actions_table = actions_table.select(["path", "size_bytes", "num_records"])
    table = files_table.join(actions_table, keys="path")

    stats: dict[str, Any]

    stats = {
        "size_MB": MetadataValue.float(
            pc.sum(table.column("size_bytes")).as_py() * 0.00000095367432,  # type: ignore
        ),
    }
    row_count = MetadataValue.int(
        pc.sum(table.column("num_records")).as_py(),  # type: ignore
    )
    if partition_filters is not None:
        stats["dagster/partition_row_count"] = row_count
    else:
        stats["dagster/row_count"] = row_count

    return stats
