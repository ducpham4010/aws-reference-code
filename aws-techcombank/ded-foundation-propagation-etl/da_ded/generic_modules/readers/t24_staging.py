from .base import BaseReader
from ..models.constants import SerializationFormats


class T24StagingReader(BaseReader):
    __cached_df = {}

    def _read_catalog_df(
        self,
        database: str,
        table: str,
        push_down_predicate: str = "",
        transformation_ctx: str = "",
    ):
        """
        For some weird reason, glue bookmark behaves like this:
        If all data are already processed, first time read from catalog, it returns a dataframe with count = 0.
        Second time read from catalog with same args, it returns full dataframe as if no bookmark.
        So, I cached the first df read from catalog.
        Glue pls!
        """
        # normalize push_down_predicate
        if not push_down_predicate:
            push_down_predicate = ""

        key = f"{database}_{table}_{push_down_predicate}"
        print("CACHE KEY ", key)

        if key in self.__cached_df:
            print("FOUND CACHED CATALOG DF WITH KEY ", key)
            df = self.__cached_df[key]

            if df is None:
                print("EMPTY DF")
            else:
                print("NOT EMPTY DF ", df.count())

            return df

        print("CACHE MISSED")

        try:
            kwargs = {
                "database": database,
                "table_name": table,
                "transformation_ctx": transformation_ctx,
            }

            # unsure how empty push_down_predicate works, so I do this for safety
            if push_down_predicate:
                kwargs["push_down_predicate"] = push_down_predicate

            df = self.glue_context.create_dynamic_frame.from_catalog(**kwargs).toDF()

            cnt = df.count()

            if cnt > 0:
                print("NOT EMPTY DATA", database, table, cnt)
                self.__cached_df[key] = df
                return df
            else:
                print("EMPTY DATA", database, table)
                self.__cached_df[key] = None
        except Exception:
            # need better way to handle this
            print("TABLE NOT EXISTS", database, table)
            self.__cached_df[key] = None

    def read(self):
        assert self.job_input.serde.format == SerializationFormats.PARQUET
        database = self.job_input.glue_catalog["database"]
        table = self.job_input.glue_catalog["table"]
        push_down_predicate = self.job_input.glue_catalog.get("push_down_predicate")
        transformation_ctx = self.job_input.glue_catalog.get("transformation_ctx")
        if transformation_ctx == "":
            transformation_ctx = f"staging_{database}_{table}"

        df = self._read_catalog_df(database, table, push_down_predicate, transformation_ctx)

        if df:
            yield df