# -*- coding: utf-8 -*-

"""Fork of `luigi.contrib.sqla` that implements SQLAlchemyTarget as input.
"""

import abc
import collections
import datetime
import itertools
import logging
import luigi
from luigi.contrib import sqla
import os
import sqlalchemy


class CopyToTable(sqla.CopyToTable):
    """
    Fork from origin luigi CopyToTable that uses SQLAlchemyTarget as input.

    Usage:

    * subclass and override the required `connection_string`, `table` and `columns` attributes.
    """

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.

        """

        with self.input().engine.begin() as con:
            metadata = sqlalchemy.MetaData()
            engine = self.input().engine
            table = self.input().target_table
            table_bound = sqlalchemy.Table(table,
                                           metadata,
                                           autoload=True,
                                           autoload_with=engine)

            result = con.execute(table_bound.select())
            for row in result:
                yield dict(row)

    def run(self):
        self._logger.info("Running task copy to table for update id %s for table %s" % (self.update_id(), self.table))
        output = self.output()
        engine = output.engine
        self.create_table(engine)
        with engine.begin() as conn:
            rows = self.rows()
            ins_rows = [{'_' + key: value for key, value in row.items()}
                        for row in rows]
            while ins_rows:
                self.copy(conn, ins_rows, self.table_bound)
                ins_rows = [{'_' + key: value for key, value in row.items()}
                        for row in rows]
                self._logger.info("Finished inserting %d rows into SQLAlchemy target" % len(ins_rows))
        output.touch()
        self._logger.info("Finished inserting rows into SQLAlchemy target")
