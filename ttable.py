# -*- coding: utf-8 -*-

"""This module contains class to access Temporal Tables.

Temporal tables are a type of database tables introduced in SQL Server 2016,
these tables are system-versioned and keep history of changes (insert, delete,
update) of everything happened on data rows.

Retrieving change log from these tables are easy. These tables can simply tell
you what was the data at specific point of the time in the table. These tables
works with 2 datetime columns to keep FROM DATE and TO DATE information of each
change. These tables can be used for implementing Slowly Changing Dimension
without complex ETL implementation.

"""

import datetime
import hashlib
import itertools
import pandas as pd
import sqlalchemy
from sqlalchemy import TEXT, Integer, Float, REAL, Date, Index, VARCHAR
from sqlalchemy.orm import Session
import luigi
from luigi.contrib import sqla


class TemporalTable(sqla.CopyToTable):
    """A class for accessing a temporal table.

    Args:
        delete (Optional[boolean]): If True, rows in the DB not present in the
            input will be set as obsolete. Defaults to True.

    Usage:
        Subclass and override the required attributes:
            `connection_string`
            `table`
            `columns`
            `natural_key`
            `asof`

        Optional:
            obsolete_deleted_rows: If True, rows in the DB not present in the
                input will be set as obsolete. Defaults to True.

    """
    enddate = datetime.datetime.strptime('2999-12-31', '%Y-%m-%d').date()
    obsolete_deleted_rows = True
    chunksize = 5000
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.keycache = dict()
        self.idcache = dict()
        self.seenkeys = dict()

        # List of ids not seen in the input. This need to stay outside of the
        # rows loop because we need to read all chunks to be sure that an id
        # is not present in any of them.
        self.obsolete_ids = list()

    def _hash(self, row, algorithm='sha256', encoding='latin-1'):
        """Computes hash of the entire row.
           See hashlib.algorithms_guaranteed for the complete algorithm list.
        """
        m = hashlib.new(algorithm)

        values = [row[col] for col in sorted(row.keys())
                  if col not in ['id', 'SysStartDate', 'SysEndDate']]
        for value in values:
            if not isinstance(value, bytes):
                value = str(value).encode(encoding)
            m.update(value)

        return m.hexdigest()
    
    def _make_search_tuple(self, row):
        return tuple(row[key] for key in self.natural_key)

    def _prefill_cache(self, conn):
        """Cache hash and id of rows that are currently valid.
        """
        self._logger.debug('Loading temporal table hash cache')

        with self.output().engine.begin() as conn:
                engine = self.output().engine
                session = Session(engine)

                for row in session.query(
                    self.table_bound).yield_per(self.chunksize):
                    row_dict = dict(zip(self.table_bound.columns.keys(), row))
                    searchtuple = self._make_search_tuple(row_dict)

                    self.keycache[searchtuple] = self._hash(row_dict)
                    self.idcache[searchtuple] = row_dict['id']

    def _set_as_obsolete(self, conn, ids, table_bound):
        """This method set rows as obsolete updating the `SysEndDate` column of
        rows given by ids to `asof`.

        Args:
            conn (sqlalchemy.engine): The sqlalchemy connection object.
            ids (list): List of ids to set as obsolete.
            table_bound (sqlalchemy.Table): The object referring to the table

        """
        rows = [{'_id': id, '_SysEndDate': self.asof} for id in ids]

        sql = (self.table_bound.update().\
            where(self.table_bound.c.id == sqlalchemy.bindparam('_id')).\
            values(SysEndDate=sqlalchemy.bindparam('_SysEndDate')))

        conn.execute(sql, rows)


    def create_table(self, engine):
        """Override of the `luigi.contrib.sqla.create_table()` with only one
        modification: it inserts some special rows when creating tables.

        """
        def construct_sqla_columns(columns):
            columns.insert(0, (['id', Integer], {'primary_key': True}))
            columns.append((['SysStartDate', Date], {}))
            columns.append((['SysEndDate', Date], {'index': True}))

            retval = [sqlalchemy.Column(*c[0], **c[1]) for c in columns]
            return retval

        needs_setup = (len(self.columns) == 0) or (False in [len(c) == 2 for c in self.columns]) if not self.reflect else False
        if needs_setup:
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented for %r "
                                      "and columns types not "
                                      "specified" % self.table)
        else:
            # if columns is specified as (name, type) tuples
            with engine.begin() as con:
                metadata = sqlalchemy.MetaData()
                try:
                    if not con.dialect.has_table(con, self.table):
                        sqla_columns = construct_sqla_columns(self.columns)
                        self.table_bound = sqlalchemy.Table(self.table,
                                                            metadata,
                                                            *sqla_columns)

                        # Create index on natural keys
                        index_name =\
                            'ix_{}_{}'.format(self.table_bound.name,
                                              '_'.join(self.natural_key))
                        index_columns = [getattr(self.table_bound.c, c)
                                         for c in self.natural_key]
                        Index(index_name, *index_columns)

                        # Create table
                        self.table_bound.create(engine, checkfirst=True)

                        # Search for TEXT columns
                        bound_cols = \
                            dict((c, sqlalchemy.bindparam('_' + c.key))
                                 for c in self.table_bound.columns
                                 if isinstance(c.type, sqlalchemy.TEXT))
                        sql = self.table_bound.insert().values(bound_cols)

                        # Fill TEXT columns with a predefined value
                        ins_rows = \
                            dict(zip(['_' + c.key for c in bound_cols],
                                     ['#Não informado'] * len(bound_cols)))

                        con.execute(sql, ins_rows)
                    else:
                        metadata.reflect(bind=engine)
                        self.table_bound = metadata.tables[self.table]
                except Exception as e:
                    self._logger.exception(self.table + str(e))

    def rows(self, skiprows=1):
        """Return/yield tuples or lists corresponding to each input row.

        This is an override of the `luigi.contrib.sqla.CopyToTable.rows()`
        that uses SQLAlchemyTarget instead of LocalTarget inputs.

        """
        try:
            with self.input().engine.begin() as conn:
                metadata = sqlalchemy.MetaData()
                engine = self.input().engine
                table = self.input().target_table
                table_bound = sqlalchemy.Table(table,
                                               metadata,
                                               autoload=True,
                                               autoload_with=engine)
                session = Session(engine)
                for row in session.query(table_bound).yield_per(self.chunksize):
                    yield dict(zip(table_bound.columns.keys(), row))
        except AttributeError:
            raise TypeError('Input must be an SQLAlchemyTarget')

    def run(self):
        """Lookup and insert/update a version of a temporal table row.
        """
        self._logger.info('Updating temporal table {}: {}'
                          .format(self.table, self.update_id()))
        output = self.output()
        engine = output.engine
        self.create_table(engine)

        with engine.begin() as conn:
            self._prefill_cache(conn)
            rows = self.rows()
            self._log_changes(rows, conn)

            # Check if there were deleted rows.
            # We can only check if there were deleted rows after going
            # through all input rows.
            if self.obsolete_deleted_rows:
                for key, id in self.idcache.items():
                    if key not in self.seenkeys:
                        self.obsolete_ids.append(id)
                self._logger.debug('{} rows set as obsolete'
                                   .format(len(self.obsolete_ids)))

            # Update the `SysEndDate` attribute in the old row version and
            # in the deleted rows in the DB.
            if self.obsolete_ids:
                self._set_as_obsolete(conn, self.obsolete_ids,
                                        self.table_bound)

        output.touch()
        self._logger.info("Finished inserting rows into SQLAlchemy target")

    def _log_changes(self, rows, conn):
        """Lookup input rows in the db table and keep history of changes
        (insert, delete, update) of everything happened on data rows.

        """
        self._logger.debug('Logging changes')

        new_rows = list()
        modified_rows = list()
        
        for row in rows:
            row['SysStartDate'] = self.asof
            row['SysEndDate'] = self.enddate
        
             # Get the newest version and compare to that
            searchtuple = self._make_search_tuple(row)
        
            if searchtuple not in self.idcache:
                # It is a new member. We add the first version.
                new_rows.append(row)
            else:
                # There is an existing version. Check if the attributes
                # are identical
                dim_hash = self.keycache[searchtuple]
                current_hash = self._hash(row)
                if dim_hash != current_hash:
                    # The hash of the rows are different. Add the new version
                    # of the row to the `modified_rows` list, and the id to the
                    # `obsolete_ids` list.
                    modified_rows.append(row)
                    self.obsolete_ids.append(self.idcache[searchtuple])
        
                # Add this key to the `seenkeys` dictionary
                self.seenkeys[searchtuple] = self.idcache[searchtuple]

            if len(new_rows) >= self.chunksize:
                self._logger.debug('Writing {} rows'.format(len(new_rows)))
                self._insert(conn, new_rows, self.table_bound)
                new_rows = list()

            if len(modified_rows) >= self.chunksize:
                self._logger.debug('Updating {} rows'.format(len(modified_rows)))
                self._insert(conn, modified_rows, self.table_bound)
                modified_rows = list()
        
        # Insert new rows
        if new_rows:
            self._logger.debug('Writing {} rows'.format(len(new_rows)))
            self._insert(conn, new_rows, self.table_bound)
        
        # Insert a new row version of modified rows
        if modified_rows:
            self._logger.debug('Updating {} rows'.format(len(modified_rows)))
            self._insert(conn, modified_rows, self.table_bound)

    def _insert(self, conn, ins_rows, table_bound):
        """This method does the actual insertion of the rows of data given by
        ins_rows into the database.

        Args:
            conn (sqlalchemy.engine): The sqlalchemy connection object.
            ins_rows (dict): The dictionary of rows with the keys in the
                format _<column_name>. For example if you have a table with a
                column name "property", then the key in the dictionary would
                be "_property". This format is consistent with the bindparam
                usage in sqlalchemy.
            table_bound (sqlalchemy.Table): The object referring to the table

        """
        bound_cols = dict((c, sqlalchemy.bindparam("_" + c.key))
                          for c in table_bound.columns if c.key != 'id')
        sql = table_bound.insert().values(bound_cols)
        conn.execute(sql, ins_rows)
