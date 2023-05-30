import unittest

import mysql_distill


# noinspection SpellCheckingInspection
class TestQueryParser(unittest.TestCase):
    def test_get_tables(self):
        self.assertEqual(
            mysql_distill.get_tables(
                (
                    "REPLACE /*foo.bar:3/3*/ INTO checksum.checksum (db, tbl, "
                    "chunk, boundaries, this_cnt, this_crc) SELECT 'foo', 'bar', "
                    "2 AS chunk_num, '`id` >= 2166633', COUNT(*) AS cnt, "
                    "LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', `id`, `created_by`, "
                    "`created_date`, `updated_by`, `updated_date`, `ppc_provider`, "
                    "`account_name`, `provider_account_id`, `campaign_name`, "
                    "`provider_campaign_id`, `adgroup_name`, `provider_adgroup_id`, "
                    "`provider_keyword_id`, `provider_ad_id`, `foo`, `reason`, "
                    "`foo_bar_bazz_id`, `foo_bar_baz`, CONCAT(ISNULL(`created_by`), "
                    "ISNULL(`created_date`), ISNULL(`updated_by`), ISNULL(`updated_date`), "
                    "ISNULL(`ppc_provider`), ISNULL(`account_name`), "
                    "ISNULL(`provider_account_id`), ISNULL(`campaign_name`), "
                    "ISNULL(`provider_campaign_id`), ISNULL(`adgroup_name`), "
                    "ISNULL(`provider_adgroup_id`), ISNULL(`provider_keyword_id`), "
                    "ISNULL(`provider_ad_id`), ISNULL(`foo`), ISNULL(`reason`), "
                    "ISNULL(`foo_base_foo_id`), ISNULL(`fooe_foo_id`)))) AS UNSIGNED)), 10, "
                    "16)) AS crc FROM `foo`.`bar` USE INDEX (`PRIMARY`) WHERE "
                    "(`id` >= 2166633); "
                )
            ),
            ["checksum.checksum", "`foo`.`bar`"],
            msg="gets tables from nasty checksum query",
        )
        self.assertEqual(
            mysql_distill.get_tables(
                "SELECT STRAIGHT_JOIN distinct foo, bar FROM A, B, C"
            ),
            ["A", "B", "C"],
            msg="gets tables from STRAIGHT_JOIN",
        )

        self.assertEqual(
            mysql_distill.get_tables(
                "replace into checksum.checksum select `last_update`, `foo` from foo.foo"
            ),
            ["checksum.checksum", "foo.foo"],
            msg="gets tables with reserved words",
        )

        self.assertEqual(
            mysql_distill.get_tables(
                "SELECT * FROM (SELECT * FROM foo WHERE UserId = 577854809 ORDER BY foo DESC) q1 GROUP BY foo ORDER BY bar DESC LIMIT 3"
            ),
            ["foo"],
            "get_tables on simple subquery",
        )

        self.assertEqual(
            mysql_distill.get_tables(
                'INSERT INTO my.tbl VALUES("I got this from the newspaper")'
            ),
            ["my.tbl"],
            "Not confused by quoted string",
        )

        self.assertEqual(
            mysql_distill.get_tables("create table db.tbl (i int)"),
            ["db.tbl"],
            msg="get_tables: CREATE TABLE",
        )
        self.assertEqual(
            mysql_distill.get_tables("create TEMPORARY table db.tbl2 (i int)"),
            ["db.tbl2"],
            msg="get_tables: CREATE TEMPORARY TABLE",
        )
        self.assertEqual(
            mysql_distill.get_tables("create table if not exists db.tbl (i int)"),
            ["db.tbl"],
            msg="get_tables: CREATE TABLE IF NOT EXISTS",
        )
        self.assertEqual(
            mysql_distill.get_tables(
                "create TEMPORARY table IF NOT EXISTS db.tbl3 (i int)"
            ),
            ["db.tbl3"],
            "get_tables: CREATE TEMPORARY TABLE IF NOT EXISTS",
        )

        self.assertEqual(
            mysql_distill.get_tables(
                "CREATE TEMPORARY TABLE `foo` AS select * from bar where id = 1"
            ),
            ["bar"],
            "get_tables: CREATE TABLE ... SELECT",
        )

        self.assertEqual(
            mysql_distill.get_tables("ALTER TABLE db.tbl ADD COLUMN (j int)"),
            ["db.tbl"],
            "get_tables: ALTER TABLE",
        )

        self.assertEqual(
            mysql_distill.get_tables("DROP TABLE db.tbl"),
            ["db.tbl"],
            "get_tables: DROP TABLE",
        )

        self.assertEqual(
            mysql_distill.get_tables("truncate table db.tbl"),
            ["db.tbl"],
            "get_tables: TRUNCATE TABLE",
        )

        self.assertEqual(
            mysql_distill.get_tables("create database foo"),
            [],
            "get_tables: CREATE DATABASE (no tables)",
        )

        self.assertEqual(
            mysql_distill.get_tables(
                'INSERT INTO `foo` (`s`,`from`,`t`,`p`) VALVUES ("not","me","foo",1)'
            ),
            ["`foo`"],
            "Throws out suspicious table names",
        )

    def _test_query(self, query, aliases, tables, msg):
        self.assertEqual(
            mysql_distill.get_tables(query), tables, msg=f"get_tables: {msg}"
        )

    def test_one_table(self):
        """
        All manner of "normal" SELECT queries.
        """

        self._test_query(
            "SELECT * FROM t1",
            {"DATABASE": {}, "TABLE": {"t1": "t1"}},
            ["t1"],
            "one table no alias and no following clauses",
        )
        # 1 table
        self._test_query(
            "SELECT * FROM t1 WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "t1": "t1",
                },
            },
            ["t1"],
            "one table no alias",
        )

        self._test_query(
            "SELECT * FROM t1 a WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                },
            },
            ["t1"],
            "one table implicit alias",
        )

        self._test_query(
            "SELECT * FROM t1 AS a WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                },
            },
            ["t1"],
            "one table AS alias",
        )

    def test_two_tables(self):
        # 2 tables
        self._test_query(
            "SELECT * FROM t1, t2 WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "t1": "t1",
                    "t2": "t2",
                },
            },
            ["t1", "t2"],
            "two tables no aliases",
        )

        self._test_query(
            'SELECT * FROM t1 a, t2 WHERE foo = "bar"',
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "t2": "t2",
                },
            },
            ["t1", "t2"],
            "two tables implicit alias and no alias",
        )

        self._test_query(
            "SELECT * FROM t1 a, t2 b WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                },
            },
            ["t1", "t2"],
            "two tables implicit aliases",
        )

        self._test_query(
            "SELECT * FROM t1 AS a, t2 AS b WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                },
            },
            ["t1", "t2"],
            "two tables AS aliases",
        )

        self._test_query(
            "SELECT * FROM t1 AS a, t2 b WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                },
            },
            ["t1", "t2"],
            "two tables AS alias and implicit alias",
        )

        self._test_query(
            "SELECT * FROM t1 a, t2 AS b WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                },
            },
            ["t1", "t2"],
            "two tables implicit alias and AS alias",
        )

        self._test_query(
            "SELECT * FROM t1 a, t2 AS b WHERE id = 1",
            [
                "t1 a",
                "t2 AS b",
            ],
            ["t1", "t2"],
            "two tables implicit alias and AS alias, with alias",
        )

        # ANSI JOINs
        self._test_query(
            "SELECT * FROM t1 JOIN t2 ON a.id = b.id",
            {
                "DATABASE": {},
                "TABLE": {
                    "t1": "t1",
                    "t2": "t2",
                },
            },
            ["t1", "t2"],
            "two tables no aliases JOIN",
        )

        self._test_query(
            "SELECT * FROM t1 a JOIN t2 b ON a.id = b.id",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                },
            },
            ["t1", "t2"],
            "two tables implicit aliases JOIN",
        )

        self._test_query(
            "SELECT * FROM t1 AS a JOIN t2 as b ON a.id = b.id",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                },
            },
            ["t1", "t2"],
            "two tables AS aliases JOIN",
        )

        self._test_query(
            "SELECT * FROM t1 AS a JOIN t2 b ON a.id=b.id WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                },
            },
            ["t1", "t2"],
            "two tables AS alias and implicit alias JOIN",
        )

        self._test_query(
            "SELECT * FROM t1 LEFT JOIN t2 ON a.id = b.id",
            {
                "DATABASE": {},
                "TABLE": {
                    "t1": "t1",
                    "t2": "t2",
                },
            },
            ["t1", "t2"],
            "two tables no aliases LEFT JOIN",
        )

        self._test_query(
            "SELECT * FROM t1 a LEFT JOIN t2 b ON a.id = b.id",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                },
            },
            ["t1", "t2"],
            "two tables implicit aliases LEFT JOIN",
        )

        self._test_query(
            "SELECT * FROM t1 AS a LEFT JOIN t2 as b ON a.id = b.id",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                },
            },
            ["t1", "t2"],
            "two tables AS aliases LEFT JOIN",
        )

        self._test_query(
            "SELECT * FROM t1 AS a LEFT JOIN t2 b ON a.id=b.id WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                },
            },
            ["t1", "t2"],
            "two tables AS alias and implicit alias LEFT JOIN",
        )

    def test_three_tables(self):
        # 3 tables
        self._test_query(
            'SELECT * FROM t1 JOIN t2 ON t1.col1=t2.col2 JOIN t3 ON t2.col3 = t3.col4 WHERE foo = "bar"',
            {
                "DATABASE": {},
                "TABLE": {
                    "t1": "t1",
                    "t2": "t2",
                    "t3": "t3",
                },
            },
            ["t1", "t2", "t3"],
            "three tables no aliases JOIN",
        )

        self._test_query(
            "SELECT * FROM t1 AS a, t2, t3 c WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "t2": "t2",
                    "c": "t3",
                },
            },
            ["t1", "t2", "t3"],
            "three tables AS alias, no alias, implicit alias",
        )

        self._test_query(
            "SELECT * FROM t1 a, t2 b, t3 c WHERE id = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "a": "t1",
                    "b": "t2",
                    "c": "t3",
                },
            },
            ["t1", "t2", "t3"],
            "three tables implicit aliases",
        )

    def test_db_qualified_queries(self):
        # Db-qualified tables
        self._test_query(
            "SELECT * FROM db.t1 AS a WHERE id = 1",
            {
                "TABLE": {
                    "a": "t1",
                },
                "DATABASE": {
                    "t1": "db",
                },
            },
            ["db.t1"],
            "one db-qualified table AS alias",
        )

        self._test_query(
            "SELECT * FROM `db`.`t1` AS a WHERE id = 1",
            {
                "TABLE": {
                    "a": "t1",
                },
                "DATABASE": {
                    "t1": "db",
                },
            },
            ["`db`.`t1`"],
            "one db-qualified table AS alias with backticks",
        )

    def test_other_cases(self):
        # Other cases
        self._test_query(
            "SELECT a FROM store_orders_line_items JOIN store_orders",
            {
                "DATABASE": {},
                "TABLE": {
                    "store_orders_line_items": "store_orders_line_items",
                    "store_orders": "store_orders",
                },
            },
            ["store_orders_line_items", "store_orders"],
            "Embedded ORDER keyword",
        )

    def test_non_select_queries(self):
        # #############################################################################
        # Non-SELECT queries.
        # #############################################################################
        self._test_query(
            "UPDATE foo AS bar SET value = 1 WHERE 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "bar": "foo",
                },
            },
            ["foo"],
            "update with one AS alias",
        )

        self._test_query(
            "UPDATE IGNORE foo bar SET value = 1 WHERE 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "bar": "foo",
                },
            },
            ["foo"],
            "update ignore with one implicit alias",
        )

        self._test_query(
            "UPDATE IGNORE bar SET value = 1 WHERE 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "bar": "bar",
                },
            },
            ["bar"],
            "update ignore with one not aliased",
        )

        self._test_query(
            "UPDATE LOW_PRIORITY baz SET value = 1 WHERE 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "baz": "baz",
                },
            },
            ["baz"],
            "update low_priority with one not aliased",
        )

        self._test_query(
            "UPDATE LOW_PRIORITY IGNORE bat SET value = 1 WHERE 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "bat": "bat",
                },
            },
            ["bat"],
            "update low_priority ignore with one not aliased",
        )

        self._test_query(
            "INSERT INTO foo VALUES (1)",
            {
                "DATABASE": {},
                "TABLE": {
                    "foo": "foo",
                },
            },
            ["foo"],
            "insert with one not aliased",
        )

        self._test_query(
            "INSERT INTO foo VALUES (1) ON DUPLICATE KEY UPDATE bar = 1",
            {
                "DATABASE": {},
                "TABLE": {
                    "foo": "foo",
                },
            },
            ["foo"],
            "insert / on duplicate key update",
        )

    def test_non_dms_query(self):
        # #############################################################################
        # Non-DMS queries.
        # #############################################################################
        self._test_query(
            "BEGIN",
            {
                "DATABASE": {},
                "TABLE": {},
            },
            [],
            "BEGIN",
        )

    def test_diabolical_dbs_and_tbls_with_spaces_in_their_names(self):
        # #############################################################################
        # Diabolical dbs and tbls with spaces in their names.
        # #############################################################################

        self._test_query(
            "select * from `my table` limit 1;",
            {
                "DATABASE": {},
                "TABLE": {
                    "my table": "my table",
                },
            },
            ["`my table`"],
            "one table with space in name, not aliased",
        )

        self._test_query(
            "select * from `my database`.mytable limit 1;",
            {
                "TABLE": {
                    "mytable": "mytable",
                },
                "DATABASE": {
                    "mytable": "my database",
                },
            },
            ["`my database`.mytable"],
            "one db.tbl with space in db, not aliased",
        )

        self._test_query(
            "select * from `my database`.`my table` limit 1; ",
            {
                "TABLE": {
                    "my table": "my table",
                },
                "DATABASE": {
                    "my table": "my database",
                },
            },
            ["`my database`.`my table`"],
            "one db.tbl with space in both db and tbl, not aliased",
        )

    def test_misc(self):
        # #############################################################################
        # Issue 185: QueryParser fails to parse table ref for a JOIN ... USING
        # #############################################################################
        self._test_query(
            "select  n.column1 = a.column1, n.word3 = a.word3 from db2.tuningdetail_21_265507 n inner join db1.gonzo a using(gonzo)",
            {
                "TABLE": {
                    "n": "tuningdetail_21_265507",
                    "a": "gonzo",
                },
                "DATABASE": {
                    "tuningdetail_21_265507": "db2",
                    "gonzo": "db1",
                },
            },
            ["db2.tuningdetail_21_265507", "db1.gonzo"],
            "SELECT with JOIN ON and no WHERE (issue 185)",
        )

        # #############################################################################
        self._test_query(
            "select 12_13_foo from (select 12foo from 123_bar) as 123baz",
            {
                "DATABASE": {},
                "TABLE": {
                    "123baz": None,
                },
            },
            ["123_bar"],
            "Subquery in the FROM clause",
        )

        self._test_query(
            (
                "UPDATE GARDEN_CLUPL PL, GARDENJOB GC, APLTRACT_GARDENPLANT ABU SET "
                "GC.MATCHING_POT = 5, GC.LAST_GARDENPOT = 5, GC.LAST_NAME="
                "'Rotary', GC.LAST_BUCKET='Pail', GC.LAST_UPDATE='2008-11-27 04:00:59' WHERE "
                "PL.APLTRACT_GARDENPLANT_ID = GC.APLTRACT_GARDENPLANT_ID AND PL."
                "APLTRACT_GARDENPLANT_ID = ABU.ID AND GC.MATCHING_POT = 0 AND GC.PERFORM_DIG=1 "
                "AND ABU.DIG = 6 AND ( ((SOIL-COST) > -80.0 "
                "AND BUGS < 60.0 AND (SOIL-COST) < 200.0) AND POTS < 10.0 )"
            ),
            {
                "DATABASE": {},
                "TABLE": {
                    "PL": "GARDEN_CLUPL",
                    "GC": "GARDENJOB",
                    "ABU": "APLTRACT_GARDENPLANT",
                },
            },
            ["GARDEN_CLUPL", "GARDENJOB", "APLTRACT_GARDENPLANT"],
            "Gets tables from query with aliases and comma-join",
        )

        self._test_query(
            (
                """
        SELECT count(*) AS count_all FROM `impact_actions`  LEFT OUTER JOIN 
        recommended_change_events ON (impact_actions.event_id = 
        recommended_change_events.event_id) LEFT OUTER JOIN 
        recommended_change_aments ON (impact_actions.ament_id = 
        recommended_change_aments.ament_id) WHERE (impact_actions.user_id = 71058 
        # An old version of the regex used to think , was the precursor to a
        # table name, so it would pull out 7,8,9,10,11 as table names.
        AND (impact_actions.action_type IN (4,7,8,9,10,11) AND 
        (impact_actions.change_id = 2699 OR recommended_change_events.change_id = 
        2699 OR recommended_change_aments.change_id = 2699)))
        """
            ),
            {
                "DATABASE": {},
                "TABLE": {
                    "impact_actions": "impact_actions",
                    "recommended_change_events": "recommended_change_events",
                    "recommended_change_aments": "recommended_change_aments",
                },
            },
            [
                "`impact_actions`",
                "recommended_change_events",
                "recommended_change_aments",
            ],
            "Does not think IN() list has table names",
        )

        self._test_query(
            'INSERT INTO my.tbl VALUES("I got this FROM the newspaper today")',
            {
                "TABLE": {
                    "tbl": "tbl",
                },
                "DATABASE": {"tbl": "my"},
            },
            ["my.tbl"],
            "Not confused by quoted string",
        )

    def test_misc_queries(self):
        # #############################################################################
        # Issue 563: Lock tables is not distilled
        # #############################################################################

        self.assertEqual(
            mysql_distill.get_tables("LOCK TABLES foo READ"),
            ["foo"],
            "LOCK TABLES foo READ",
        )
        self.assertEqual(
            mysql_distill.get_tables("LOCK TABLES foo WRITE"),
            ["foo"],
            "LOCK TABLES foo WRITE",
        )
        self.assertEqual(
            mysql_distill.get_tables("LOCK TABLES foo READ, bar WRITE"),
            ["foo", "bar"],
            "LOCK TABLES foo READ, bar WRITE",
        )
        self.assertEqual(
            mysql_distill.get_tables("LOCK TABLES foo AS als WRITE"),
            ["foo"],
            "LOCK TABLES foo AS als WRITE",
        )
        self.assertEqual(
            mysql_distill.get_tables("LOCK TABLES foo AS als1 READ, bar AS als2 WRITE"),
            ["foo", "bar"],
            "LOCK TABLES foo AS als READ, bar AS als2 WRITE",
        )
        self.assertEqual(
            mysql_distill.get_tables("LOCK TABLES foo als WRITE"),
            ["foo"],
            "LOCK TABLES foo als WRITE",
        )
        self.assertEqual(
            mysql_distill.get_tables("LOCK TABLES foo als1 READ, bar als2 WRITE"),
            ["foo", "bar"],
            "LOCK TABLES foo als READ, bar als2 WRITE",
        )

        self.assertEqual(
            mysql_distill.get_tables(
                """CREATE TEMPORARY TABLE mk_upgrade AS SELECT col1, col2
                FROM foo, bar
                WHERE id = 1"""
            ),
            ["foo", "bar"],
            "Get tables from special case multi-line query",
        )

        self.assertEqual(
            mysql_distill.get_tables("select * from (`mytable`)"),
            ["`mytable`"],
            "Get tables when there are parens around table name (issue 781)",
        )

        self.assertEqual(
            mysql_distill.get_tables("select * from (select * from mytable) t"),
            ["mytable"],
            "Does not consider subquery SELECT as a table (issue 781)",
        )

        self.assertEqual(
            mysql_distill.get_tables(
                "lock tables t1 as t5 read local, t2 low_priority write"
            ),
            ["t1", "t2"],
            "get_tables works for lowercased LOCK TABLES",
        )

        self.assertEqual(
            mysql_distill.get_tables(
                "LOAD DATA INFILE '/tmp/foo.txt' INTO TABLE db.tbl"
            ),
            ["db.tbl"],
            "LOAD DATA db.tbl",
        )
