import unittest

import mysql_distill


# noinspection SpellCheckingInspection
class TestQueryRewriter(unittest.TestCase):
    def test_distill(self):
        self.assertEqual(
            mysql_distill.distill("SELECT /*!40001 SQL_NO_CACHE */ * FROM `film`"),
            "SELECT film",
            "Distills mysqldump SELECTs to selects",
        )

        self.assertEqual(
            mysql_distill.distill("CALL foo(1, 2, 3)"),
            "CALL foo",
            "Distills stored procedure calls specially",
        )

        self.assertEqual(
            mysql_distill.distill(
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
                    "(`id` >= 2166633) "
                )
            ),
            "REPLACE SELECT checksum.checksum foo.bar",
            "Distills mk-table-checksum query",
        )

        self.assertEqual(
            mysql_distill.distill("use `foo`"),
            "USE",
            "distills USE",
        )

        self.assertEqual(
            mysql_distill.distill(
                r"delete foo.bar b from foo.bar b left join baz.bat c on a=b where nine>eight"
            ),
            "DELETE foo.bar baz.bat",
            "distills and then collapses same tables",
        )

        self.assertEqual(
            mysql_distill.distill("select \n--bar\n foo"),
            "SELECT",
            "distills queries from DUAL",
        )

        self.assertEqual(
            mysql_distill.distill("select null, 5.001, 5001. from foo"),
            "SELECT foo",
            "distills simple select",
        )

        self.assertEqual(
            mysql_distill.distill(
                "select 'hello', '\nhello\n', \"hello\", '\\'' from foo"
            ),
            "SELECT foo",
            "distills with quoted strings",
        )

        self.assertEqual(
            mysql_distill.distill("select foo_1 from foo_2_3"),
            "SELECT foo_?_?",
            "distills numeric table names",
        )

        self.assertEqual(
            mysql_distill.distill("insert into abtemp.coxed select foo.bar from foo"),
            "INSERT SELECT abtemp.coxed foo",
            "distills insert/select",
        )

        self.assertEqual(
            mysql_distill.distill("insert into foo(a, b, c) values(2, 4, 5)"),
            "INSERT foo",
            "distills value lists",
        )

        self.assertEqual(
            mysql_distill.distill("select 1 union select 2 union select 4"),
            "SELECT UNION",
            "distill unions together",
        )

        self.assertEqual(
            mysql_distill.distill(
                "delete from foo where bar = baz",
            ),
            "DELETE foo",
            "distills delete",
        )

        self.assertEqual(
            mysql_distill.distill("set timestamp=134"),
            "SET",
            "distills set",
        )

        self.assertEqual(
            mysql_distill.distill(
                "replace into foo(a, b, c) values(1, 3, 5) on duplicate key update foo=bar",
            ),
            "REPLACE UPDATE foo",
            "distills ODKU",
        )

        self.assertEqual(
            mysql_distill.distill(
                (
                    "UPDATE GARDEN_CLUPL PL, GARDENJOB GC, APLTRACT_GARDENPLANT ABU SET "
                    "GC.MATCHING_POT = 5, GC.LAST_GARDENPOT = 5, GC.LAST_NAME="
                    "'Rotary', GC.LAST_BUCKET='Pail', GC.LAST_UPDATE='2008-11-27 04:00:59'WHERE"
                    " PL.APLTRACT_GARDENPLANT_ID = GC.APLTRACT_GARDENPLANT_ID AND PL."
                    "APLTRACT_GARDENPLANT_ID = ABU.ID AND GC.MATCHING_POT = 0 AND GC.PERFORM_DIG=1"
                    " AND ABU.DIG = 6 AND ( ((SOIL-COST) > -80.0"
                    " AND BUGS < 60.0 AND (SOIL-COST) < 200.0) AND POTS < 10.0 )"
                )
            ),
            "UPDATE GARDEN_CLUPL GARDENJOB APLTRACT_GARDENPLANT",
            "distills where there is alias and comma-join",
        )

        self.assertEqual(
            mysql_distill.distill(
                r"SELECT STRAIGHT_JOIN distinct foo, bar FROM A, B, C"
            ),
            "SELECT A B C",
            "distill with STRAIGHT_JOIN",
        )

        self.assertEqual(
            mysql_distill.distill(
                r"""
        REPLACE DELAYED INTO
        `db1`.`tbl2`(`col1`,col2)
        VALUES ('617653','2007-09-11')"""
            ),
            "REPLACE db?.tbl?",
            "distills replace-delayed",
        )

        self.assertEqual(
            mysql_distill.distill(
                "update foo inner join bar using(baz) set big=little",
            ),
            "UPDATE foo bar",
            "distills update-multi",
        )

        self.assertEqual(
            mysql_distill.distill(
                """
        update db2.tbl1 as p
           inner join (
              select p2.col1, p2.col2
              from db2.tbl1 as p2
                 inner join db2.tbl3 as ba
                    on p2.col1 = ba.tbl3
              where col4 = 0
              order by priority desc, col1, col2
              limit 10
           ) as chosen on chosen.col1 = p.col1
              and chosen.col2 = p.col2
           set p.col4 = 149945"""
            ),
            "UPDATE SELECT db?.tbl?",
            "distills complex subquery",
        )

        self.assertEqual(
            mysql_distill.distill(
                "replace into checksum.checksum select `last_update`, `foo` from foo.foo"
            ),
            "REPLACE SELECT checksum.checksum foo.foo",
            "distill with reserved words",
        )

        self.assertEqual(
            mysql_distill.distill("SHOW STATUS"),
            "SHOW STATUS",
            "distill SHOW STATUS",
        )

        self.assertEqual(mysql_distill.distill("commit"), "COMMIT", "distill COMMIT")

        self.assertEqual(
            mysql_distill.distill("FLUSH TABLES WITH READ LOCK"),
            "FLUSH",
            "distill FLUSH",
        )

        self.assertEqual(mysql_distill.distill("BEGIN"), "BEGIN", "distill BEGIN")

        self.assertEqual(mysql_distill.distill("start"), "START", "distill START")

        self.assertEqual(
            mysql_distill.distill("ROLLBACK"), "ROLLBACK", "distill ROLLBACK"
        )

        self.assertEqual(
            mysql_distill.distill(
                "insert into foo select * from bar join baz using (bat)",
            ),
            "INSERT SELECT foo bar baz",
            "distills insert select",
        )

        self.assertEqual(
            mysql_distill.distill("create database foo"),
            "CREATE DATABASE foo",
            "distills create database",
        )
        self.assertEqual(
            mysql_distill.distill("create table foo"),
            "CREATE TABLE foo",
            "distills create table",
        )
        self.assertEqual(
            mysql_distill.distill("alter database foo"),
            "ALTER DATABASE foo",
            "distills alter database",
        )
        self.assertEqual(
            mysql_distill.distill("alter table foo"),
            "ALTER TABLE foo",
            "distills alter table",
        )
        self.assertEqual(
            mysql_distill.distill("drop database foo"),
            "DROP DATABASE foo",
            "distills drop database",
        )
        self.assertEqual(
            mysql_distill.distill("drop table foo"),
            "DROP TABLE foo",
            "distills drop table",
        )
        self.assertEqual(
            mysql_distill.distill("rename database foo"),
            "RENAME DATABASE foo",
            "distills rename database",
        )
        self.assertEqual(
            mysql_distill.distill("rename table foo"),
            "RENAME TABLE foo",
            "distills rename table",
        )
        self.assertEqual(
            mysql_distill.distill("truncate table foo"),
            "TRUNCATE TABLE foo",
            "distills truncate table",
        )
        self.assertEqual(
            mysql_distill.distill(
                "update foo set bar=baz where bat=fiz",
            ),
            "UPDATE foo",
            "distills update",
        )

        # Issue 563: Lock tables is not distilled
        self.assertEqual(
            mysql_distill.distill("LOCK TABLES foo WRITE"),
            "LOCK foo",
            "distills lock tables",
        )
        self.assertEqual(
            mysql_distill.distill("LOCK TABLES foo READ, bar WRITE"),
            "LOCK foo bar",
            "distills lock tables (2 tables)",
        )
        self.assertEqual(
            mysql_distill.distill("UNLOCK TABLES"),
            "UNLOCK",
            "distills unlock tables",
        )

        #  Issue 712: Queries not handled by "distill"
        self.assertEqual(
            mysql_distill.distill("XA START 0x123"),
            "XA_START",
            "distills xa start",
        )
        self.assertEqual(
            mysql_distill.distill("XA PREPARE 0x123"),
            "XA_PREPARE",
            "distills xa prepare",
        )
        self.assertEqual(
            mysql_distill.distill("XA COMMIT 0x123"),
            "XA_COMMIT",
            "distills xa commit",
        )
        self.assertEqual(
            mysql_distill.distill("XA END 0x123"), "XA_END", "distills xa end"
        )
        self.assertEqual(
            mysql_distill.distill(
                """/* mysql-connector-java-5.1-nightly-20090730 ( Revision: \\${svn.Revision} ) */SHOW VARIABLES WHERE Variable_name ='language' OR Variable_name =
           'net_write_timeout' OR Variable_name = 'interactive_timeout' OR
           Variable_name = 'wait_timeout' OR Variable_name = 'character_set_client' OR
           Variable_name = 'character_set_connection' OR Variable_name =
           'character_set' OR Variable_name = 'character_set_server' OR Variable_name
           = 'tx_isolation' OR Variable_name = 'transaction_isolation' OR
           Variable_name = 'character_set_results' OR Variable_name = 'timezone' OR
           Variable_name = 'time_zone' OR Variable_name = 'system_time_zone' OR
           Variable_name = 'lower_case_table_names' OR Variable_name =
           'max_allowed_packet' OR Variable_name = 'net_buffer_length' OR
           Variable_name = 'sql_mode' OR Variable_name = 'query_cache_type' OR
           Variable_name = 'query_cache_size' OR Variable_name = 'init_connect'"""
            ),
            "SHOW VARIABLES",
            "distills /* comment */SHOW VARIABLES",
        )

        status_tests = {
            "SHOW BINARY LOGS": "SHOW BINARY LOGS",
            'SHOW BINLOG EVENTS in "log_name"': "SHOW BINLOG EVENTS",
            'SHOW CHARACTER SET LIKE "pattern"': "SHOW CHARACTER SET",
            'SHOW COLLATION WHERE "something"': "SHOW COLLATION",
            "SHOW COLUMNS FROM tbl": "SHOW COLUMNS",
            "SHOW FULL COLUMNS FROM tbl": "SHOW COLUMNS",
            "SHOW COLUMNS FROM tbl in db": "SHOW COLUMNS",
            'SHOW COLUMNS FROM tbl IN db LIKE "pattern"': "SHOW COLUMNS",
            "SHOW CREATE DATABASE db_name": "SHOW CREATE DATABASE",
            "SHOW CREATE SCHEMA db_name": "SHOW CREATE DATABASE",
            "SHOW CREATE FUNCTION func": "SHOW CREATE FUNCTION",
            "SHOW CREATE PROCEDURE proc": "SHOW CREATE PROCEDURE",
            "SHOW CREATE TABLE tbl_name": "SHOW CREATE TABLE",
            "SHOW CREATE VIEW vw_name": "SHOW CREATE VIEW",
            "SHOW DATABASES": "SHOW DATABASES",
            "SHOW SCHEMAS": "SHOW DATABASES",
            'SHOW DATABASES LIKE "pattern"': "SHOW DATABASES",
            "SHOW DATABASES WHERE foo=bar": "SHOW DATABASES",
            "SHOW ENGINE ndb status": "SHOW NDB STATUS",
            "SHOW ENGINE innodb status": "SHOW INNODB STATUS",
            "SHOW ENGINES": "SHOW ENGINES",
            "SHOW STORAGE ENGINES": "SHOW ENGINES",
            "SHOW ERRORS": "SHOW ERRORS",
            "SHOW ERRORS limit 5": "SHOW ERRORS",
            "SHOW COUNT(*) ERRORS": "SHOW ERRORS",
            "SHOW FUNCTION CODE func": "SHOW FUNCTION CODE",
            "SHOW FUNCTION STATUS": "SHOW FUNCTION STATUS",
            'SHOW FUNCTION STATUS LIKE "pattern"': "SHOW FUNCTION STATUS",
            "SHOW FUNCTION STATUS WHERE foo=bar": "SHOW FUNCTION STATUS",
            "SHOW GRANTS": "SHOW GRANTS",
            "SHOW GRANTS FOR user@localhost": "SHOW GRANTS",
            "SHOW INDEX": "SHOW INDEX",
            "SHOW INDEXES": "SHOW INDEX",
            "SHOW KEYS": "SHOW INDEX",
            "SHOW INDEX FROM tbl": "SHOW INDEX",
            "SHOW INDEX FROM tbl IN db": "SHOW INDEX",
            "SHOW INDEX IN tbl FROM db": "SHOW INDEX",
            "SHOW INNODB STATUS": "SHOW INNODB STATUS",
            "SHOW LOGS": "SHOW LOGS",
            "SHOW MASTER STATUS": "SHOW MASTER STATUS",
            "SHOW MUTEX STATUS": "SHOW MUTEX STATUS",
            "SHOW OPEN TABLES": "SHOW OPEN TABLES",
            "SHOW OPEN TABLES FROM db": "SHOW OPEN TABLES",
            "SHOW OPEN TABLES IN db": "SHOW OPEN TABLES",
            'SHOW OPEN TABLES IN db LIKE "pattern"': "SHOW OPEN TABLES",
            "SHOW OPEN TABLES IN db WHERE foo=bar": "SHOW OPEN TABLES",
            "SHOW OPEN TABLES WHERE foo=bar": "SHOW OPEN TABLES",
            "SHOW PRIVILEGES": "SHOW PRIVILEGES",
            "SHOW PROCEDURE CODE proc": "SHOW PROCEDURE CODE",
            "SHOW PROCEDURE STATUS": "SHOW PROCEDURE STATUS",
            'SHOW PROCEDURE STATUS LIKE "pattern"': "SHOW PROCEDURE STATUS",
            "SHOW PROCEDURE STATUS WHERE foo=bar": "SHOW PROCEDURE STATUS",
            "SHOW PROCESSLIST": "SHOW PROCESSLIST",
            "SHOW FULL PROCESSLIST": "SHOW PROCESSLIST",
            "SHOW PROFILE": "SHOW PROFILE",
            "SHOW PROFILES": "SHOW PROFILES",
            "SHOW PROFILES CPU FOR QUERY 1": "SHOW PROFILES CPU",
            "SHOW SLAVE HOSTS": "SHOW SLAVE HOSTS",
            "SHOW SLAVE STATUS": "SHOW SLAVE STATUS",
            "SHOW STATUS": "SHOW STATUS",
            "SHOW GLOBAL STATUS": "SHOW GLOBAL STATUS",
            "SHOW SESSION STATUS": "SHOW STATUS",
            'SHOW STATUS LIKE "pattern"': "SHOW STATUS",
            "SHOW STATUS WHERE foo=bar": "SHOW STATUS",
            "SHOW TABLE STATUS": "SHOW TABLE STATUS",
            "SHOW TABLE STATUS FROM db_name": "SHOW TABLE STATUS",
            "SHOW TABLE STATUS IN db_name": "SHOW TABLE STATUS",
            'SHOW TABLE STATUS LIKE "pattern"': "SHOW TABLE STATUS",
            "SHOW TABLE STATUS WHERE foo=bar": "SHOW TABLE STATUS",
            "SHOW TABLES": "SHOW TABLES",
            "SHOW FULL TABLES": "SHOW TABLES",
            "SHOW TABLES FROM db": "SHOW TABLES",
            "SHOW TABLES IN db": "SHOW TABLES",
            'SHOW TABLES LIKE "pattern"': "SHOW TABLES",
            'SHOW TABLES FROM db LIKE "pattern"': "SHOW TABLES",
            "SHOW TABLES WHERE foo=bar": "SHOW TABLES",
            "SHOW TRIGGERS": "SHOW TRIGGERS",
            "SHOW TRIGGERS IN db": "SHOW TRIGGERS",
            "SHOW TRIGGERS FROM db": "SHOW TRIGGERS",
            'SHOW TRIGGERS LIKE "pattern"': "SHOW TRIGGERS",
            "SHOW TRIGGERS WHERE foo=bar": "SHOW TRIGGERS",
            "SHOW VARIABLES": "SHOW VARIABLES",
            "SHOW GLOBAL VARIABLES": "SHOW GLOBAL VARIABLES",
            "SHOW SESSION VARIABLES": "SHOW VARIABLES",
            'SHOW VARIABLES LIKE "pattern"': "SHOW VARIABLES",
            "SHOW VARIABLES WHERE foo=bar": "SHOW VARIABLES",
            "SHOW WARNINGS": "SHOW WARNINGS",
            "SHOW WARNINGS LIMIT 5": "SHOW WARNINGS",
            "SHOW COUNT(*) WARNINGS": "SHOW WARNINGS",
            "SHOW COUNT ( *) WARNINGS": "SHOW WARNINGS",
        }
        for key, status_test in status_tests.items():
            self.assertEqual(
                mysql_distill.distill(key), status_test, msg=f"distills {key}"
            )

        self.assertEqual(
            mysql_distill.distill("SHOW SLAVE STATUS"),
            "SHOW SLAVE STATUS",
            "distills SHOW SLAVE STATUS",
        )
        self.assertEqual(
            mysql_distill.distill("SHOW INNODB STATUS"),
            "SHOW INNODB STATUS",
            "distills SHOW INNODB STATUS",
        )
        self.assertEqual(
            mysql_distill.distill("SHOW CREATE TABLE"),
            "SHOW CREATE TABLE",
            "distills SHOW CREATE TABLE",
        )

        shows = [
            "COLUMNS",
            "GRANTS",
            "INDEX",
            "STATUS",
            "TABLES",
            "TRIGGERS",
            "WARNINGS",
        ]
        for show in shows:
            self.assertEqual(
                mysql_distill.distill(f"SHOW {show}"),
                f"SHOW {show}",
                f"distills SHOW {show}",
            )

        #  Issue 735: mk-query-digest doesn't distill query correctly
        self.assertEqual(
            mysql_distill.distill("SHOW /*!50002 GLOBAL */ STATUS"),
            "SHOW GLOBAL STATUS",
            "distills SHOW /*!50002 GLOBAL */ STATUS",
        )

        self.assertEqual(
            mysql_distill.distill("SHOW /*!50002 ENGINE */ INNODB STATUS"),
            "SHOW INNODB STATUS",
            "distills SHOW INNODB STATUS",
        )

        self.assertEqual(
            mysql_distill.distill("SHOW MASTER LOGS"),
            "SHOW MASTER LOGS",
            "distills SHOW MASTER LOGS",
        )

        self.assertEqual(
            mysql_distill.distill("SHOW GLOBAL STATUS"),
            "SHOW GLOBAL STATUS",
            "distills SHOW GLOBAL STATUS",
        )

        self.assertEqual(
            mysql_distill.distill("SHOW GLOBAL VARIABLES"),
            "SHOW GLOBAL VARIABLES",
            "distills SHOW GLOBAL VARIABLES",
        )

        self.assertEqual(
            mysql_distill.distill("administrator command: Statistics"),
            "ADMIN STATISTICS",
            "distills ADMIN STATISTICS",
        )

        # Issue 781: mk-query-digest doesn't distill or extract tables properly
        self.assertEqual(
            mysql_distill.distill(
                "SELECT `id` FROM (`field`) WHERE `id` = '10000016228434112371782015185031'"
            ),
            "SELECT field",
            "distills SELECT clm from (`tbl`)",
        )

        self.assertEqual(
            mysql_distill.distill(
                "INSERT INTO (`jedi_forces`) (name, side, email) values ('Anakin Skywalker', 'jedi', 'anakin_skywalker_at_jedi.sw')"
            ),
            "INSERT jedi_forces",
            "distills INSERT INTO (`tbl`)",
        )

        self.assertEqual(
            mysql_distill.distill(
                "UPDATE (`jedi_forces`) set side = 'dark' and name = 'Lord Vader' where name = 'Anakin Skywalker'"
            ),
            "UPDATE jedi_forces",
            "distills UPDATE (`tbl`)",
        )

        self.assertEqual(
            mysql_distill.distill("select c from (tbl1 JOIN tbl2 on (id)) where x=y"),
            "SELECT tbl?",
            "distills SELECT (t1 JOIN t2)",
        )

        self.assertEqual(
            mysql_distill.distill("insert into (t1) value('a')"),
            "INSERT t?",
            "distills INSERT (tbl)",
        )

        # Something that will (should) never distill.
        self.assertEqual(
            mysql_distill.distill("-- how /*did*/ `THIS` #happen?"),
            "",
            "distills nonsense",
        )

        self.assertEqual(
            mysql_distill.distill("peek tbl poke db"), "", "distills non-SQL"
        )

        # Issue 1176: mk-query-digest incorrectly distills queries with certain keywords

        # I want to see first how this is handled.  It's correct because the query
        # really does read from tables a and c table b is just an alias.
        self.assertEqual(
            mysql_distill.distill(
                "select c from (select * from a) as b where exists (select * from c where id is null)"
            ),
            "SELECT a c",
            "distills SELECT with subquery in FROM and WHERE",
        )

        self.assertEqual(
            mysql_distill.distill("select c from t where col='delete'"),
            "SELECT t",
            "distills SELECT with keyword as value (issue 1176)",
        )

        self.assertEqual(
            mysql_distill.distill(
                'SELECT c, replace(foo, bar) FROM t WHERE col <> "insert"'
            ),
            "SELECT t",
            "distills SELECT with REPLACE function (issue 1176)",
        )

        # LOAD DATA
        # https://bugs.launchpad.net/percona-toolkit/+bug/821692
        # INSERT and REPLACE without INTO
        # https://bugs.launchpad.net/percona-toolkit/+bug/984053
        self.assertEqual(
            mysql_distill.distill(
                "LOAD DATA LOW_PRIORITY LOCAL INFILE 'file' INTO TABLE tbl"
            ),
            "LOAD DATA tbl",
            "distill LOAD DATA (bug 821692)",
        )

        self.assertEqual(
            mysql_distill.distill(
                "LOAD DATA LOW_PRIORITY LOCAL INFILE 'file' INTO TABLE `tbl`"
            ),
            "LOAD DATA tbl",
            "distill LOAD DATA (bug 821692)",
        )

        self.assertEqual(
            mysql_distill.distill("insert ignore_bar (id) values (4029731)"),
            "INSERT ignore_bar",
            "distill INSERT without INTO (bug 984053)",
        )

        self.assertEqual(
            mysql_distill.distill("replace ignore_bar (id) values (4029731)"),
            "REPLACE ignore_bar",
            "distill REPLACE without INTO (bug 984053)",
        )

        # IF EXISTS
        # https://bugs.launchpad.net/percona-toolkit/+bug/821690
        self.assertEqual(
            mysql_distill.distill("DROP TABLE IF EXISTS foo"),
            "DROP TABLE foo",
            "distill DROP TABLE IF EXISTS foo (bug 821690)",
        )

        self.assertEqual(
            mysql_distill.distill("CREATE TABLE IF NOT EXISTS foo"),
            "CREATE TABLE foo",
            msg="distill CREATE TABLE IF NOT EXISTS foo",
        )

        self.assertEqual(
            mysql_distill.distill(
                "(select * from table) union all (select * from other_table)"
            ),
            "SELECT UNION table other_table",
            msg="distill UNION",
        )

    def test_strip_comments(self):
        self.assertEqual(
            mysql_distill.strip_comments("select \n--bar\n foo"),
            "select \n\n foo",
            msg="Removes one-line comments",
        )

        self.assertEqual(
            mysql_distill.strip_comments("select foo--bar\nfoo"),
            "select foo\nfoo",
            msg="Removes one-line comments without running them together",
        )

        self.assertEqual(
            mysql_distill.strip_comments("select foo -- bar"),
            "select foo ",
            msg="Removes one-line comments at end of line",
        )
        self.assertEqual(
            mysql_distill.strip_comments("select /*\nhello!*/ 1"),
            "select  1",
            msg="Stripped star comment",
        )
        self.assertEqual(
            mysql_distill.strip_comments("select /*!40101 hello*/ 1"),
            "select /*!40101 hello*/ 1",
            msg="Left version star comment",
        )
