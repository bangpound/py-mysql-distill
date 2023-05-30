"""
Parser for MySQL queries.

This file is part of the mysql_distill package.
"""
import re
import logging

from typing import List, Pattern, Match

_logger = logging.getLogger(__name__)

# Table Identifier Regex
tbl_ident_sub: str = r"(?:`[^`]+`|\w+)(?:\.(?:`[^`]+`|\w+))?"

# Table Regex
_tbl_regex: Pattern[str] = re.compile(
    r"""\b(?:FROM|JOIN|(?<!KEY\s)UPDATE|INTO) # Words that precede table names
        \b\s*
        \(?                                   # Optional paren around tables
        ({tbl_ident}
           (?: (?:\s+ (?:AS\s+)? \w+)?, \s*{tbl_ident} )*
        )""".format(
        tbl_ident=tbl_ident_sub
    ),
    re.IGNORECASE | re.VERBOSE,
)

_tbl_split_re = re.compile(rf"\s*({tbl_ident_sub})(\s+.*)?", flags=re.IGNORECASE)

# Data Definition Statements
data_def_stmts: str = r"(?:CREATE|ALTER|TRUNCATE|DROP|RENAME)"

# Data Manipulation Statements
# data_manip_stmts: Pattern[str] = re.compile(r"(?:INSERT|UPDATE|DELETE|REPLACE)", re.IGNORECASE)
_ddl_stmts: Pattern[str] = re.compile(rf"^\s*({data_def_stmts})\b", flags=re.IGNORECASE)

_tbl_ident: Pattern[str] = re.compile(
    rf"TABLE\s+({tbl_ident_sub})(\s+.*)?", flags=re.IGNORECASE
)
_modifier_re = re.compile(
    r"(?:LOW_PRIORITY|IGNORE|STRAIGHT_JOIN|DELAYED)\s+", flags=re.IGNORECASE
)
_verb_lock_tables_re = re.compile(r"^\s*LOCK TABLES\s+", flags=re.IGNORECASE)

_unescape_quotes_re = re.compile(r"""\\["']""")
_quoted_strings_rs = (
    re.compile('".*?"', flags=re.DOTALL),
    re.compile("'.*?'", flags=re.DOTALL),
)
_verb_insert_replace_re = re.compile(
    r"\A\s*(?:INSERT|REPLACE)(?!\s+INTO)", flags=re.IGNORECASE
)
_verb_load_data_re = re.compile(r"\A\s*LOAD DATA", flags=re.IGNORECASE)


def get_tables(query: str) -> List[str]:
    """
    Get all tables used in a query

    :param query: Query to parse
    :return: List of tables
    """

    _logger.debug("Getting tables for %s", query)

    match = _ddl_stmts.search(query)
    if match:
        ddl_stmt = match.group(1)
        _logger.debug("Special table type: %s", ddl_stmt)
        query = re.sub(r"IF\s+(?:NOT\s+)?EXISTS", "", query, flags=re.IGNORECASE)

        if re.search(rf"{ddl_stmt} DATABASE\b", query, flags=re.IGNORECASE):
            _logger.debug("Query alters database, not a table")
            return []

        if re.search(r"CREATE.+?\bSELECT\b", query, flags=re.IGNORECASE):
            select = re.search(
                r"\b(SELECT\b.+)", query, flags=re.IGNORECASE | re.DOTALL
            )
            assert select
            _logger.debug("CREATE TABLE ... SELECT: %s", select.group(1))
            return get_tables(select.group(1))
        ddl_tbl_match = _tbl_ident.search(query)
        _logger.debug(
            "Table match: %s", ddl_tbl_match.group(1) if ddl_tbl_match else None
        )
        return list([ddl_tbl_match.group(1)]) if ddl_tbl_match else []

    query = _modifier_re.sub(" ", query)

    if _verb_lock_tables_re.search(query):
        query = re.sub(r"^\s*LOCK TABLES\s+", "", query, flags=re.IGNORECASE)
        _logger.debug("Special table type: LOCK TABLES")
        query = re.sub(
            r"\s+(?:READ(?:\s+LOCAL)?|WRITE)\s*", "", query, flags=re.IGNORECASE
        )
        _logger.debug("Locked tables: %s")
        query = "FROM " + query

    # quoted strings
    query = _unescape_quotes_re.sub("", query)
    for pattern in _quoted_strings_rs:
        query = pattern.sub("?", query)

    if _verb_insert_replace_re.search(query):
        query = re.sub(
            r"\A\s*(INSERT|REPLACE)\s+", r"\1 INTO ", query, flags=re.IGNORECASE
        )

    if _verb_load_data_re.match(query):
        tbl_match: Match[str] | None = re.search(
            r"INTO TABLE\s+(\S+)", query, flags=re.IGNORECASE
        )
        return [tbl_match.group(1)] if tbl_match else []

    tables = []
    tbl_s: str
    for tbl_s in _tbl_regex.findall(query):
        assert isinstance(tbl_s, str)
        _logger.debug("Match tables: %s", tbl_s)
        if re.search(r"\ASELECT\b", tbl_s, re.IGNORECASE):
            continue

        tbl: str
        for tbl in tbl_s.split(","):
            tbl_sub: str = _tbl_split_re.sub(r"\1", tbl)

            if not re.search(r"[a-zA-Z]", tbl_sub):
                _logger.debug("Skipping suspicious table name: %s", tbl_sub)
                continue

            tables.append(tbl_sub)

    return tables
