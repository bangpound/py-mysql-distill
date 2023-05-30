"""
Rewrite queries for canonical form

The distill() function is the main entry point. It takes a query string
and returns the canonical form of the query.

The distill_verbs() function is a helper function that distills only the
verbs from a query. It is used by the distill() function.

The distill_tables() function is a helper function that distills only the
tables from a query. It is used by the distill() function.

The strip_comments() function is a helper function that removes comments
from a query. It is used by the distill_verbs() function.

The following variables are regular expressions that match various types
of comments in a query:

olc_re: One-line comments
mlc_re: Multi-line comments
vlc_re: Version comments
vlc_rf: Version comments for SHOW queries

The following variables are regular expressions that match the start of
various types of queries:

call_re: Stored procedure calls
VERBS:   Verbs that start queries
"""
import re
import logging

from typing import Tuple, Pattern, Match, List, Optional

import mysql_distill

_logger = logging.getLogger(__name__)

_verbs_re_pattern: str = (
    r"(^SHOW|^FLUSH|^COMMIT|^ROLLBACK|^BEGIN|SELECT|"
    r"INSERT|UPDATE|DELETE|REPLACE|^SET|UNION|^START|^LOCK)"
)
_verbs_re = re.compile(rf"\b{_verbs_re_pattern}\b", re.IGNORECASE)

# One-line comments
_olc_re: Pattern[str] = re.compile(
    r"(?:--|#)[^'\"\r\n]*(?=[\r\n]|$)", flags=re.MULTILINE
)

# But not /*!version */
_mlc_re: Pattern[str] = re.compile(r"/\*[^!].*?\*/", flags=re.DOTALL | re.MULTILINE)

# For SHOW + /*!version */
_vlc_re: Pattern[str] = re.compile(
    r"/\*.*?[0-9]+.*?\*/", flags=re.DOTALL | re.MULTILINE
)

# Variation for SHOW
_vlc_rf: Pattern[str] = re.compile(
    r"^SHOW.*?/\*![0-9]+(.*?)\*/",
    flags=re.IGNORECASE | re.DOTALL | re.MULTILINE,
)

_verb_call_re: Pattern[str] = re.compile(r"\A\s*call\s+(\S+)\(", flags=re.IGNORECASE)
_verb_show_re: Pattern[str] = re.compile(r"^SHOW", flags=re.IGNORECASE)
_verb_show_ws_re = re.compile(r"\A\s*SHOW\s+", re.IGNORECASE)
_verb_load_re = re.compile(r"\A\s*LOAD", flags=re.IGNORECASE)
_verb_admin_command_re = re.compile(r"\Aadministrator command:")
_verb_load_data_re: Pattern[str] = re.compile(r"^LOAD DATA", flags=re.IGNORECASE)
_verb_use_re: Pattern[str] = re.compile(r"^USE", flags=re.IGNORECASE)
_verb_unlock_re: Pattern[str] = re.compile(r"\A\s*UNLOCK TABLES", flags=re.IGNORECASE)
_verb_xa_re: Pattern[str] = re.compile(r"\A\s*xa\s+(\S+)", flags=re.IGNORECASE)

_show_modifier_re = re.compile(r"\s+(?:SESSION|FULL|STORAGE|ENGINE)\b")
_show_modifier_count_re = re.compile(r"\s+COUNT[^)]+\)")
_show_modifier_predicate_re = re.compile(
    r"\s+(?:FOR|FROM|LIKE|WHERE|LIMIT|IN)\b.+", flags=re.MULTILINE | re.DOTALL
)
_show_modifier_2_rs = re.compile(r"\A(SHOW(?:\s+\S+){1,2}).*\Z", flags=re.DOTALL)
_whitespace_re = re.compile(r"\s+")

_predicate_into_table_re = re.compile(r"INTO TABLE\s+(\S+)", flags=re.IGNORECASE)

_dds_match_re: Pattern[str] = re.compile(
    rf"^\s*({mysql_distill.parser.data_def_stmts})\b", flags=re.IGNORECASE
)
_table_name_rewrite_re: Pattern[str] = re.compile(r"(_?)[0-9]+")


def distill(query: str) -> str:
    """
    Distill a query into a canonical form

    :param query:
    :return:
    """
    verbs, table = distill_verbs(query)

    if verbs and _verb_show_re.match(verbs):
        alias_for = {"SCHEMA": "DATABASE", "KEYS": "INDEX", "INDEXES": "INDEX"}
        for alias_for_key, alias_for_value in alias_for.items():
            verbs = verbs.replace(alias_for_key, alias_for_value)
        query = verbs
    elif verbs and _verb_load_data_re.match(verbs):
        return verbs
    else:
        tables = _distill_tables(query, table)
        query = " ".join([verbs] + tables)

    return query


def distill_verbs(query: str) -> Tuple[str, str]:
    """
    Distill the verbs from a query

    :param query:
    :return:
    """
    match: Optional[Match[str]] = _verb_call_re.match(query)
    if match:
        return rf"CALL {match.group(1)}", ""

    if _verb_use_re.match(query):
        return "USE", ""

    if _verb_unlock_re.match(query):
        return "UNLOCK", ""

    match = _verb_xa_re.match(query)
    if match:
        return f"XA_{match.group(1)}", ""

    if _verb_load_re.match(query):
        match = _predicate_into_table_re.search(query)
        tbl = ""
        if match:
            tbl = match.group(1)
        tbl = tbl.replace("`", "")
        return f"LOAD DATA {tbl}", ""

    if _verb_admin_command_re.match(query):
        query = query.replace("administrator command:", "ADMIN")
        query = query.upper()
        return query, ""

    query = strip_comments(query)

    if _verb_show_ws_re.match(query):
        _logger.debug(query)
        query = query.upper()
        query = _show_modifier_re.sub(" ", query)
        query = _show_modifier_count_re.sub("", query)
        query = _show_modifier_predicate_re.sub("", query)
        query = _show_modifier_2_rs.sub(r"\1", query)
        query = _whitespace_re.sub(" ", query)
        _logger.debug(query)
        return query, ""

    dds_match: Optional[Match[str]] = _dds_match_re.match(query)
    if dds_match:
        dds: str = dds_match.group(1)
        query = re.sub(r"\s+IF(?:\s+NOT)?\s+EXISTS", " ", query, re.IGNORECASE)
        obj_match: Optional[Match[str]] = re.search(
            rf"{dds}.+(DATABASE|TABLE)\b", query, re.IGNORECASE
        )
        obj: str = ""
        if obj_match:
            obj = obj_match.group(1).upper()
        _logger.debug('Data definition statement "%s" for %s', dds, obj)
        db_or_tbl_match: Optional[Match[str]] = re.search(
            rf"(?:TABLE|DATABASE)\s+({mysql_distill.tbl_ident_sub})(\s+.*)?",
            query,
            re.IGNORECASE,
        )
        db_or_tbl: str = ""
        if db_or_tbl_match:
            db_or_tbl = db_or_tbl_match.group(1)
        _logger.debug("Matches db or table: %s", db_or_tbl)
        return dds.upper() + (" " + obj if obj else ""), db_or_tbl

    verbs = _verbs_re.findall(query)
    last = ""
    verbs = [last := v for v in map(str.upper, verbs) if v != last]

    if verbs and verbs[0] == "SELECT" and len(verbs) > 1:
        _logger.debug('False-positive verbs after SELECT: "%s"', verbs[1:])
        union = any(verb == "UNION" for verb in verbs)
        verbs = ["SELECT", "UNION"] if union else ["SELECT"]

    verb_str = " ".join(verbs)
    return verb_str, ""


def _distill_tables(query: str, table: str) -> List[str]:
    """
    Distill the tables from a query

    :param query: The query to distill
    :param table: The table to add to the list of tables
    :return: The list of tables
    """
    tables = [
        _table_name_rewrite_re.sub(r"\1?", table_name.replace("`", ""))
        for table_name in mysql_distill.get_tables(query)
        if table_name is not None
    ]

    if table:
        tables.append(table)

    # Remove duplicates while maintaining order
    last = ""
    tables = [last := table_name for table_name in tables if table_name != last]

    return tables


def strip_comments(query: str) -> str:
    """
    Strip comments from a query
    :param query: The query to strip comments from
    :return:
    """
    query = _mlc_re.sub("", query)
    query = _olc_re.sub("", query)
    match = _vlc_rf.match(query)
    if match:
        qualifier = match.group(1)
        query = _vlc_re.sub(qualifier, query)
    return query
