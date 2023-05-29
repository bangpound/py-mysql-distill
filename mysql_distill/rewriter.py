import re
import logging

from typing import Tuple, Pattern, Match, List

import mysql_distill.parser

logger = logging.getLogger(__name__)

VERBS: str = r"(^SHOW|^FLUSH|^COMMIT|^ROLLBACK|^BEGIN|SELECT|INSERT|UPDATE|DELETE|REPLACE|^SET|UNION|^START|^LOCK)"

call_re: Pattern[str] = re.compile(r"\A\s*call\s+(\S+)\(", flags=re.IGNORECASE)

# One-line comments
olc_re: Pattern[str] = re.compile(
    r"(?:--|#)[^'\"\r\n]*(?=[\r\n]|$)", flags=re.MULTILINE
)

# But not /*!version */
mlc_re: Pattern[str] = re.compile(r"/\*[^!].*?\*/", flags=re.DOTALL | re.MULTILINE)

# For SHOW + /*!version */
vlc_re: Pattern[str] = re.compile(r"/\*.*?[0-9]+.*?\*/", flags=re.DOTALL | re.MULTILINE)

# Variation for SHOW
vlc_rf: Pattern[str] = re.compile(
    r"^SHOW.*?/\*![0-9]+(.*?)\*/",
    flags=re.IGNORECASE | re.DOTALL | re.MULTILINE,
)


def distill(query: str) -> str:
    """
    Distill a query into a canonical form

    :param query:
    :return:
    """
    verbs, table = distill_verbs(query)

    if verbs and re.match(r"^SHOW", verbs):
        alias_for = {"SCHEMA": "DATABASE", "KEYS": "INDEX", "INDEXES": "INDEX"}
        for key in alias_for:
            verbs = re.sub(key, alias_for[key], verbs)
        query = verbs
    elif verbs and re.match(r"^LOAD DATA", verbs):
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
    match = call_re.match(query)
    if match:
        return rf"CALL {match.group(1)}", ""

    if re.match(r"\A\s*use\s+", query, re.IGNORECASE):
        return "USE", ""

    if re.match(r"\A\s*UNLOCK TABLES", query, re.IGNORECASE):
        return "UNLOCK", ""

    match = re.match(r"\A\s*xa\s+(\S+)", query, re.IGNORECASE)
    if match:
        return f"XA_{match.group(1)}", ""

    if re.match(r"\A\s*LOAD", query, re.IGNORECASE):
        match = re.search(r"INTO TABLE\s+(\S+)", query, re.IGNORECASE)
        tbl = match.group(1) if match else ""
        tbl = tbl.replace("`", "")
        return f"LOAD DATA {tbl}", ""

    if re.match(r"\Aadministrator command:", query):
        query = query.replace("administrator command:", "ADMIN")
        query = query.upper()
        return query, ""

    query = strip_comments(query)

    if re.match(r"\A\s*SHOW\s+", query, re.IGNORECASE):
        logger.debug(query)
        query = query.upper()
        query = re.sub(r"\s+(?:SESSION|FULL|STORAGE|ENGINE)\b", " ", query)
        query = re.sub(r"\s+COUNT[^)]+\)", "", query)
        query = re.sub(
            r"\s+(?:FOR|FROM|LIKE|WHERE|LIMIT|IN)\b.+",
            "",
            query,
            flags=re.MULTILINE | re.DOTALL,
        )
        query = re.sub(r"\A(SHOW(?:\s+\S+){1,2}).*\Z", r"\1", query, flags=re.DOTALL)
        query = re.sub(r"\s+", " ", query)
        logger.debug(query)
        return query, ""

    dds_match: Match[str] | None = re.match(
        rf"^\s*({mysql_distill.parser.data_def_stmts})\b", query, re.IGNORECASE
    )
    if dds_match:
        dds: str = dds_match.group(1)
        query = re.sub(r"\s+IF(?:\s+NOT)?\s+EXISTS", " ", query, re.IGNORECASE)
        obj_match: Match[str] | None = re.search(
            rf"{dds}.+(DATABASE|TABLE)\b", query, re.IGNORECASE
        )
        obj: str = ""
        if obj_match:
            obj = obj_match.group(1).upper()
        logger.debug('Data definition statement "%s" for %s', dds, obj)
        db_or_tbl_match: Match[str] | None = re.search(
            rf"(?:TABLE|DATABASE)\s+({mysql_distill.parser.tbl_ident_sub})(\s+.*)?",
            query,
            re.IGNORECASE,
        )
        db_or_tbl: str = ""
        if db_or_tbl_match:
            db_or_tbl = db_or_tbl_match.group(1)
        logger.debug("Matches db or table: %s", db_or_tbl)
        return dds.upper() + (" " + obj if obj else ""), db_or_tbl

    verbs = re.findall(rf"\b{VERBS}\b", query, re.IGNORECASE)
    last = ""
    verbs = [last := v for v in map(str.upper, verbs) if v != last]

    if verbs and verbs[0] == "SELECT" and len(verbs) > 1:
        logger.debug('False-positive verbs after SELECT: "%s"', verbs[1:])
        union = any(verb == "UNION" for verb in verbs)
        verbs = ["SELECT", "UNION"] if union else ["SELECT"]

    verb_str = " ".join(verbs)
    return verb_str, ""


def _distill_tables(query: str, table: str) -> List[str]:
    """
    Distill the tables from a query

    :param query:
    :param table:
    :return:
    """
    tables = [
        re.sub(r"(_?)[0-9]+", r"\1?", table_name.replace("`", ""))
        for table_name in mysql_distill.parser.get_tables(query)
        if table_name is not None
    ]

    if table:
        tables.append(table)

    # Remove duplicates while maintaining order
    tables = list(dict.fromkeys(tables))

    return tables


def strip_comments(query: str) -> str:
    """
    Strip comments from a query
    :param query:
    :return:
    """
    query = mlc_re.sub("", query)
    query = olc_re.sub("", query)
    match = vlc_rf.match(query)
    if match:
        qualifier = match.group(1) or ""
        query = vlc_re.sub(qualifier, query)
    return query
