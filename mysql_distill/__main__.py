"""
Usage:
    mysql-distill --query "SELECT * FROM table WHERE id = 1"
    cat queries.sql | mysql-distill
"""
import argparse
import re
import sys

import mysql_distill

parser = argparse.ArgumentParser()
parser.add_argument("--query")
args = parser.parse_args()

if args.query:
    print(mysql_distill.distill(args.query))
else:
    for line in sys.stdin:
        query = line.rstrip().split(";")[0]  # Split on ';', get first part
        query = re.sub(
            r"^#.*$", "", query, flags=re.MULTILINE
        )  # Remove lines starting with '#'
        query = query.lstrip()  # Remove leading whitespaces
        if re.match(r"^[\w(]", query):  # Continue if query starts with a word character
            print(mysql_distill.distill(query))
