from pglast import parse_sql, ast


def extract_query_details(sql_query):
    try:
        tree = parse_sql(sql_query)
    except Exception:
        return []

    findings = []
    for raw_stmt in tree:
        stmt = raw_stmt.stmt
        if not isinstance(stmt, ast.SelectStmt):
            continue

        # Map all tables and aliases in FROM and JOINs
        alias_map = {}
        _extract_tables(stmt.fromClause, alias_map)

        # Traverse WHERE clause recursively
        if stmt.whereClause:
            findings.extend(_walk_where(stmt.whereClause, alias_map))
    return findings


def _extract_tables(nodes, res):
    for item in nodes:
        if isinstance(item, ast.RangeVar):
            alias = item.alias.aliasname if item.alias else item.relname
            res[alias] = item.relname
        elif isinstance(item, ast.JoinExpr):
            _extract_tables([item.larg, item.rarg], res)


def _walk_where(node, alias_map):
    results = []
    if isinstance(node, ast.BoolExpr):
        for arg in node.args:
            results.extend(_walk_where(arg, alias_map))
    elif isinstance(node, ast.A_Expr):
        # Normalize Postgres internal ops: ~~ is LIKE, !~~ is NOT LIKE
        raw_op = "".join([n.sval for n in node.name if hasattr(n, "sval")])
        op_map = {"~~": "LIKE", "!~~": "NOT LIKE", "<>": "!=", "~~*": "ILIKE"}
        op = op_map.get(raw_op, raw_op)

        col, alias = _get_col_info(node.lexpr)
        # Use alias map or default to the first table found
        table = alias_map.get(alias or list(alias_map.keys())[0])

        if table and col:
            results.append(
                {"table": table.lower(), "column": col.lower(), "operator": op}
            )
    return results


def _get_col_info(node):
    if isinstance(node, ast.ColumnRef):
        fields = [f.sval for f in node.fields if hasattr(f, "sval")]
        return (fields[-1], fields[0] if len(fields) > 1 else None)
    return None, None
