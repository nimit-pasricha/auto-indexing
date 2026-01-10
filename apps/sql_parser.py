from pglast import parse_sql, ast, enums


def extract_query_details(sql_query):
    try:
        tree = parse_sql(sql_query)
    except Exception:
        return []

    findings = []
    for raw_stmt in tree:
        stmt = raw_stmt.stmt
        if isinstance(stmt, ast.SelectStmt):
            # Map all tables and aliases in FROM and JOINs
            alias_map = {}
            _extract_tables(stmt.fromClause, alias_map)

            # Traverse WHERE clause recursively
            if stmt.whereClause:
                findings.extend(_walk_where(stmt.whereClause, alias_map))
        elif isinstance(stmt, (ast.InsertStmt, ast.UpdateStmt, ast.DeleteStmt)):
            table_name = None
            table_name = stmt.relation.relname

            if table_name:
                findings.append(
                    {
                        "table": table_name.lower(),
                        "column": "__WRITE__",  # Special marker for writes
                        "operator": "WRITE",
                    }
                )
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
        col, alias = _get_col_info(node.lexpr)
        table = alias_map.get(
            alias or (list(alias_map.keys())[0] if alias_map else None)
        )

        if not table or not col:
            return results

        if node.kind == enums.A_Expr_Kind.AEXPR_BETWEEN:
            results.append(
                {"table": table.lower(), "column": col.lower(), "operator": "BETWEEN"}
            )

        elif node.kind == enums.A_Expr_Kind.AEXPR_IN:
            op_name = "".join([n.sval for n in node.name])
            results.append(
                {
                    "table": table.lower(),
                    "column": col.lower(),
                    "operator": "NOT IN" if op_name == "<>" else "IN",
                }
            )

        else:
            raw_op = "".join([n.sval for n in node.name if hasattr(n, "sval")])
            op_map = {"~~": "LIKE", "!~~": "NOT LIKE", "<>": "!=", "~~*": "ILIKE"}
            results.append(
                {
                    "table": table.lower(),
                    "column": col.lower(),
                    "operator": op_map.get(raw_op, raw_op),
                }
            )
    return results


def _get_col_info(node):
    if isinstance(node, ast.ColumnRef):
        fields = [f.sval for f in node.fields if hasattr(f, "sval")]
        return (fields[-1], fields[0] if len(fields) > 1 else None)
    return None, None
