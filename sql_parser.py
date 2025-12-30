import sqlparse
from sqlparse.sql import Where, Comparison, Identifier
from sqlparse.tokens import Keyword, DML

def extract_metadata(sql: str):
    parsed = sqlparse.parse(sql)[0]
    
    if parsed.get_type() != 'SELECT':
        return None

    metadata = {
        "table": None,
        "filters": []
    }

    # 1. Extract Table Name
    # We look for the token following the 'FROM' keyword
    from_seen = False
    for token in parsed.tokens:
        if from_seen and isinstance(token, Identifier):
            metadata["table"] = token.get_real_name()
            break
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True

    # 2. Extract WHERE clause details
    for token in parsed.tokens:
        if isinstance(token, Where):
            for i, condition in enumerate(token.tokens):
                if isinstance(condition, Comparison):
                    # item.left is the column, item.value is the whole comparison
                    # e.g., "age > 25" -> left is "age", operator is ">"
                    col = condition.left.value
                    # Extract the operator (>, <=, =, etc.)
                    operator = ""
                    for t in condition.tokens:
                        if t.ttype == sqlparse.tokens.Operator.Comparison:
                            operator = t.value
                    
                    metadata["filters"].append({
                        "column": col.strip(),
                        "operator": operator.strip()
                    })

                elif condition.value.upper() == "IS":
                    prev_item = token.token_prev(i, skip_cm=True)
                    next_item = token.token_next(i, skip_cm=True)
                    metadata["filters"].append({
                        "column": prev_item[1].value,
                        "operator": "IS NULL" if "NOT" not in next_item[1].value else "IS NOT NULL"
                    })
                
                elif condition.ttype is Keyword and condition.value.upper() == 'IN':
                    prev_item = token.token_prev(i, skip_cm=True)
                    metadata["filters"].append({
                        "column": prev_item[1].value,
                        "operator": "IN"
                    })

    return metadata

# --- TEST IT ---
test_sql = "SELECT * FROM users WHERE age IN ('A', 'B', 'C')"
data = extract_metadata(test_sql)
print(data)