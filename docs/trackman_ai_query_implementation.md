# Trackman AI-Powered Query Implementation

## Overview

This document describes the implementation of AI-powered natural language querying for Trackman data, which enhances the existing intent-based query system with flexible LLM-generated SQL queries and multi-step analysis capabilities.

## Architecture

### Three-Tier Query System

The implementation uses a **three-tier architecture**:

1. **Intent-based queries**: Fast, pre-defined SQL for common query patterns
2. **AI-generated queries**: LLM-powered SQL generation for custom questions (with caching)
3. **Multi-step analysis**: Coordinated multi-query analysis with synthesis

```
User Question
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│  Semantic Kernel Orchestrator (auto_invoke=False)           │
│  Single tool selection per turn                             │
└─────────────────────────┬───────────────────────────────────┘
                          │
         ┌────────────────┼────────────────┬─────────────────┐
         │                │                │                 │
         ▼                ▼                ▼                 ▼
┌─────────────┐  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐
│ query_      │  │ query_      │  │ analyze_     │  │ search_     │
│ trackman_   │  │ trackman_   │  │ trackman_    │  │ documents   │
│ data        │  │ flexible    │  │ data         │  │ (RAG)       │
│ (Intent)    │  │ (AI SQL)    │  │ (Multi-step) │  │             │
└──────┬──────┘  └──────┬──────┘  └──────┬───────┘  └─────────────┘
       │                │                │
       │         ┌──────┴──────┐         │
       │         │ SQL Cache   │         │
       │         │ (7-day TTL) │         │
       │         └──────┬──────┘         │
       │                │                │
       │         ┌──────┴──────┐  ┌──────┴───────────────┐
       │         │ LLM: Gen SQL│  │ Multiple Queries     │
       │         └──────┬──────┘  │ + LLM Synthesis      │
       │                │         └──────┬───────────────┘
       └────────────────┴────────────────┘
                        │
                        ▼
               ┌────────────────┐
               │  Data Source   │
               │  (Redshift/    │
               │   PostgreSQL)  │
               └────────────────┘
```

## Files Modified/Created

### New Files

| File | Purpose |
|------|---------|
| [trackman_nl_query_tool.py](../code/backend/batch/utilities/tools/trackman_nl_query_tool.py) | AI query tool with SQL generation + caching |
| [trackman_analysis_tool.py](../code/backend/batch/utilities/tools/trackman_analysis_tool.py) | Multi-step analysis with query synthesis |

### Modified Files

| File | Changes |
|------|---------|
| [redshift_config.py](../code/backend/batch/utilities/helpers/trackman/redshift_config.py) | Added schema descriptions, SQL generation prompt, validation functions |
| [data_source_interface.py](../code/backend/batch/utilities/helpers/trackman/data_source_interface.py) | Added abstract `execute_custom_query()` method |
| [redshift_data_source.py](../code/backend/batch/utilities/helpers/trackman/redshift_data_source.py) | Implemented `execute_custom_query()` method |
| [excel_data_source.py](../code/backend/batch/utilities/helpers/trackman/excel_data_source.py) | Added stub for `execute_custom_query()` (raises NotImplementedError) |
| [chat_plugin.py](../code/backend/batch/utilities/plugins/chat_plugin.py) | Added `query_trackman_flexible()` Semantic Kernel function |

## Security Guardrails

The implementation includes multiple layers of security:

### 1. SELECT-Only Enforcement
```python
if not sql_upper.startswith("SELECT"):
    return False, "Only SELECT queries are allowed"
```

### 2. Dangerous Pattern Detection
Blocked patterns include:
- `INSERT`, `UPDATE`, `DELETE`, `DROP`, `TRUNCATE`, `ALTER`, `CREATE`
- `GRANT`, `REVOKE`
- Multiple statements (`;`)
- SQL comments (`--`, `/*`)
- File output (`INTO OUTFILE`, `INTO DUMPFILE`)

### 3. Table Allowlist Validation
```python
all_tables = set(ALLOWED_TABLES.keys()) | set(TABLE_ALIASES.keys())
for table in tables_used:
    if table not in all_tables:
        return False, f"Table '{table}' is not in the allowed list"
```

### 4. Automatic LIMIT Enforcement
```python
def add_limit_if_missing(sql_query: str, max_rows: int = 100) -> str:
    if not re.search(r"\bLIMIT\s+\d+", sql_query, re.IGNORECASE):
        return f"{sql_query.rstrip()} LIMIT {max_rows}"
    return sql_query
```

## Query Pipeline

1. **User asks natural language question**
2. **Semantic Kernel routes to `query_trackman_flexible()`**
3. **LLM generates SQL** using schema-aware system prompt
4. **SQL validation** checks for dangerous patterns and table allowlist
5. **LIMIT enforcement** adds row limit if missing
6. **Query execution** via `execute_custom_query()`
7. **Result formatting** as markdown table

## Usage Examples

### Standard Intent-Based Query (Existing)
```
User: "Show me errors from the last 10 days"
→ Uses: query_trackman_data(intent="errors_summary", range_days=10)
→ Pre-defined SQL query
```

### Flexible AI-Powered Query (New)
```
User: "Which 5 facilities have the most disconnections this week?"
→ Uses: query_trackman_flexible(question="Which 5 facilities...")
→ LLM generates: SELECT facility_name, SUM(disconnection_cnt) as total
                 FROM connections
                 WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                 GROUP BY facility_name
                 ORDER BY total DESC
                 LIMIT 5
```

## Configuration

### Schema Description
The `SCHEMA_DESCRIPTION` dictionary in `redshift_config.py` provides LLM-friendly documentation for each table:

```python
SCHEMA_DESCRIPTION = {
    "sessions": {
        "description": "Usage sessions data: strokes, activity dates, facility/bay info",
        "common_uses": ["Usage patterns", "Stroke counts", "Activity analysis"],
        "key_columns": {
            "activity_date": "Date of activity (use for time filtering)",
            ...
        },
    },
    ...
}
```

### System Prompt
The `SQL_GENERATION_SYSTEM_PROMPT` includes:
- Complete schema with allowed tables and columns
- Rules for safe query generation
- Examples of correct query patterns
- Current date for time-based queries

## Testing

### Test the Implementation

1. **Restart the backend:**
```bash
pkill -f "flask run"
cd /Users/Amira/chat-with-your-data-solution-accelerator
source .venv/bin/activate
cd code
nohup python -m flask run --port 5050 --host 0.0.0.0 > /tmp/flask.log 2>&1 &
```

2. **Test standard queries:**
```
"Show me errors from the last 7 days"
"What are the top 5 error messages?"
```

3. **Test flexible queries:**
```
"Which facilities have the most disconnections this month?"
"Compare error rates between FAC001 and FAC002"
"Show me facilities with more than 100 strokes per day"
```

4. **Test multi-step analysis:**
```
"Analyze the health of facility FAC001"
"Compare facilities FAC001 and FAC002"
"What's the trend in errors over the last month?"
"Are errors related to connectivity issues?"
```

## SQL Caching

The implementation includes a 2-tier caching system to reduce LLM calls:

### How It Works

```python
# Cache is pattern-based: numbers are normalized
"show me errors from last 7 days"  → hash1
"show me errors from last 10 days" → hash1  # Same pattern!
```

### Cache Configuration

| Setting | Value | Description |
|---------|-------|-------------|
| TTL | 7 days | Cached SQL expires after 7 days |
| Max Size | 1000 | Maximum cached patterns |
| Pattern Matching | Yes | Numbers normalized for pattern matching |

### Cache Statistics

```python
from backend.batch.utilities.tools.trackman_nl_query_tool import SQLCache
cache = SQLCache()
print(cache.stats())  # {'size': 42, 'max_size': 1000, 'ttl_days': 7.0}
```

## Multi-Step Analysis Tool

The `analyze_trackman_data()` function provides coordinated multi-query analysis:

### Analysis Types

| Type | Description | Required Parameters |
|------|-------------|---------------------|
| `facility_health` | Comprehensive facility assessment | `facility_id` |
| `compare_facilities` | Side-by-side comparison | `facility_ids` (comma-separated) |
| `trend` | Trend analysis over time | `metric` (errors/connectivity/data_quality) |
| `correlation` | Error-connectivity correlation | None (optional: `facility_id`) |

### Example Usage

```
User: "Analyze the health of facility FAC001"
→ analyze_trackman_data(analysis_type="facility_health", facility_id="FAC001")
→ Runs 4 queries: errors, top errors, connectivity, data quality
→ LLM synthesizes insights from all results
```

### Why Not auto_invoke=True?

We chose a dedicated analysis tool over `auto_invoke=True` because:

1. **Predictable behavior**: Tool controls exactly which queries run
2. **Controlled costs**: Known number of LLM calls per analysis type
3. **Easier debugging**: Clear execution path
4. **No runaway loops**: Can't get stuck in infinite tool-calling

## TODO / Considerations

### Completed ✅

1. **[x] Query Caching**
   - 7-day TTL, pattern-based matching
   - Up to 1000 cached patterns

2. **[x] Multi-Step Analysis**
   - Dedicated analysis tool with 4 analysis types
   - Controlled multi-query execution with synthesis

### Must Do Before Production

1. **[ ] Unit Tests**
   - Test SQL generation for various natural language inputs
   - Test validation function with edge cases
   - Test error handling for malformed queries

2. **[ ] Integration Tests**
   - End-to-end tests with actual database
   - Performance benchmarks for query generation

3. **[ ] Prompt Engineering**
   - Fine-tune system prompt based on real user queries
   - Add more examples to improve SQL accuracy
   - Handle edge cases (empty results, invalid questions)

### Should Consider

4. **[ ] Query Caching**
   - Cache common query patterns to reduce LLM calls
   - Hash natural language questions for cache keys

5. **[ ] Query Explanation**
   - Return generated SQL in metadata for transparency
   - Log all generated queries for audit trail

6. **[ ] Rate Limiting**
   - Limit flexible queries per user/session
   - Fall back to standard queries when rate limited

7. **[ ] Cost Monitoring**
   - Track token usage for flexible queries
   - Alert on unusual query volume

### Future Enhancements

8. **[ ] Column-Level Validation**
   - Validate that generated SQL only uses allowed columns
   - Currently only validates tables

9. **[ ] Query Optimization**
   - Add EXPLAIN plan analysis
   - Reject queries with poor execution plans

10. **[ ] Multi-Table Joins**
    - Allow safe JOINs between allowed tables
    - Prevent Cartesian products

11. **[ ] Result Summarization**
    - Use LLM to summarize large result sets
    - Provide natural language insights

## Limitations

1. **Excel Data Source**: The flexible query feature only works with Redshift/PostgreSQL. Excel data sources will raise `NotImplementedError`.

2. **Complex Queries**: Very complex analytical queries may not generate correctly. For these cases, consider adding new intent-based queries.

3. **Schema Changes**: Any changes to the database schema must be reflected in `redshift_config.py` to remain queryable.

## Rollback

If issues arise, the flexible query feature can be disabled by:

1. Removing the `query_trackman_flexible` function from `chat_plugin.py`
2. Removing the `analyze_trackman_data` function from `chat_plugin.py`
3. The existing intent-based `query_trackman_data` function remains unchanged

## Design Decisions

### Why Keep Dual LLM Calls (Not Single)?

The orchestrator makes one LLM call to select a tool, then `query_trackman_flexible` makes another to generate SQL. We kept this separation because:

- **Clean architecture**: SQL generation logic stays in the tool, not the orchestrator
- **Easier maintenance**: Schema changes don't affect orchestrator
- **Better testability**: Each component testable in isolation
- **Acceptable cost**: Extra ~$0.002 per query

### Why Dedicated Analysis Tool (Not auto_invoke)?

Instead of enabling `auto_invoke=True` for multi-step queries, we created `TrackmanAnalysisTool`:

- **Deterministic**: Known queries for each analysis type
- **Cost-controlled**: Fixed number of LLM calls per analysis
- **Debuggable**: Clear execution path
- **Safe**: No risk of infinite loops

---

*Document created: January 14, 2026*
*Implementation version: 2.0 - Added caching and multi-step analysis*
