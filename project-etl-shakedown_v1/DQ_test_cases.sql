DQ Test Catalogue (Designed Tests)
🧱 1. Structural & Schema Tests

Table exists

Table is readable

Expected schema name

Expected table type (table / view)

Column count matches metadata

Column names match metadata

Column data types match metadata

Mandatory columns present

No unexpected columns

Column order validation (contract-based)

📊 2. Row Count & Volume Tests

Row count > 0

Row count within expected min/max

Row count delta vs previous run

Row count reconciliation (source → target)

Partition row count validation

Duplicate batch load detection

🧩 3. Null & Completeness Tests

NOT NULL columns have no nulls

Mandatory business fields populated

Null percentage below threshold

Conditional null checks (if A then B not null)

🔐 4. Uniqueness & Key Tests

Primary key uniqueness

Composite key uniqueness

Business key uniqueness

Surrogate key uniqueness

No duplicate records

One current record per business key (SCD)

🔗 5. Referential Integrity Tests

No orphan child records

Foreign keys exist in parent table

FK nullability respected

Fact → dimension key validation

Late arriving dimension detection

Invalid reference value detection

📐 6. Data Type & Format Tests

Numeric fields contain only numeric values

Date fields contain valid dates

Timestamp precision validation

String length within limits

No truncation detected

Invalid datatype coercion detection

🧮 7. Domain & Allowed Value Tests

Value in allowed list

Case consistency check

Invalid enum detection

No placeholder values (UNKNOWN, N/A, 9999)

Boolean consistency checks

📏 8. Business Rule Tests

Date range validity (end_date >= start_date)

Logical consistency checks

Status-based rule validation

Mutually exclusive column checks

Amount >= 0 checks

Threshold-based validations

🔄 9. Cross-Table Reconciliation Tests

Row count reconciliation

Key count reconciliation

Aggregate reconciliation (SUM / COUNT)

Hash-based row comparison

Duplicate propagation detection

Data drift across layers

🕰️ 10. Timeliness & Freshness Tests

Latest load timestamp within SLA

Missing batch detection

Duplicate batch detection

Late-arriving data detection

Partial load detection

🧬 11. SCD (Slowly Changing Dimension) Tests

Only one current record per key

Correct effective/expiry dates

No overlapping date ranges

Correct version increment

No unnecessary versioning

Change detection accuracy

🧾 12. Audit & Metadata Tests

Load timestamp populated

Source system populated

Batch ID populated

Run ID populated

Created/updated audit fields populated

Lineage column integrity

📈 13. Distribution & Trend Tests

Value distribution drift detection

Category frequency comparison

Spike/drop anomaly detection

Historical trend comparison

Seasonal deviation checks

🔐 14. Security & Compliance Tests

PII columns masked

Sensitive data only in secure schemas

Test data not in prod

Prod data not in test

Row-level security compliance

⚙️ 15. Operational & Performance Tests

Table statistics available

Partition pruning effective

Skew detection

Exploding join detection

Query performance baseline check