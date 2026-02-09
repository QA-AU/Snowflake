DQ Category	DQ Check	Native DMF	Native DMF Function	Custom DMF	Custom DQ Logic
Structural	Table exists	❌	N/A	❌	✅
Structural	Column exists	❌	N/A	❌	✅
Structural	Column data type validation	❌	N/A	⚠️	✅
Structural	Column count validation	❌	N/A	❌	✅
Structural	Schema drift detection	❌	N/A	❌	✅
Structural	Column order validation	❌	N/A	❌	✅
Volume	Row count	✅	ROW_COUNT	❌	✅
Volume	Row count delta vs previous run	❌	N/A	❌	✅
Volume	Partition-level row count	❌	N/A	❌	✅
Volume	Duplicate batch detection	❌	N/A	❌	✅
Volume	Source → target reconciliation	❌	N/A	❌	✅
Completeness	NULL count	✅	NULL_COUNT	✅	✅
Completeness	NULL percentage	✅	NULL_PERCENT	✅	✅
Completeness	Blank count	✅	BLANK_COUNT	✅	✅
Completeness	Blank percentage	✅	BLANK_PERCENT	✅	✅
Completeness	Mandatory field populated	❌	N/A	✅	✅
Completeness	Conditional null logic (same row)	❌	N/A	⚠️	✅
Uniqueness	Unique value count	✅	UNIQUE_COUNT	✅	✅
Uniqueness	Duplicate value count	✅	DUPLICATE_COUNT	✅	✅
Uniqueness	Single-column uniqueness	⚠️	UNIQUE_COUNT	⚠️	✅
Uniqueness	Primary key uniqueness	❌	N/A	⚠️	✅
Uniqueness	Composite key uniqueness	❌	N/A	❌	✅
Referential	Foreign key existence	❌	N/A	❌	✅
Referential	Orphan child detection	❌	N/A	❌	✅
Referential	Fact → dimension validation	❌	N/A	❌	✅
Data Type	Numeric statistics	✅	MIN / MAX / AVG / STDDEV	✅	✅
Data Type	Numeric-only validation	❌	N/A	✅	✅
Data Type	Date validity	❌	N/A	✅	✅
Data Type	Timestamp validity	❌	N/A	✅	✅
Data Type	String length bounds	❌	N/A	✅	✅
Data Type	Regex / pattern validation	❌	N/A	✅	✅
Domain	Static allowed values	⚠️	ACCEPTED_VALUES	✅	✅
Domain	Enum validation	⚠️	ACCEPTED_VALUES	✅	✅
Domain	Placeholder detection	❌	N/A	✅	✅
Domain	Reference table lookup	❌	N/A	❌	✅
Business Rule	Value ≥ 0	❌	N/A	✅	✅
Business Rule	Range checks	❌	N/A	✅	✅
Business Rule	Cross-column logic (same row)	❌	N/A	⚠️	✅
Business Rule	Cross-row logic	❌	N/A	❌	✅
Business Rule	Cross-table logic	❌	N/A	❌	✅
Timeliness	Data freshness	✅	FRESHNESS	✅	✅
Timeliness	SLA breach detection	❌	N/A	❌	✅
Timeliness	Missing batch detection	❌	N/A	❌	✅
Timeliness	Partial load detection	❌	N/A	❌	✅
SCD	One current record	❌	N/A	❌	✅
SCD	Overlapping effective dates	❌	N/A	❌	✅
SCD	Correct versioning	❌	N/A	❌	✅
Profiling	Distribution drift detection	❌	N/A	❌	✅
Profiling	Trend analysis over time	❌	N/A	❌	✅
Audit	Load metadata populated	❌	N/A	❌	✅
Audit	Batch / run tracking	❌	N/A	❌	✅
Audit	Historical DQ trends	❌	N/A	❌	✅
Audit	CI/CD enforcement	❌	N/A	❌	✅