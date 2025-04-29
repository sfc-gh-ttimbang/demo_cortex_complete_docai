-- ============================================================================
-- >> Step 0: Set Context <<
-- ============================================================================
-- Create objects
USE ROLE SYSADMIN;
USE SCHEMA DEMO_CORTEX_COMPLETE_DOCAI.DOCS;
USE WAREHOUSE DEMO_CC_WH;

-- Landing table for parsed watermarked documents
CREATE OR REPLACE TABLE reports_docs_raw (
    file_url VARCHAR,              -- URL of the source file in the stage
    relative_path VARCHAR,         -- Relative path within the stage
    parse_result VARIANT,          -- Store the full VARIANT output from PARSE_DOCUMENT
    extracted_text VARCHAR,        -- Holds the extracted plain text
    error_info VARCHAR,            -- Contains error information if extraction fails.
    metadata VARIANT,              -- Other metadata of PARSE_DOCUMENT
    processed_timestamp TIMESTAMP_LTZ -- Timestamp of when the processing occurred
);

-- Process files from the stage and insert results
INSERT INTO reports_docs_raw (
    file_url,
    relative_path,
    parse_result,
    extracted_text,
    error_info,
    metadata,
    processed_timestamp
)
SELECT
    FILE_URL,
    RELATIVE_PATH,
    TO_VARIANT(
    SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
        '@REPORT_DOCS',
        DIR.RELATIVE_PATH,
        {'mode': 'OCR'}) -- OCR vs Layout
    ) AS OCR_LAYOUT, -- Full output of parse_document function
    OCR_LAYOUT:content::VARCHAR as content, -- Raw extracted text 
    OCR_LAYOUT:error_information::VARCHAR as error_information, -- Error information (If unable to process document)
    OCR_LAYOUT:metadata::VARIANT as metadata, -- Parse document metadata
    SYSTIMESTAMP()
FROM DIRECTORY(@REPORT_DOCS) DIR;

-- Check a few rows to see the extracted text and status
SELECT
    relative_path,
    LEFT(extracted_text, 500) AS extracted_text_snippet, -- Show beginning of text
    processed_timestamp
FROM reports_docs_raw
LIMIT 10;

-- Check token count
SELECT SNOWFLAKE.CORTEX.COUNT_TOKENS('snowflake-arctic', extracted_text) FROM reports_docs_raw;
-- 177894 Tokens

-- Landing table for processed watermarked documents
CREATE OR REPLACE TABLE reports_docs_chunk (
    relative_path VARCHAR,         -- Relative path within the stage
    chunks VARCHAR,          -- Store the full VARIANT output from Cortex COMPLETE Structured Output
    processed_timestamp TIMESTAMP_LTZ -- Timestamp of when the processing occurred
);

-- Chunk text and insert into chunked table
insert into reports_docs_chunk
-- split text
SELECT
   relative_path,
   c.value,
   SYSDATE()
FROM
   reports_docs_raw,
   LATERAL FLATTEN( input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER (
      extracted_text,
      'none',
      500, -- chunk size characters
      100 -- overlap
   )) c;

-- check chunked text
select * from reports_docs_chunk limit 10;

-- Create Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE report_search_service
  ON chunks
  ATTRIBUTES relative_path
  WAREHOUSE = demo_cc_wh
  TARGET_LAG = '1 day'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  AS (
    SELECT
        chunks,
        relative_path
    FROM reports_docs_chunk
);

-- Inspect vectors
SELECT
  *
FROM
  TABLE (
    CORTEX_SEARCH_DATA_SCAN (
      SERVICE_NAME => 'report_search_service'
    )
  ) limit 10;
  
-- Try extraction
with rag as (
SELECT
PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'demo_cortex_complete_docai.docs.report_search_service',
      '{
        "query": "What is Globe\'s consolidates services revenue?",
        "columns":[
            "chunks",
            "relative_path"
        ],
        "filter": {"@eq": {"relative_path": "Globe-2024-Integrated-Report.pdf"} },
        "limit":2
      }'
  )
)['results'] as retrieval
) select
retrieval[0]:chunks::string as retrieve_chunk,
SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', 
[
 {'role': 'system', 'content': 'Act as an expert data extraction agent specializing in official annual report documents. Carefully read the provided text from snippet of an annual report extract the precise information for the following fields given. **Extraction Fields:** * `Consolidates Services Revenue`: Total consolidated service revenue in philippine pesos. *'}, 
 {'role': 'user', 'content': retrieve_chunk}
],       
      {     'temperature': 0,
            'response_format':{
            'type':'json',
            'schema':{'type' : 'object','properties' : 
             {'document_entities':{'type':'array','items':{'type':'object','properties':
                {
                     'services_revenue': {'type':'number', 'description': 'Total consolidated service revenue in philippine pesos.'}
                }}}}}}})
AS extract_info,
from rag;

WITH RAG AS (
SELECT
CONCAT(
PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'demo_cortex_complete_docai.docs.report_search_service',
      '{
        "query": "What is Globe\'s net income after tax in 2024?",
        "columns":[
            "chunks",
            "relative_path"
        ],
        "filter": {"@eq": {"relative_path": "Globe-2024-Integrated-Report.pdf"} },
        "limit":1
      }'
  )
)['results'][0]:chunks::string
, ' | '
,
PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'demo_cortex_complete_docai.docs.report_search_service',
      '{
        "query": "What is Globe\'s consolidates services revenue?",
        "columns":[
            "chunks",
            "relative_path"
        ],
        "filter": {"@eq": {"relative_path": "Globe-2024-Integrated-Report.pdf"} },
        "limit":1
      }'
  )
)['results'][0]:chunks::string
) as retrieval
) SELECT 
RETRIEVAL as retrieve_chunk,
SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', 
[
 {'role': 'system', 'content': 'Act as an expert data extraction agent specializing in official annual report documents. Carefully read the provided text from snippet of an annual report extract the precise information for the following fields given. **Extraction Fields:** * `Consolidates Services Revenue`: Total consolidated service revenue in philippine pesos. * `Net Income After Tax`: Total net income after taxes.*'}, 
 {'role': 'user', 'content': retrieve_chunk}
],       
      {     'temperature': 0,
            'response_format':{
            'type':'json',
            'schema':{'type' : 'object','properties' : 
             {'document_entities':{'type':'array','items':{'type':'object','properties':
                {
                     'services_revenue': {'type':'number', 'description': 'Total consolidated service revenue in philippine pesos.'},
                     'net_income': {'type':'number', 'description': 'Total net income after tax in philippine pesos.'}
                }}}}}}})
AS extract_info, sysdate()
FROM RAG;

-- save extracts
create or replace table annual_report_extract as
select * from table(result_scan(last_query_id()));

-- check results
select * from annual_report_extract;
