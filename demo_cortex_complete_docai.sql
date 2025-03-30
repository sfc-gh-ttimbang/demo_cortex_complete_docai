-- ============================================================================
-- >> Step 0: Set Context <<
-- ============================================================================
-- Create objects
USE ROLE SYSADMIN;

CREATE OR REPLACE DATABASE DEMO_CORTEX_COMPLETE_DOCAI;
CREATE OR REPLACE SCHEMA DEMO_CORTEX_COMPLETE_DOCAI.DOCS;
USE SCHEMA DEMO_CORTEX_COMPLETE_DOCAI.DOCS;

CREATE OR REPLACE WAREHOUSE DEMO_CC_WH
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
WAREHOUSE_SIZE = 'XSMALL';

USE WAREHOUSE DEMO_CC_WH;

-- ============================================================================
-- >> Step 1: Create stage to host our files<<
-- ============================================================================

--DROP STAGE IF EXISTS DTI_DOCS;
CREATE OR REPLACE STAGE DTI_DOCS
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
DIRECTORY=(ENABLE=true);


-- ============================================================================
-- >> Step 1: Upload Documents to the stage <<
-- ============================================================================

/*
Use the Snowflake Web UI by navigating to Data → Databases → [DEMO_CORTEX_COMPLETE_DOCAI] → [DOCS] → Stages, then selecting your [DTI_DOCS] and using the Upload Files button
*/

-- ============================================================================
-- >> Step 2: Create objects <<
-- ============================================================================

-- Landing table for parsed documents
CREATE OR REPLACE TABLE docs_raw (
    file_url VARCHAR,              -- URL of the source file in the stage
    relative_path VARCHAR,         -- Relative path within the stage
    parse_result VARIANT,          -- Store the full VARIANT output from PARSE_DOCUMENT
    extracted_text VARCHAR,        -- Holds the extracted plain text
    error_info VARCHAR,            -- Contains error information if extraction fails.
    metadata VARIANT,              -- Other metadata of PARSE_DOCUMENT
    processed_timestamp TIMESTAMP_LTZ -- Timestamp of when the processing occurred
);

-- Landing table for processed documents
CREATE OR REPLACE TABLE docs_processed (
    file_url VARCHAR,              -- URL of the source file in the stage
    relative_path VARCHAR,         -- Relative path within the stage
    extract_info VARIANT,          -- Store the full VARIANT output from Cortex COMPLETE Structured Output
    processed_timestamp TIMESTAMP_LTZ -- Timestamp of when the processing occurred
);

-- ============================================================================
-- >> Step 2: Process Documents in Stage using PARSE_DOCUMENT <<
-- ============================================================================

-- Process files from the stage and insert results
INSERT INTO docs_raw (
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
        '@DTI_DOCS',
        DIR.RELATIVE_PATH,
        {'mode': 'LAYOUT'})
    ) AS OCR_LAYOUT, -- Full output of parse_document function
    OCR_LAYOUT:content::VARCHAR as content, -- Raw extracted text 
    OCR_LAYOUT:error_information::VARCHAR as error_information, -- Error information (If unable to process document)
    OCR_LAYOUT:metadata::VARIANT as metadata, -- Parse document metadata
    SYSTIMESTAMP()
FROM DIRECTORY(@DTI_DOCS) DIR;

-- ===========================================================================
-- >> Step 3: Verify OCR Results <<
-- ============================================================================

-- Check a few rows to see the extracted text and status
SELECT
    relative_path,
    LEFT(extracted_text, 500) AS extracted_text_snippet, -- Show beginning of text
    processed_timestamp
FROM docs_raw
LIMIT 10;

-- Inspect the full VARIANT result for a specific file if needed for debugging
-- or understanding the output structure (e.g., if text extraction path is different)
/*
SELECT parse_result
FROM docs_raw
WHERE relative_path = '1.pdf';
*/

-- Count successes and potential errors
SELECT
    SNOWFLAKE.CORE.NULL_COUNT(SELECT error_info FROM docs_raw) AS Success,
    SNOWFLAKE.CORE.NULL_COUNT(SELECT extracted_text FROM docs_raw) AS Errors;

-- ===========================================================================
-- >> Step 4: Extract key fields with Cortex Complete <<
-- ============================================================================

-- Use Cortex COMPLETE structured output to extract key information and insert results into our results table
INSERT INTO docs_processed
SELECT
file_url,
relative_path,
SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', 
[
 {'role': 'system', 'content': 'Act as an expert data extraction agent specializing in official business documents. Carefully read the provided text from a business name certificate and extract the precise information for the following fields given. Pay close attention to labels and context to differentiate between similar dates (e.g., Issue Date vs. Validity Start Date). **Extraction Fields:** * `Business Name`: The primary name registered for the business. * `Business Owner`: The individual listed as the owner/proprietor. * `Validity Date Start`: The specific date the business name registration becomes effective. * `Validity Date End`: The specific date the business name registration expires. * `Business Address`: The complete registered address of the business. * `Issue Date`: The date the certificate document was officially issued. * Ensure dates are extracted in a consistent format if possible (e.g., YYYY-MM-DD), otherwise, extract them exactly as they appear.'}, 
 {'role': 'user', 'content': extracted_text}
],       
      {     'temperature': 0,
            'response_format':{
            'type':'json',
            'schema':{'type' : 'object','properties' : 
             {'document_entities':{'type':'array','items':{'type':'object','properties':
                {
                     'business_name': {'type':'string', 'description': 'The primary name registered for the business'}, 
                     'business_owner': {'type':'string', 'description': 'The individual listed as the owner/proprietor.'},
                     'business_address': {'type':'string', 'description': 'The complete registered address of the business.'},
                     'validity_date_start': {'type':'string', 'description': 'The specific date the business name registration becomes effective.'},
                     'validity_date_end': {'type':'string', 'description': 'The specific date the business name registration expires.'},
                     'issue_date': {'type':'string', 'description': 'The date the certificate document was officially issued.'}
                }}}}}}})
AS extract_info,
SYSTIMESTAMP()
FROM docs_raw;

/*
Balance model of choice between performance vs cost:
Flagship complex models:
deepseek-r1:              3.06 credits / million tokens
llama3.1-405b:            3.00 credits / million tokens
snowflake-llama-3.1-405b: 0.96 credits / million tokens
claude-3-5-sonnet:        2.55 credits / million tokens
mistral-large2:           1.95 credits / million tokens


Smaller models:
llama3.3-70b:             1.21 credits / million tokens
snowflake-llama-3.3-70b:  0.29 credits / million tokens
snowflake-arctic:         0.84 credits / million tokens
llama3.1-8b:              0.19 credits / million tokens
*/

-- ===========================================================================
-- >> Step 5: Verify results and Create a view to for user ease of use <<
-- ============================================================================

-- Verify results
SELECT * FROM docs_processed;

-- Create a view for ease of use and reading
CREATE OR REPLACE VIEW VW_DOCS_PROCESSED AS
SELECT
    file_url,
    relative_path,
    -- Extract each key from the 'document_entities' object
    -- The 'entity.value' represents each individual object within the 'document_entities' array
    entity.value:business_address::STRING    AS business_address,
    entity.value:business_name::STRING      AS business_name,
    entity.value:business_owner::STRING     AS business_owner,
    entity.value:issue_date::DATE           AS issue_date,         -- Cast to DATE
    entity.value:validity_date_end::DATE    AS validity_date_end,  -- Cast to DATE
    entity.value:validity_date_start::DATE  AS validity_date_start, -- Cast to DATE
    extract_info:model::string as model,
    extract_info:usage::variant as usage,
    processed_timestamp
FROM
    docs_processed t,
    LATERAL FLATTEN(input => t.EXTRACT_INFO:structured_output[0]:raw_message:document_entities) entity
WHERE
    IS_ARRAY(t.EXTRACT_INFO:structured_output[0]:raw_message:document_entities);

SELECT * FROM VW_DOCS_PROCESSED;

-- ===========================================================================
-- >> Dismantle Demo <<
-- ===========================================================================

--DROP DATABASE DEMO_CORTEX_COMPLETE_DOCAI;
--DROP WAREHOUSE DEMO_CC_WH;
