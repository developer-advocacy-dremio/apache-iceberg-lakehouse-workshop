-- ====================================================================================
-- DREMIO MEDIA WORKFLOW EXAMPLE USING AI FUNCTIONS
-- ====================================================================================
-- Scenario:
--   You run a digital media organization that publishes articles, reviews,
--   and breaking news pieces. Writers submit draft content as raw text
--   stored in a table (simulating unstructured documents).
--
--   The goal is to:
--     • Extract structured metadata from messy article drafts         → AI_GENERATE
--     • Classify the article into editorial categories                → AI_CLASSIFY
--     • Generate short summaries and SEO descriptions                 → AI_COMPLETE
--     • Demonstrate how LIST_FILES would work if unstructured files
--       (Word docs, PDFs, audio transcripts) were available.
--
--   Pattern:
--     Bronze  = landing tables with raw unstructured text
--     Silver  = AI-enriched views with structured metadata + labels
--     Gold    = business metrics, editorial dashboards
-- ====================================================================================


-- =========================================
-- 1. NAMESPACES
-- =========================================
CREATE FOLDER IF NOT EXISTS dremio.media;
CREATE FOLDER IF NOT EXISTS dremio.media.raw;
CREATE FOLDER IF NOT EXISTS dremio.media.silver;
CREATE FOLDER IF NOT EXISTS dremio.media.gold;


-- =========================================
-- 2. BRONZE: RAW ARTICLE SUBMISSIONS
-- =========================================
-- In production:
--   • These articles might come from emails, CMS exports, Word docs,
--     PDFs, or audio transcript files processed via LIST_FILES.
--
-- In this example:
--   • We simulate raw unstructured content with VARCHAR fields.
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.media.raw.article_submissions (
  article_id     BIGINT,
  author_name    VARCHAR,
  draft_text     VARCHAR,    -- unstructured content
  submitted_at   TIMESTAMP
);

DELETE FROM dremio.media.raw.article_submissions;

INSERT INTO dremio.media.raw.article_submissions (
  article_id, author_name, draft_text, submitted_at
)
VALUES
  (3001, 'Alice Rivera',
    'The new superhero film “Nova Strike” delivered huge box office numbers this weekend. '
    || 'Fans praised its diverse cast and energetic action sequences, though critics noted a weak villain arc.',
    TIMESTAMP '2025-09-01 09:10:00'),

  (3002, 'Marcus Lee',
    'Apple unveiled its newest AR headset today during its annual event. '
    || 'The device integrates health monitoring, gesture control, and holographic collaboration tools.',
    TIMESTAMP '2025-09-01 10:05:00'),

  (3003, 'Dana Fox',
    'A major cyberattack disrupted several hospitals across the East Coast. '
    || 'Officials believe it originated from a ransomware group demanding payments in cryptocurrency.',
    TIMESTAMP '2025-09-01 11:30:00'),

  (3004, 'Jin Park',
    'The indie band “Midnight Trails” released their new album. '
    || 'The sound blends folk influences, lo-fi production, and emotional lyricism about solitude and growth.',
    TIMESTAMP '2025-09-01 13:20:00');


-- =========================================
-- 3. SILVER: AI-ENRICHED METADATA EXTRACTION (AI_GENERATE)
-- =========================================
-- Goal:
--   Extract structured metadata from raw draft_text:
--     • headline
--     • main_topic
--     • entities (people, products, orgs mentioned)
--     • sentiment_score (numeric 0–1, where 1 = very positive)
--
-- Pattern:
--   • AI_GENERATE returns a structured row using WITH SCHEMA.
--   • In production, you would send full files using (prompt, file_reference)
--     from LIST_FILES('@media/articles/2025/').
-- =========================================
CREATE OR REPLACE VIEW dremio.media.silver.article_ai_struct AS
SELECT
  a.*,
  AI_GENERATE(
    'You are an editor extracting metadata from a draft article. From the text, extract: '
    || 'headline (short phrase), '
    || 'main_topic (e.g., Film, Tech, Crime, Music, Politics, Sports), '
    || 'entities (comma-separated list), '
    || 'sentiment_score (float between 0 and 1). '
    || 'Return ONLY the structured fields. TEXT: ' || a.draft_text
    WITH SCHEMA ROW(
      headline         VARCHAR,
      main_topic       VARCHAR,
      entities         VARCHAR,
      sentiment_score  DOUBLE
    )
  ) AS ai_struct
FROM dremio.media.raw.article_submissions a;


CREATE OR REPLACE VIEW dremio.media.silver.article_ai_flat AS
SELECT
  article_id,
  author_name,
  submitted_at,
  draft_text,

  ai_struct['headline']        AS headline,
  ai_struct['main_topic']      AS main_topic,
  ai_struct['entities']        AS entities,
  ai_struct['sentiment_score'] AS sentiment_score
FROM dremio.media.silver.article_ai_struct;


-- =========================================
-- 4. SILVER: EDITORIAL CATEGORY CLASSIFICATION (AI_CLASSIFY)
-- =========================================
-- Goal:
--   Label each article with an editorial desk:
--     ['Entertainment', 'Technology', 'Politics', 'Health', 'Music', 'World', 'Finance', 'Other']
--
-- Pattern:
--   • AI_CLASSIFY chooses ONE of the given categories.
--   • Useful for routing content automatically inside the CMS.
--
-- Generalization:
--   • With LIST_FILES, you can classify PDFs, audio transcripts,
--     or social media screenshots directly.
-- =========================================
CREATE OR REPLACE VIEW dremio.media.silver.article_categorized AS
SELECT
  af.*,
  AI_CLASSIFY(
    'Based on this article text, determine the editorial category: '
    || draft_text,
    ARRAY[
      'Entertainment', 'Technology', 'Politics', 'Health',
      'Music', 'World', 'Finance', 'Other'
    ]
  ) AS editorial_category
FROM dremio.media.silver.article_ai_flat af;


-- =========================================
-- 5. SILVER: GENERATE SUMMARIES & SEO DESCRIPTIONS (AI_COMPLETE)
-- =========================================
-- Goal:
--   Produce:
--     • a one-sentence editorial summary for editors
--     • a 140–160 character SEO description for metadata
--
-- Pattern:
--   • AI_COMPLETE is intentionally simple and always VARCHAR.
-- =========================================
CREATE OR REPLACE VIEW dremio.media.silver.article_with_summaries AS
SELECT
  ac.*,

  AI_COMPLETE(
    'Write a concise editorial summary (1 sentence) of this article: ' || draft_text
  ) AS editorial_summary,

  AI_COMPLETE(
    'Write an SEO description (140-160 characters) for this article based on this text: '
    || draft_text
  ) AS seo_description

FROM dremio.media.silver.article_categorized ac;


-- =========================================
-- 6. GOLD: EDITORIAL DASHBOARD METRICS
-- =========================================
-- Goal:
--   Provide newsroom-level metrics:
--     • number of articles per desk
--     • average sentiment by desk
--     • trending topics
-- =========================================
CREATE OR REPLACE VIEW dremio.media.gold.editorial_metrics AS
SELECT
  editorial_category,
  COUNT(*) AS article_count,
  AVG(sentiment_score) AS avg_sentiment,
  LISTAGG(DISTINCT main_topic, ', ') AS topics_covered
FROM dremio.media.silver.article_with_summaries
GROUP BY editorial_category;


-- =========================================
-- 7. GOLD: DAILY CONTENT PRODUCTION SUMMARY
-- =========================================
-- Useful for:
--   • newsroom standups
--   • editorial planning tools
--   • performance dashboards
-- =========================================
CREATE OR REPLACE VIEW dremio.media.gold.daily_content_summary AS
SELECT
  CAST(submitted_at AS DATE) AS publish_day,
  editorial_category,
  COUNT(*) AS total_articles,
  AVG(sentiment_score) AS avg_sentiment_score
FROM dremio.media.silver.article_with_summaries
GROUP BY
  CAST(submitted_at AS DATE), editorial_category;


-- =========================================
-- 8. LIST_FILES EXAMPLE — PROCESSING REAL UNSTRUCTURED MEDIA FILES
-- =========================================
-- EXAMPLE ONLY — WILL NOT RUN UNTIL YOU CONNECT A SOURCE NAMED @media_files
-- Demonstrates how you would analyze:
--   • PDF press releases
--   • Word documents
--   • Audio transcript text files
-- =========================================

/*
WITH press_release_ai AS (
  SELECT
    file['path'] AS source_file,
    AI_GENERATE(
      'gpt.4o',
      ('Extract the following from this press release: '
       || 'headline, organization, announcement_type, sentiment_score, key_topics.',
       file
      )
      WITH SCHEMA ROW(
        headline           VARCHAR,
        organization       VARCHAR,
        announcement_type  VARCHAR,
        sentiment_score    DOUBLE,
        key_topics         VARCHAR
      )
    ) AS info
  FROM TABLE(LIST_FILES('@media_files/press_releases/2025/'))
)
SELECT
  source_file,
  info['headline']          AS headline,
  info['organization']      AS organization,
  info['announcement_type'] AS announcement_type,
  info['sentiment_score']   AS sentiment_score,
  info['key_topics']        AS key_topics
FROM press_release_ai;
*/

-- Explanation:
--   • LIST_FILES returns file handles.
--   • (prompt, file) sends both the prompt and file to the LLM.
--   • This is identical to our VARCHAR-based approach, but now the
--     content is coming from PDFs, docx, audio transcripts, etc.


-- ====================================================================================
-- END OF MEDIA EXAMPLE
-- ====================================================================================
