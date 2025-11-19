-- ================================
-- Music Classification Example
-- ================================

-- Create a folder for the example
CREATE FOLDER IF NOT EXISTS dremio.music;

-- Create a table with sample music tracks and attributes
CREATE TABLE IF NOT EXISTS dremio.music.tracks AS
SELECT 1 AS "id",
       'Night Drive' AS "title",
       ARRAY['synth','bass','electronic','steady beat'] AS "attributes",
       CURRENT_TIMESTAMP AS "created_at"
UNION ALL
SELECT 2,
       'Steel Sunrise',
       ARRAY['acoustic guitar','vocals','harmonies','storytelling'],
       CURRENT_TIMESTAMP
UNION ALL
SELECT 3,
       'Thunder Ritual',
       ARRAY['distorted guitar','drums','screaming','fast tempo'],
       CURRENT_TIMESTAMP;

-- Classify each track into a genre using AI_CLASSIFY
CREATE OR REPLACE VIEW dremio.music.tracks_enhanced AS
SELECT
  id,
  title,
  attributes,
  AI_CLASSIFY(
    'Classify this song based on its musical attributes: ' || ARRAY_TO_STRING(attributes, ','),
    ARRAY['Electronic', 'Folk', 'Rock', 'Metal', 'Unknown']
  ) AS genre
FROM dremio.music.tracks;



-- ================================
-- Movie Genre Classification Example
-- ================================

CREATE FOLDER IF NOT EXISTS dremio.movies;

-- Sample movies table with keywords describing each movie
CREATE TABLE IF NOT EXISTS dremio.movies.movies AS
SELECT 1 AS "id",
       'Love Across the Galaxy' AS "title",
       ARRAY['romance','space','adventure','emotional'] AS "keywords",
       CURRENT_TIMESTAMP AS "created_at"
UNION ALL
SELECT 2,
       'Shadow Protocol',
       ARRAY['espionage','action','government','intense'],
       CURRENT_TIMESTAMP
UNION ALL
SELECT 3,
       'The Ancient Door',
       ARRAY['mystery','supernatural','discovery','dark'],
       CURRENT_TIMESTAMP;

-- Classify each movie into a genre
CREATE OR REPLACE VIEW dremio.movies.movies_enhanced AS
SELECT
  id,
  title,
  keywords,
  AI_CLASSIFY(
    'Choose the genre that best fits these keywords: ' || ARRAY_TO_STRING(keywords, ','),
    ARRAY['Romance', 'Action', 'Thriller', 'Sci-Fi', 'Horror', 'Fantasy']
  ) AS genre
FROM dremio.movies.movies;




-- ================================
-- Health Level Classification Example
-- ================================

CREATE FOLDER IF NOT EXISTS dremio.health;

-- Sample table describing people with basic health indicators
CREATE TABLE IF NOT EXISTS dremio.health.people AS
SELECT 1 AS "id",
       'Alex M.' AS "name",
       ARRAY['active lifestyle','balanced diet','normal blood pressure'] AS "attributes",
       CURRENT_TIMESTAMP AS "created_at"
UNION ALL
SELECT 2,
       'Jordan T.',
       ARRAY['sedentary','high stress','overweight'],
       CURRENT_TIMESTAMP
UNION ALL
SELECT 3,
       'Casey R.',
       ARRAY['smoker','irregular sleep','low energy'],
       CURRENT_TIMESTAMP;

-- Classify each person into a health level
CREATE OR REPLACE VIEW dremio.health.people_enhanced AS
SELECT
  id,
  name,
  attributes,
  AI_CLASSIFY(
    'Based on these lifestyle indicators, classify the overall health level: ' || ARRAY_TO_STRING(attributes, ','),
    ARRAY['Excellent', 'Good', 'Average', 'Poor']
  ) AS health_level
FROM dremio.health.people;
