-- Create the recipes table with an ARRAY column for ingredients (sample rows)
-- Note: this uses CREATE TABLE AS SELECT to create a physical table with sample data.
CREATE FOLDER IF NOT EXISTS dremio.recipes;
CREATE TABLE IF NOT EXISTS dremio.recipes.recipes AS
SELECT 1 AS "id",
       'Mild Salsa' AS "name",
       ARRAY['tomato','onion','cilantro','jalapeno','lime'] AS "ingredients",
       CURRENT_TIMESTAMP AS "created_at"
UNION ALL
SELECT 2, 'Medium Chili', ARRAY['beef','tomato','onion','chili powder','cumin','jalapeno'], CURRENT_TIMESTAMP
UNION ALL
SELECT 3, 'Spicy Vindaloo', ARRAY['chicken','chili','ginger','garlic','vinegar','habanero'], CURRENT_TIMESTAMP;

-- Create View where AI is used to classify each recipe as Mild, Medium or Spicy
CREATE OR REPLACE VIEW dremio.recipes.recipes_enhanced AS SELECT id,
       name,
       ingredients,
       AI_CLASSIFY('Identify the Spice Level:' || ARRAY_TO_STRING(ingredients, ','), ARRAY [ 'mild', 'medium', 'spicy' ]) AS spice_level
from   dremio.recipes.recipes;
