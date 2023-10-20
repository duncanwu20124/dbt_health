WITH QA AS (
    SELECT
        emo.question AS emo_question,
        emo.answer AS emo_answer,
        pet.question AS pet_question,
        pet.answer AS pet_answer

    FROM {{ source('health_source','emotion') }} AS emo
    JOIN {{ source('health_source','pet') }} AS pet ON emo.class_id = pet.class_id
)
SELECT DISTINCT
    emo_question AS question,
    emo_answer AS answer
FROM QA
