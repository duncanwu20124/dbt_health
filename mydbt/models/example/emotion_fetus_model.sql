WITH QA AS (
    SELECT
        emo.question AS emo_question,
        emo.answer AS emo_answer,
        fet.question AS fet_question,
        fet.answer AS fet_answer

    FROM {{ source('health_source','emotion') }} AS emo
    JOIN {{ source('health_source','fetus') }} AS fet ON emo.class_id = fet.class_id
)
SELECT DISTINCT
    fet_question AS question,
    fet_answer AS answer
FROM QA
