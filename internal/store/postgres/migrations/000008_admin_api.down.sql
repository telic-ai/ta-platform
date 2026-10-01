DROP INDEX interviews_company_created_idx;
DROP TABLE company_policies;
DROP TABLE interview_scores;
ALTER TABLE users DROP CONSTRAINT users_role_known_check;
