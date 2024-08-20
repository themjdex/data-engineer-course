CREATE TABLE DE_course (
    id          integer PRIMARY KEY,
    name        varchar(40) NOT NULL,
    score       integer NOT NULL
);

INSERT INTO DE_course (id, name, score) VALUES (1, 'Marat', 5)