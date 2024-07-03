CREATE TABLE first_name
(
	id INT PRIMARY KEY,
	first_name VARCHAR
);

CREATE TABLE last_name
(
	id INT PRIMARY KEY,
	last_name VARCHAR
);

CREATE TABLE patronymic
(
	id INT PRIMARY KEY,
	patronymic VARCHAR
);

INSERT INTO last_name (id, last_name) VALUES
	('1', 'Иванов'),
	('2', 'Петров'),
	('3', 'Сидоров');

INSERT INTO first_name (id, first_name) VALUES
	('1', 'Иван'),
	('2', 'Петр'),
	('3', 'Сидор');

INSERT INTO patronymic (id, patronymic) VALUES
	('1', 'Иванович'),
	('2', 'Петрович'),
	('3', 'Сидорович');

SELECT l.last_name || ' ' || f.first_name || ' ' || p.patronymic AS full_name
FROM last_name AS l
INNER JOIN first_name AS f ON l.id = f.id
INNER JOIN patronymic AS p ON l.id = p.id
ORDER BY full_name DESC;