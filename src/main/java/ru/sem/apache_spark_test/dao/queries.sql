-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlDialectInspectionForFile

create table categories(
	id serial PRIMARY KEY,
	Name varchar(50) not null
)

insert into categories (Name) values ('Театр'),('Магазин'),('Достопримечательность'),('Парк'),('Музей')

CREATE TABLE area(
	id serial PRIMARY KEY,
	Name varchar(50) not null
)

insert into areas(Name) values ('Москва'),('Санкт-Петербург')

CREATE TABLE persona(
	id serial PRIMARY KEY,
	Name varchar(50) not null
)

CREATE TABLE places_of_interest(
	place_id serial NOT null PRIMARY KEY,
	Name VARCHAR(100) NOT null,
	category int4 references categories(id) NOT null,
	description varchar(100),
	latitude double precision,
	longitude double precision,
	Area_id int4 references areas(id) NOT null,
	Date varchar(20) NOT null
)

insert into places_of_interest (Name, category, description, latitude, longitude, Area_id, Date)
values
('Государственный академический малый театр', 1, 'Государственный академический малый театр…',55.760176,37.619699, 1,'20190201'),
('Музей Ю. В. Никулина', 5, 'Музей Ю. В. Никулина…',55.757666,37.634706, 1,'20190201'),
('Театр Эстрады', 1, 'Московский театр Эстрады',55.757666,37.634706, 1,'20190201'),
('ГУМ', 2, 'Торговый Центр ГУМ',55.754572,37.621182, 1,'20190203'),
('Парк Зарядье', 4, 'Парк Зарядье...',55.751264,37.628510,1,'20190202'),
('Эрмитаж', 5, 'Государственный Эрмитаж',59.939864,30.314566,2,'20190201'),
('Новая Голландия', 1, 'Остров Новая Голландия',59.929605,30.287086,2,'20190201'),
('Петропавловская крепость', 3, 'Петропавловская крепость...',59.950186,30.317488, 2,'20190203')

CREATE TABLE persona_locations (
	persona_id serial NOT null,
	Date_time varchar(20) NOT null,
	latitude double precision,
	longitude double precision,
	Area_id int4 references areas(id) NOT null,
	Date varchar(20) NOT null
)