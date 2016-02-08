CREATE TABLE application (
	id int unsigned,
	name varchar(30),
	type varchar(30) not null,
	priority int unsigned,
	active tinyint,
	complete tinyint,
	last_modified varchar(30),
	num_nodes int unsigned,
	PRIMARY KEY(id)
);

CREATE TABLE job (
	id int unsigned,
	type varchar(30) not null,
	app_id int unsigned,
	active tinyint,
	complete tinyint,
	exe_filename varchar(30) not null,
	last_modified varchar(30),
	num_nodes int unsigned,
	priority int unsigned,
	PRIMARY KEY (id),
	FOREIGN KEY (app_id) REFERENCES application(id) ON DELETE CASCADE
);

CREATE TABLE task (
	id int unsigned,
	job_id int unsigned,
	completing_node varchar(30),
	active tinyint,
	complete tinyint,
	status varchar(70),
	last_modified varchar(30),
	PRIMARY KEY (id),
	FOREIGN KEY (job_id) REFERENCES job(id) ON DELETE CASCADE
);

CREATE TABLE dependency (
	id int unsigned,
	dep_id int unsigned,
	PRIMARY KEY (id, dep_id),
	FOREIGN KEY (id) REFERENCES job(id) ON DELETE CASCADE,
	FOREIGN KEY (dep_id) REFERENCES job(id) ON DELETE CASCADE
);


CREATE TABLE job_file (
	id int unsigned,
	filename varchar(50),
	PRIMARY KEY (id, filename),
	FOREIGN KEY (id) REFERENCES job(id) ON DELETE CASCADE
);