CREATE TABLE node (
	id varchar(30),
	ip varchar(30) not null,
	type varchar(10),
	latitude float,
	longitude float,
	online long,
	PRIMARY KEY (id, type)
);

CREATE TABLE node_conn (
	source varchar(30),
	dest varchar(30),
	bandwidth float,
	latency float,
	last_update long,
	PRIMARY KEY (source, dest),
	FOREIGN KEY (source) REFERENCES node(id) ON DELETE CASCADE,
	FOREIGN KEY (dest) REFERENCES node(id) ON DELETE CASCADE
);


CREATE TABLE application (
	id long unsigned,
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
	id long unsigned,
	type varchar(30) not null,
	app_id long unsigned,
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
	id long unsigned,
	job_id long unsigned,
	completing_node varchar(30),
	active tinyint,
	complete tinyint,
	status varchar(70),
	last_modified varchar(30),
	PRIMARY KEY (id),
	FOREIGN KEY (job_id) REFERENCES job(id) ON DELETE CASCADE
);

CREATE TABLE dependency (
	id long unsigned,
	dep_id long unsigned,
	PRIMARY KEY (id, dep_id),
	FOREIGN KEY (id) REFERENCES job(id) ON DELETE CASCADE,
	FOREIGN KEY (dep_id) REFERENCES job(id) ON DELETE CASCADE
);


CREATE TABLE job_file (
	id long unsigned,
	filename varchar(50),
	PRIMARY KEY (id, filename),
	FOREIGN KEY (id) REFERENCES job(id) ON DELETE CASCADE
);
