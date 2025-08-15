-- ==========================
-- Table data: department
-- ==========================
INSERT INTO department (name, location) VALUES 
	('Sales', 'New York City'),
	('Marketing and Advertising', 'Los Angeles'),
	('Product Development', 'San Francisco'),
	('Human Resources', 'Chicago'),
	('Digital Transformation', 'Seattle');

-- ==========================
-- Table data: project
-- ==========================
INSERT INTO project (name, start_date, end_date) VALUES 
	('Project Alpha', '2022-01-01', '2023-01-01'),
	('Project Beta', '2020-06-15', NULL),
	('Project Gamma', '2019-03-01', '2021-09-30'),
	('Project Delta', '2024-02-20', '2026-08-31'),
	('Project Epsilon', '2015-09-01', '2018-03-15');

-- ==========================
-- Table data: client
-- ==========================
INSERT INTO client (name, industry) VALUES 
	('Alfredo Enterprises', 'Finance'),
	('Everest Corporation', 'Technology'),
	('NovaTech Solutions', 'Energy'),
	('Kazimir Industries', 'Construction'),
	('Vest Technologies', 'Agriculture'),
	('Nova Group Inc.', 'Education'),
	('Eclipse Industries Ltd.', 'Healthcare'),
	('Rigel Corporation', 'Manufacturing');

-- ==========================
-- Table data: vendor
-- ==========================
INSERT INTO vendor (name, service_type) VALUES 
	('Walmart', 'Retail'),
	('Costco Wholesale', 'Retail'),
	('Target', 'Retail'),
	('BJs Wholesale Club', 'Membership based warehouse club'),
	('Kohl\\s Department Stores', 'Department store');

-- ==========================
-- Table data: certification
-- ==========================
INSERT INTO certification (name, issuer, valid_until) VALUES 
	('TOEFL', 'ETS - Educational Testing Service', '2024-06-30'),
	('GRE', 'ETS - Educational Testing Service', '2030-03-15'),
	('Certified Scrum Master', 'Scrum Alliance', '2025-09-01'),
	('Advanced Placement', 'College Board', '2030-03-15'),
	('German Language Proficiency Test', 'Goethe-Institut', '2026-12-31');

-- ==========================
-- Table data: task
-- ==========================
INSERT INTO task (project_id, name, due_date) VALUES 
	(1, 'Finish report for quarterly review', '2023-04-30'),
	(4, 'Conduct market analysis for Project Delta', '2023-09-20'),
	(2, 'Develop a comprehensive testing plan for Project Beta', '2024-02-10'),
	(5, 'Create project charter for Project Epsilon', '2023-11-15'),
	(3, 'Develop a proof of concept for Project Gamma', '2024-05-25');

-- ==========================
-- Table data: project_client
-- ==========================
INSERT INTO project_client (project_id, client_id, contract_value) VALUES 
	(4, 3, 25000.0),
	(5, 7, 40000.0),
	(1, 8, '60000.00'),
	(4, 8, 70000.0),
	(2, 5, 80000.0);

-- ==========================
-- Table data: employee
-- ==========================
INSERT INTO employee (first_name, last_name, hire_date, department_id) VALUES 
	('Raj', 'Singh', '2019-07-01', 3),
	('Emily', 'Lee', '2020-03-01', 4),
	('Sophia', 'Kim', '2021-10-01', 5),
	('Maya', 'Patel', '2022-01-15', 1),
	('Ava', 'Wang', '2022-05-01', 2);

-- ==========================
-- Table data: department_vendor
-- ==========================
INSERT INTO department_vendor (department_id, vendor_id) VALUES 
	(2, 5),
	(5, 2),
	(4, 3),
	(4, 1),
	(1, 4);

-- ==========================
-- Table data: client_vendor
-- ==========================
INSERT INTO client_vendor (client_id, vendor_id) VALUES 
	(1, 3),
	(5, 2),
	(6, 5),
	(8, 5),
	(4, 1);

-- ==========================
-- Table data: employee_certification
-- ==========================
INSERT INTO employee_certification (employee_id, certification_id) VALUES 
	(5, 3),
	(1, 4),
	(3, 1),
	(2, 5),
	(4, 3);

-- ==========================
-- Table data: task_assignment
-- ==========================
INSERT INTO task_assignment (task_id, employee_id, hours_allocated) VALUES 
	(1, 4, 40),
	(8, 2, 48),
	(6, 5, 32),
	(7, 3, 24),
	(6, 1, 56);

-- ==========================
-- Table data: employee_project
-- ==========================
INSERT INTO employee_project (employee_id, project_id, role) VALUES 
	(5, 1, 'Junior Developer'),
	(2, 1, 'Senior Quality Assurance Engineer'),
	(3, 5, 'Data Analyst'),
	(4, 3, 'UX Designer'),
	(1, 2, 'Full Stack Developer');

