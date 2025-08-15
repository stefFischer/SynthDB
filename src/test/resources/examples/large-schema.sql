
CREATE TABLE department (
    department_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(100)
);

CREATE TABLE employee (
    employee_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    hire_date DATE NOT NULL,
    department_id INT REFERENCES department(department_id)
);

CREATE TABLE project (
    project_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE
);

CREATE TABLE client (
    client_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    industry VARCHAR(50)
);

-- Many-to-many: employees can work on many projects
CREATE TABLE employee_project (
    employee_id INT REFERENCES employee(employee_id) ON DELETE CASCADE,
    project_id INT REFERENCES project(project_id) ON DELETE CASCADE,
    role VARCHAR(50),
    PRIMARY KEY (employee_id, project_id)
);

-- Many-to-many: projects have many clients (and vice versa)
CREATE TABLE project_client (
    project_id INT REFERENCES project(project_id) ON DELETE CASCADE,
    client_id INT REFERENCES client(client_id) ON DELETE CASCADE,
    contract_value NUMERIC(12, 2),
    PRIMARY KEY (project_id, client_id)
);

-- Another layer: tasks inside projects
CREATE TABLE task (
    task_id INT PRIMARY KEY AUTO_INCREMENT,
    project_id INT REFERENCES project(project_id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    due_date DATE
);

-- Many-to-many: employees assigned to tasks
CREATE TABLE task_assignment (
    task_id INT REFERENCES task(task_id) ON DELETE CASCADE,
    employee_id INT REFERENCES employee(employee_id) ON DELETE CASCADE,
    hours_allocated INT,
    PRIMARY KEY (task_id, employee_id)
);

-- Longer chain: certifications for employees
CREATE TABLE certification (
    certification_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    issuer VARCHAR(100),
    valid_until DATE
);

CREATE TABLE employee_certification (
    employee_id INT REFERENCES employee(employee_id) ON DELETE CASCADE,
    certification_id INT REFERENCES certification(certification_id) ON DELETE CASCADE,
    PRIMARY KEY (employee_id, certification_id)
);

-- Vendors providing services for projects via clients (indirect relation path)
CREATE TABLE vendor (
    vendor_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    service_type VARCHAR(100)
);

CREATE TABLE client_vendor (
    client_id INT REFERENCES client(client_id) ON DELETE CASCADE,
    vendor_id INT REFERENCES vendor(vendor_id) ON DELETE CASCADE,
    PRIMARY KEY (client_id, vendor_id)
);

-- Long path example:
-- Employee -> Employee_Project -> Project -> Project_Client -> Client -> Client_Vendor -> Vendor

-- Extra: departments can partner with vendors directly
CREATE TABLE department_vendor (
    department_id INT REFERENCES department(department_id) ON DELETE CASCADE,
    vendor_id INT REFERENCES vendor(vendor_id) ON DELETE CASCADE,
    PRIMARY KEY (department_id, vendor_id)
);