package com.logic;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

import com.domain.Employee;

public class ManageEmployee {
	private static SessionFactory factory;
	static Logger log = Logger.getLogger(ManageEmployee.class.getName());
	private static org.hibernate.classic.Session session;

	public static void main(String[] args) {
		try {
			factory = new Configuration().configure("hibernate.cfg.xml").buildSessionFactory();
			session = factory.openSession();

		} catch (Throwable ex) {
			ex.printStackTrace();
		}
		ManageEmployee me = new ManageEmployee();

		log.info("Add employee record in database");
		me.addEmployee("New", "1", 9999.994);
		log.info("Add employee record in database");
		me.addEmployee("New", "1", 999.9);

		log.info("List down all the employees");
		me.listEmployees();

		log.info("Update employee's records");
		me.updateEmployee(1, 5000);

		log.info("Delete an employee from the database");
		me.deleteEmployee(9);

		log.info("List down all the employees");
		me.listEmployees();

		session.close();
	}

	/* Method to CREATE an employee in the database */
	public Integer addEmployee(String fname, String lname, double salary) {
		Transaction tx = null;
		Integer employeeID = null;
		try {
			tx = session.beginTransaction();
			Employee employee = new Employee(fname, lname, salary);
			employeeID = (Integer) session.save(employee);
			tx.commit();
		} catch (HibernateException e) {
			if (tx != null)
				tx.rollback();
			e.printStackTrace();
		}
		return employeeID;
	}

	/* Method to READ all the employees */
	public void listEmployees() {
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			List<Employee> employees = session.createQuery("FROM Employee").list();
			for (Iterator<Employee> iterator = employees.iterator(); iterator.hasNext();) {
				Employee employee = iterator.next();
				log.info("First Name: " + employee.getFirstName());
				log.info(" Last Name: " + employee.getLastName());
				log.info(" Salary: " + employee.getSalary());
			}
			tx.commit();
		} catch (HibernateException e) {
			if (tx != null)
				tx.rollback();
			e.printStackTrace();
		}
	}

	/* Method to UPDATE salary for an employee */
	public void updateEmployee(Integer EmployeeID, int salary) {
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			Employee employee = (Employee) session.get(Employee.class, EmployeeID);
			employee.setSalary(salary);
			session.update(employee);
			tx.commit();
		} catch (Exception e) {
			if (tx != null)
				tx.rollback();
			e.printStackTrace();
		}
	}

	/* Method to DELETE an employee from the records */
	public void deleteEmployee(Integer EmployeeID) {
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			Employee employee = (Employee) session.get(Employee.class, EmployeeID);
			session.delete(employee);
			tx.commit();
		} catch (HibernateException e) {
			if (tx != null)
				tx.rollback();
			e.printStackTrace();
		}
	}
}
