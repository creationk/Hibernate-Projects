package com.domain;

public class Employee {
	   private int id;
	   private String firstName; 
	   private String lastName;   
	   private double salary;  

	   public Employee() {}
	   public Employee(String fname, String lname, double d) {
	      this.firstName = fname;
	      this.lastName = lname;
	      this.salary = d;
	   }
	   public int getId() {
	      return id;
	   }
	   public void setId( int id ) {
	      this.id = id;
	   }
	   public String getFirstName() {
	      return firstName;
	   }
	   public void setFirstName( String first_name ) {
	      this.firstName = first_name;
	   }
	   public String getLastName() {
	      return lastName;
	   }
	   public void setLastName( String last_name ) {
	      this.lastName = last_name;
	   }
	   public double getSalary() {
	      return salary;
	   }
	   public void setSalary( double salary ) {
	      this.salary = salary;
	   }
	}