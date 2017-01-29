package com.data.entities;

import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.TableGenerator;
import javax.persistence.Transient;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Entity
@Table(name = "APPUSER")
@Access(value = AccessType.PROPERTY)
public class User {
	@Transient
	final Level MYLOG = Level.forName("MYLOG", 350);

	static Logger logger = LogManager.getLogger(User.class.getName());
	
	private Long userId;
	private String firstName;
	private String lastName;
	private Date birthdate;
	private String emailAddress;
	private Date lastUpdatedDate;
	private String lastUpdatedBy;
	private Date createdDate;
	private String createdBy;
	
	/*#1: Generation type = AUTO */	
	
	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)	
	
	
	/*#2: Generation type = SEQUENCE */	
	/*
	@Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="id_sequence")
	@SequenceGenerator(name="id_sequence",sequenceName="ID_SEQ")	
	*/
	
	/*#3: Generation type = TABLE */	
	/*
	@Id
	@GeneratedValue(strategy=GenerationType.TABLE,generator="user_table_generator")	
	@TableGenerator(name="user_table_generator",table="USERIDKEY",pkColumnName="PK_NAME",valueColumnName="PK_VALUE")
	*/
	
	
	@Column(name = "USER_ID")
	public Long getUserId() {
		logger.log(Level.getLevel("MYLOG"), "************\ngetUserId(): "+userId);
		return userId;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	@Column (name="FN",updatable=false)
	public String getFirstName() {
		logger.info("In getFirstName()");
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public Date getBirthdate() {
		return birthdate;
	}

	public void setBirthdate(Date birthdate) {
		this.birthdate = birthdate;
	}
	@Column(nullable=false)
	public String getEmailAddress() {
		return emailAddress;
	}

	public void setEmailAddress(String emailAdress) {
		this.emailAddress = emailAdress;
	}

	public Date getLastUpdatedDate() {
		return lastUpdatedDate;
	}

	public void setLastUpdatedDate(Date lastUpdatedDate) {
		this.lastUpdatedDate = lastUpdatedDate;
	}
	@Transient
	public String getLastUpdatedBy() {
		return lastUpdatedBy;
	}

	public void setLastUpdatedBy(String lastUpdatedBy) {
		this.lastUpdatedBy = lastUpdatedBy;
	}

	@Column (updatable=false)
	public Date getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}
	@Column (updatable=false)
	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

}
