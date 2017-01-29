package com.data.application;

import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;

import com.data.entities.User;
import com.data.util.HibernateUtil;

public class Application {

	public static void main(String[] args) {
		Logger logger = LogManager.getLogger(Application.class.getName());

		Session session = HibernateUtil.getSessionfactory().openSession();
		session.getTransaction().begin();

		User user = new User();
		user.setBirthdate(new Date(17, 12, 1991));
		user.setCreatedBy("Srujana Kalluru");
		user.setCreatedDate(new Date());

		// not-null property references a null or transient value:
		// com.data.entities.User.emailAddress
		// user.setEmailAddress(null);
		user.setEmailAddress("myemail@email.com");
		user.setFirstName("Srujana");
		user.setLastName("Kalluru");
		user.setLastUpdatedBy("Srujana Kalluru");
		user.setLastUpdatedDate(new Date());

		session.save(user);

		User fetchUser = (User) session.get(User.class, user.getUserId());

		logger.info(
				"\n\nis user==fetchUser? :" + (user == fetchUser) + "\nuser.equals(fetchUser): " + user.equals(fetchUser));

		fetchUser.setFirstName("BBB");
		fetchUser.setLastName("AAA");
		session.save(fetchUser);
		session.getTransaction().commit();

		session.close();

	}

}
