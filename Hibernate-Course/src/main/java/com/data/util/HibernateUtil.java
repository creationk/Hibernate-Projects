package com.data.util;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

public class HibernateUtil {
	private static final SessionFactory sessionFactory = buildSessionFactory();

	private static SessionFactory buildSessionFactory() {
		try {
			Configuration configuration = new Configuration().configure();
			/*
			 * When new Configuration() invoked, it will try to read the
			 * configuration from hibernate.properties. When configure() invoked,
			 * it will read hibernate.cfg.xml by default
			 */
			return configuration.buildSessionFactory();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error creating sessionFactory");
		}
	}

	public static SessionFactory getSessionfactory() {
		return sessionFactory;
	}
}
