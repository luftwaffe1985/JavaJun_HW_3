package Serialization;

import java.sql.*;

public class Main {
    public static void main(String[] args) {
        HW homework = new HW();
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test")) {
            homework.createPersonTable(connection);
            homework.addPersonData(connection);
            homework.createDepartmentTable(connection);
            homework.addDepartmentData(connection);
            homework.selectDepartmentData(connection);
            homework.changePersonColumn(connection);
            homework.selectPersonData(connection);
            System.out.println(homework.getPersonDepartmentName(connection, 4));
            System.out.println(homework.getDepartments(connection));
            System.out.println(homework.getDepartmentPersons(connection));
        } catch (SQLException e) {
            System.err.println("Connection error: " + e.getMessage());
        }
    }
}