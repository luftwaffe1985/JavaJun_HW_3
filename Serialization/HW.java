package Serialization;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class HW {

    /**
     * Создайте класс Person с полями name и age. Реализуйте сериализацию и
     * десериализацию этого класса в файл.
     *
     * Используя JPA, создайте базу данных для хранения объектов класса Person.
     * Реализуйте методы для добавления, обновления и удаления объектов Person.
     */

    public void createPersonTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table person (
                    id bigint primary key,
                    name varchar(256),
                    age integer,
                    active boolean
                    )
                    """);
        }
    }

    public void addPersonData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("add into person(id, name, age, active) values\n");
            for (int i = 1; i <= 10; i++) {
                int age = ThreadLocalRandom.current().nextInt(20, 60);
                boolean active = ThreadLocalRandom.current().nextBoolean();
                insertQuery.append(String.format("(%s, '%s', %s, %s)", i, "Person #" + i, age, active));
                if (i != 10) {
                    insertQuery.append(",\n");
                }
            }
            int insertCount = statement.executeUpdate(insertQuery.toString());
            System.out.println("Added lines: " + insertCount);
        }
    }

    public void selectPersonData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
                    select id, name, age, active, department_id
                    from person""");
            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                boolean active = resultSet.getBoolean("active");
                long dep_id = resultSet.getLong("department_id");
                System.out.println("[id = " + id + ", name = " + name + ", age = " + age + ", active = " + active
                        + ", depatment_id = " + dep_id + "]");
            }
        }
    }

    public void createDepartmentTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table department (
                    id bigint primary key,
                    name varchar(128) not null
                    )
                    """);
        }
    }

    public void addDepartmentData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("added into department(id, name) values\n");
            for (int i = 1; i <= 3; i++) {
                insertQuery.append(String.format("(%s, '%s')", i, "Department #" + i));
                if (i != 3) {
                    insertQuery.append(",\n");
                }
            }
            int insertCount = statement.executeUpdate(insertQuery.toString());
            System.out.println("Added lines: " + insertCount);
        }
    }

    public void selectDepartmentData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select id, name from department");
            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                System.out.println("[id = " + id + ", name = " + name + ", age = " + "]");
            }
        }
    }

    public int getRowsCount(Connection connection, String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("select count (1) from " + tableName);
            rs.next();
            return rs.getInt(1);
        }
    }

    public void changePersonColumn(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("change the Person table adding department_id bigint");
            int countRows = getRowsCount(connection, "person");
            for (int i = 1; i <= countRows; i++) {
                int idDepartment = ThreadLocalRandom.current().nextInt(1, 4);
                statement.executeUpdate(
                        "update the Person data setting department_id = " + idDepartment + " where id = " + i);
            }
            System.out.println("department_id column added and filled in with random values");
        }
    }

    public String getPersonDepartmentName(Connection connection, long personId) throws SQLException {
        String nameDep = "";
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(
                    "select name from department where id = select department_id from person where person.id = "
                            + personId);
            while (resultSet.next()) {
                nameDep = resultSet.getString("name");
                System.out.print("Employee with ID = " + personId + " is in the department ");
                // throw new UnsupportedOperationException();
            }
        }
        return nameDep;
    }

    public Map<String, String> getDepartments(Connection connection) throws SQLException {
        Map<String, String> personDepMap = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
                    select person.name name, dep.name dep from
                    (select id,  name, department_id from person) as person
                    left join
                    (select id, name from department) as dep  on  person.department_id = dep.id""");
            while (resultSet.next()) {
                String namePerson = resultSet.getString("name");
                String nameDep = resultSet.getString("dep");
                if (!personDepMap.containsKey(namePerson)) {
                    personDepMap.put(namePerson, nameDep);
                }
                // personDepMap.computeIfAbsent(namePerson, data -> nameDep);
            }
        }
        return personDepMap;
    }

    public Map<String, List<String>> getDepartmentPersons(Connection connection) throws SQLException {
        Map<String, List<String>> departmentPersMap = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
                    select person.name name, dep.name dep from
                    (select id,  name, department_id from person) as person
                    left join
                    (select id, name from department) as dep  on  person.department_id = dep.id""");
            while (resultSet.next()) {
                String namePers = resultSet.getString("name");
                String nameDep = resultSet.getString("dep");
                if (!departmentPersMap.containsKey(nameDep)) {
                    departmentPersMap.put(nameDep, new ArrayList<>());
                    departmentPersMap.get(nameDep).add(namePers);
                } else {
                    departmentPersMap.get(nameDep).add(namePers);
                }
            }
        }
        return departmentPersMap;
    }
}