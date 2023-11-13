package com.spark_example.model;

import java.io.Serializable;


public class Employee implements Serializable {

  private static final long serialVersionUID = 1504015432749744005L;
  long id;
  String name;
  String surname;


  public Employee() {
  }

  public Employee(long id, String name, String surname) {
    this.id = id;
    this.name = name;
    this.surname = surname;
  }

  @Override
  public String toString() {
    return "Employee{" +
        "id=" + id +
        "| name='" + name + '\'' +
        "| surname='" + surname + '\'' +
        '}';
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSurname() {
    return surname;
  }

  public void setSurname(String surname) {
    this.surname = surname;
  }
}
