package com.spark_example.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class Employee implements Serializable {

  private static final long serialVersionUID = 1504015432749744005L;
  long id;
  String name;
  String surname;

  Double salary;


  public Employee() {
  }

  @Override
  public String toString() {
    return "Employee{" +
        "id=" + id +
        "| name='" + name + '\'' +
        "| surname='" + surname + '\'' +
        '}';
  }
}
