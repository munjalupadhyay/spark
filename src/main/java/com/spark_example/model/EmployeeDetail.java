package com.spark_example.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EmployeeDetail  implements Serializable {

  private static final long serialVersionUID = 1504015432749746005L;

  long id;
  String name;
  String surname;
  Double salary;
  int pin;
  String cityname;
  String shortform;
}
