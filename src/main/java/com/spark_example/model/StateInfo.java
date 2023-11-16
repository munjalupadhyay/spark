package com.spark_example.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StateInfo implements Serializable {

  private static final long serialVersionUID = 1503014432749746005L;

  String cityname;
  String shortform;
  int pincode;

}
