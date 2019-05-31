package com.emini.presto.config;


import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class JDBCConfig {

    private String connectionURL;
    private String user;
    private String pass;
}
