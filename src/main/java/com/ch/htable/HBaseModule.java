package com.ch.htable;

//import org.apache.hadoop.hbase.client.Connection;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;

//@Configuration
//public class HBaseModule {
//
//    @Bean
//    Connection connection() {
//        return PersistentConnectionFactory.getConnection();
//    }
//
//    @Bean
//    AnnotationAnalyzer metaModel() {
//        return AnnotationAnalyzer.getInstance();
//    }
//
//    @Bean
//    HEntityManager entityManager(Connection connection, AnnotationAnalyzer metaModel,
//                                 @Value("${fido.mapr.tablenamespace}") String tableNameSpace) {
//        return new HEntityManager(connection, metaModel, tableNameSpace);
//    }
//
//    @Bean
//    HBaseImportService importService(HEntityManager entityManager) {
//        return new HBaseImportService(entityManager);
//    }
//}
