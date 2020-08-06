package com.zhevzhyk.test;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

public class App {

  public static void main(String[] args) throws Exception {
    String serviceUrl = "pulsar+ssl://..."; // TODO: change the service url
    String adminUrl = "https://..."; // TODO: change the admin url
    String token = "your kesque jwt"; // TODO: change the token
    String topic = "your topic name"; // TODO: change the topic name

    System.out.print("Starting ... \n" + adminUrl + "\n");

    ClientConfigurationData conf = new ClientConfigurationData();
    conf.setServiceUrl(serviceUrl);
    conf.setUseTls(true);
    conf.setAuthPluginClassName(AuthenticationToken.class.getName());
    conf.setAuthParams(token);

    StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties props = new Properties();
    props.setProperty("topic", topic);

    FlinkPulsarSource<String> source = new FlinkPulsarSource<>(adminUrl, conf, new SimpleStringSchema(), props);

    DataStream<String> stream = see.addSource(source);
    stream.addSink(new DiscardingSink<>());

    try {
      System.out.print("starting execute() \n");
      see.execute();
    } catch (Exception e) {
      System.out.print(e.getMessage());
    }
  }
}
