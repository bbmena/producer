package com.proserve.logs.producer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.proserve.logs.producer.config.KafkaProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.syslog.inbound.RFC6587SyslogDeserializer;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
public class KafkaController {

    @Autowired
    private KafkaProducerConfig kafkaProducerConfig;

    @RequestMapping(path = "/logdrain")
    public void message(@RequestBody String body) throws ExecutionException, InterruptedException, IOException {
        // "application/logplex-1" does not conform to RFC5424.
        // It leaves out STRUCTURED-DATA but does not replace it with
        // a NILVALUE. To workaround this, we inject empty STRUCTURED-DATA.
        String[] parts = body.split("router - ");
//        String log = parts[0] + "router - [] " + (parts.length > 1 ? parts[1] : "");
        String log = "242 <158>1 2016-06-20T21:56:57.107495+00:00 host heroku router - at=info method=GET path=\"/\" host=demodayex.herokuapp.com request_id=1850b395-c7aa-485c-aa04-7d0894b5f276 fwd=\"68.32.161.89\" dyno=web.1 connect=0ms service=6ms status=200 bytes=1548";
        RFC6587SyslogDeserializer parser = new RFC6587SyslogDeserializer();
        InputStream is = new ByteArrayInputStream(log.getBytes());
        Map<String, ?> messages = parser.deserialize(is);
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(messages);

        kafkaProducerConfig.send(json);
    }
}
