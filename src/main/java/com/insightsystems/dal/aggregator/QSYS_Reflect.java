package com.insightsystems.dal.aggregator;

import com.avispl.symphony.api.dal.dto.monitor.ExtendedStatistics;
import com.avispl.symphony.api.dal.dto.monitor.Statistics;
import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.api.dal.monitor.Monitorable;
import com.avispl.symphony.api.dal.monitor.aggregator.Aggregator;
import com.avispl.symphony.dal.communicator.RestCommunicator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.auth.AuthenticationException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;

import javax.naming.ConfigurationException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * QSC Q-SYS Reflect Device Aggregator
 * Company: Insight Systems
 * @author Jayden Loone (@JaydenLInsight)
 * @version 0.2
 *
 */
public class QSYS_Reflect extends RestCommunicator implements Aggregator, Monitorable {
    private static final String API_URL = "/api/public/v0";

    private enum ObjectType {core,item}
    private int systemCount = 0;
    private int coreCount = 0;
    private int itemCount = 0;
    private long lastPoll = 0L;


    public QSYS_Reflect(){
        this.setBaseUri(API_URL);
    }

    @Override
    protected void authenticate() throws Exception {}

    @Override
    protected void internalInit() throws Exception {
        super.internalInit();
        if (this.getPassword().isEmpty())
            throw new AuthenticationException("API Token is required. Please provide API token as device HTTP Password");

        if (!this.getProtocol().equals("https"))
            throw new ConfigurationException("API does not support HTTP. Please enable HTTPS and retry.");
    }

    @Override
    public List<Statistics> getMultipleStatistics() throws Exception {
        ExtendedStatistics extStats = new ExtendedStatistics();
        Map<String,String> stats = new HashMap<>();

        stats.put("NumSystems",systemCount+"");
        stats.put("NumCores",coreCount+"");
        stats.put("NumItems",itemCount+"");
        stats.put("lastPollTime",lastPoll+"");

        extStats.setStatistics(stats);

        return Collections.singletonList(extStats);
    }

    @Override
    public List<AggregatedDevice> retrieveMultipleStatistics() throws Exception {
        List<AggregatedDevice> devices = new ArrayList<>();
        ArrayNode systems = this.doGet("systems",ArrayNode.class);
        if (!systems.isArray()) { throw new Exception("Unexpected response from the server. Expected an array."); }


        lastPoll = System.currentTimeMillis();
        systemCount = 0;
        coreCount = 0;
        itemCount = 0;
        Iterator<JsonNode> systemNodes = systems.elements();
        while (systemNodes.hasNext()){ //Loop through each system
            JsonNode system = systemNodes.next();
            systemCount++;
            String systemId = system.at("/id").asText();
            String coreId = system.at("/core/id").asText();

            JsonNode coreInfo = this.doGet("cores/"+coreId,JsonNode.class);
            AggregatedDevice core = createDevice(coreInfo,systemId,ObjectType.core);

            ArrayNode items = this.doGet("systems/"+systemId+"/items",ArrayNode.class);
            Iterator<JsonNode> itemNodes = items.elements();
            while (itemNodes.hasNext()) { //Loop through each item in the system
                JsonNode item = itemNodes.next();
                itemCount++;
                if (item.at("/type").asText().equals("Core")){ //Add the additional stats from the Core Inventory item to the device
                    Map<String,String> stats = new HashMap<>();
                    stats.put("Inventory#StatusCode",item.at("/status/code").asText());
                    stats.put("Inventory#StatusMessage",item.at("/status/message").asText());
                    stats.put("Inventory#StatusDetails",item.at("/status/details").asText());
                    stats.put("Inventory#Location",item.at("/location").asText());

                    core.setDeviceOnline(item.at("/status/message").asText().equals("OK"));

                    Map<String,String> coreStats = core.getStatistics();
                    coreStats.putAll(stats);
                    core.setStatistics(coreStats);

                } else {
                    devices.add(createDevice(item,systemId,ObjectType.item));
                }
            }
            devices.add(core);

            ArrayNode cores = this.doGet("cores",ArrayNode.class);
            if (cores.isArray()){
                coreCount = cores.size();
            }
        }
        return devices;
    }

    private AggregatedDevice createDevice(JsonNode deviceNode,String systemId,ObjectType objectType) {
        AggregatedDevice device = new AggregatedDevice();
        Map<String,String> stats = new HashMap<>();

        device.setTimestamp(System.currentTimeMillis());
        switch (objectType){
            case core:
                device.setDeviceMake("QSC");
                device.setDeviceName(deviceNode.at("/name").asText() + " " + deviceNode.at("/model").asText() );
                device.setCategory("Audio DSP");
                device.setDeviceId(deviceNode.at("/serial").asText());
                device.setDeviceModel(deviceNode.at("/model").asText());
                device.setType("Audio DSP");

                //Extended Statistics
                stats.put("FirmwareVersion",deviceNode.at("/firmware").asText());
                stats.put("AccessMode",deviceNode.at("/accessMode").asText());
                stats.put("AccessLevel",deviceNode.at("/accessLevel").asText());
                stats.put("UptimeSince",deviceNode.at("/uptime").asText());

                stats.put("StatusCode",deviceNode.at("/status/code").asText());
                stats.put("StatusMessage",deviceNode.at("/status/message").asText());
                stats.put("StatusDetails",deviceNode.at("/status/details").asText());

                if (deviceNode.at("/redundancy").isNull()){
                    stats.put("Redundancy","Not Available");
                }

                //Todo- Include redundancy setup and status, need redundant setup to see what the API returns

                break;
            case item:
                device.setDeviceName(deviceNode.at("/name").asText() + " " + deviceNode.at("/model").asText() );
                device.setCategory(deviceNode.at("/type").asText());
                device.setDeviceId(systemId + "-" + deviceNode.at("/id").asText());
                device.setDeviceModel(deviceNode.at("/model").asText());
                device.setDeviceMake(deviceNode.at("/manufacturer").asText());
                device.setType(deviceNode.at("/type").asText());
                device.setDeviceOnline(deviceNode.at("/status/message").asText().equals("OK"));
                stats.put("StatusCode",deviceNode.at("/status/code").asText());
                stats.put("StatusMessage",deviceNode.at("/status/message").asText());
                stats.put("StatusDetails",deviceNode.at("/status/details").asText());
                stats.put("Location",deviceNode.at("/location").asText());
                break;
        }
        device.setStatistics(stats);
        return device;
    }

    @Override
    public List<AggregatedDevice> retrieveMultipleStatistics(List<String> list) throws Exception {
        return retrieveMultipleStatistics()
                .stream()
                .filter(aggregatedDevice -> list.contains(aggregatedDevice.getDeviceId()))
                .collect(Collectors.toList());
    }

    @Override
    protected HttpHeaders putExtraRequestHeaders(HttpMethod httpMethod, String uri, HttpHeaders headers) throws Exception {
        headers.set("Content-Type","application/json");
        headers.set("Authorization","Bearer " + this.getPassword());
        return headers;
    }

    public static void main(String[] args) throws Exception {
        QSYS_Reflect ag = new QSYS_Reflect();
        ag.setHost("reflect.qsc.com");
        ag.setPassword("ebbfaf5d8f0558038d5f4fba3f9066be413a6d19e45a85a1246033789400ca44");
        ag.setLogin("");
        ag.setProtocol("https");
        ag.init();

        ag.retrieveMultipleStatistics().forEach(device ->{
            System.out.println(device.getDeviceName());
            System.out.println("\t"+ device.getDeviceId());
            System.out.println("\t"+ device.getDeviceMake());
            System.out.println("\t"+ device.getDeviceModel());
            System.out.println("\t"+ device.getType());
            System.out.println("\t"+ device.getDeviceOnline());
            device.getStatistics().forEach((k,v)->{
                System.out.println("\t\t"+ k+" : " + v);
            });
        });
    }
}
