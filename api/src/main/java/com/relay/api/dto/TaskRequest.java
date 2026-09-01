package com.relay.api.dto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskRequest {

    private String id;
    private String type;
    private String adapterType = "inline";
    private String owner;
    private String environment;
    private Integer version = 1;
    private String idempotencyKey;
    private Map<String, Object> payload = new HashMap<>();
    private List<String> dependsOn = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAdapterType() {
        return adapterType == null || adapterType.isBlank() ? "inline" : adapterType;
    }

    public void setAdapterType(String adapterType) {
        this.adapterType = adapterType == null || adapterType.isBlank() ? "inline" : adapterType;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public Integer getVersion() {
        return version == null ? 1 : version;
    }

    public void setVersion(Integer version) {
        this.version = version == null ? 1 : version;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload == null ? new HashMap<>() : payload;
    }

    public List<String> getDependsOn() {
        return dependsOn;
    }

    public void setDependsOn(List<String> dependsOn) {
        this.dependsOn = dependsOn == null ? new ArrayList<>() : dependsOn;
    }
}
