package net.thechunk.playpen.plugin.mcbalancer;

import lombok.Data;

@Data
public class ServerConfig {
    private String packageId;
    private String prefix;
    private double targetRatio;
    private int minServers;
    private int maxServers;
    private long autoRestartTime;
}
