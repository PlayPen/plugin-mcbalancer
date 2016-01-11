package io.playpen.plugin.mcbalancer;

import io.playpen.core.coordinator.network.Server;
import lombok.Data;

import java.net.InetSocketAddress;

@Data
public class ServerInfo {
    private ServerConfig config;
    private Server server;
    private InetSocketAddress address;
    private int players;
    private int maxPlayers;
    private long startupTime;
    private boolean error = false;
    private boolean dnr = false;
}
