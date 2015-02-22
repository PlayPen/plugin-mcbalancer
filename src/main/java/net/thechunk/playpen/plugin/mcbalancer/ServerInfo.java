package net.thechunk.playpen.plugin.mcbalancer;

import lombok.Data;
import net.thechunk.playpen.coordinator.network.Server;

import java.net.InetSocketAddress;

@Data
public class ServerInfo {
    private ServerConfig config;
    private Server server;
    private InetSocketAddress address;
    private int players;
    private int maxPlayers;
    private boolean error = false;
}
