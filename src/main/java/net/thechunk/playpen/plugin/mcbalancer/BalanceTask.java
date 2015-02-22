package net.thechunk.playpen.plugin.mcbalancer;

import lombok.Data;
import lombok.extern.log4j.Log4j2;
import net.thechunk.craftywedge.ServerPinger;
import net.thechunk.playpen.coordinator.network.LocalCoordinator;
import net.thechunk.playpen.coordinator.network.Network;
import net.thechunk.playpen.coordinator.network.Server;
import net.thechunk.playpen.p3.P3Package;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@Log4j2
public class BalanceTask implements Runnable {
    @Override
    public void run() {
        log.debug("Balancing network");

        List<ServerInfo> infoList = new LinkedList<>();
        Map<String, CoordinatorNumerics> numerics = new HashMap<>();

        // build a list of managed servers
        for(LocalCoordinator coord : Network.get().getCoordinators().values()) {
            if(!coord.isEnabled())
                continue;

            String ip = ((InetSocketAddress)coord.getChannel().remoteAddress()).getAddress().getHostAddress();

            List<ServerInfo> servers = new LinkedList<>();

            for(Server server : coord.getServers().values()) {
                if(!server.isActive())
                    continue;

                if(!"mcbalancer".equals(server.getProperties().getOrDefault("managed_by", null)))
                    continue;

                if(!MCBalancerPlugin.getInstance().getConfigs().containsKey(server.getP3().getId())) {
                    log.warn("Server " + server.getName() + " has a package that isn't registered despite being managed");
                    continue;
                }

                if(!server.getProperties().containsKey("port")) {
                    log.warn("Server " + server.getName() + " doesn't have a port set despite being managed");
                    continue;
                }

                int port;
                try {
                    port = Integer.valueOf(server.getProperties().get("port"));
                }
                catch(NumberFormatException e) {
                    log.warn("Server " + server.getName() + " has an invalid port despite being managed", e);
                    continue;
                }

                ServerInfo info = new ServerInfo();
                info.setServer(server);
                info.setAddress(new InetSocketAddress(ip, port));
                info.setConfig(MCBalancerPlugin.getInstance().getConfigs().get(server.getP3().getId()));
                infoList.add(info);
                servers.add(info);
            }

            CoordinatorNumerics cn = new CoordinatorNumerics();
            for (ServerInfo server : servers) {
                int port = server.getAddress().getPort();
                if (cn.getMaxPort() < port) {
                    for (int i = cn.getMaxPort() + 1; i < port; ++i) {
                        cn.getAvailablePorts().add(i);
                    }

                    cn.setMaxPort(port);
                }

                int id = Integer.valueOf(server.getServer().getProperties().getOrDefault("port", "0"));
                if (cn.getMaxId() < id) {
                    for (int i = cn.getMaxId() + 1; i < id; ++i) {
                        cn.getAvailableIds().add(i);
                    }

                    cn.setMaxId(id);
                }

                cn.getAvailablePorts().remove(port);
                cn.getAvailableIds().remove(id);
            }

            numerics.put(coord.getUuid(), cn);
        }

        final CountDownLatch latch = new CountDownLatch(infoList.size());

        // ping the servers
        ServerPinger pinger = new ServerPinger(Network.get().getEventLoopGroup());
        for(final ServerInfo info : infoList) {
            pinger.ping(info.getAddress(), 500, (pingReply, error) -> {
                if(error != null) {
                    log.warn("Unable to ping server " + info.getServer().getName(), error);
                    info.setError(true);
                }
                else {
                    info.setPlayers(pingReply.getPlayers().getOnline());
                    info.setMaxPlayers(pingReply.getPlayers().getMax());
                }

                latch.countDown();
            });
        }

        try {
            latch.await();
        }
        catch(InterruptedException e) {
            log.error("Interrupted while waiting for server ping responses", e);
            return;
        }

        // sort the servers by package
        Map<String, List<ServerInfo>> packageMap = new HashMap<>();
        for(ServerInfo info : infoList) {
            if(!packageMap.containsKey(info.getServer().getP3().getId()))
                packageMap.put(info.getServer().getP3().getId(), new LinkedList<>());

            List<ServerInfo> list = packageMap.get(info.getServer().getP3().getId());
            list.add(info);
        }

        // balance the servers
        for(ServerConfig config : MCBalancerPlugin.getInstance().getConfigs().values()) {
            List<ServerInfo> servers = packageMap.getOrDefault(config.getPackageId(), new LinkedList<>());

            int amt = config.getMinServers();
            double ratio = 0;
            double idealRatio = config.getTargetRatio();

            // figure out the ratio
            int totalPlayers = 0;
            int totalMaxPlayers = 0;
            double avgMaxPlayers = 0;
            for(ServerInfo server : servers) {
                totalPlayers += server.getPlayers();
                totalMaxPlayers += server.getMaxPlayers();
                avgMaxPlayers = server.getMaxPlayers();
            }

            if(servers.size() != 0 && totalMaxPlayers != 0) {
                // we need an average of max players per server since we may have servers with different max player counts
                // due to running different versions of the same package.
                avgMaxPlayers /= servers.size();

                ratio = ((double) totalPlayers) / ((double) totalMaxPlayers);

                // how many servers do we need to add/subtract to get a nice ratio
                // idealRatio = totalPlayers / (totalMaxPlayers + avgMaxPlayers * amt)
                // idealRatio * (totalMaxPlayers + avgMaxPlayers * amt) = totalPlayers
                // totalMaxPlayers + avgMaxPlayers * amt = totalPlayers / idealRatio
                // avgMaxPlayers * amt = totalPlayers / idealRatio - totalMaxPlayers
                // amt = (totalPlayers / idealRatio - totalMaxPlayers) / avgMaxPlayers
                amt = (int)Math.round((totalPlayers / idealRatio - totalMaxPlayers) / avgMaxPlayers);
            }

            int target = Math.max(config.getMinServers(), Math.min(config.getMaxServers(), servers.size() + amt));
            amt = target - servers.size();

            if(amt == 0) {
                log.info("Package " + config.getPackageId() + " needs no balancing");
                continue;
            }

            log.info("Package " + config.getPackageId() + " needs balancing: ratio = " + ratio + ", ideal = " + idealRatio +
                        ", amt = " + amt);

            if(amt > 0) {
                P3Package p3 = Network.get().getPackageManager().resolve(config.getPackageId(), "promoted");
                if(p3 == null) {
                    log.error("Unable to resolve package " + config.getPackageId() + " at promoted, can't balance");
                    continue;
                }

                for(int i = 0; i < amt; ++i) {
                    LocalCoordinator coord = Network.get().selectCoordinator(p3);
                    if(coord == null) {
                        log.error("Unable to select a coordinator for balancing");
                        break; // trying again and again won't get a different result
                    }

                    CoordinatorNumerics cn = numerics.get(coord.getUuid());
                    if(cn == null) {
                        log.error("Unable to find numerics instance for coordinator " + coord.getUuid());
                        break; // ditto
                    }

                    // figure out what port and id to use
                    int port;
                    int id;

                    if(cn.getAvailablePorts().size() > 0) {
                        port = cn.getAvailablePorts().iterator().next();
                    }
                    else {
                        port = cn.getMaxPort() + 1;
                        cn.setMaxPort(port);
                    }

                    cn.getAvailablePorts().remove(port);

                    if(cn.getAvailableIds().size() > 0) {
                        id = cn.getAvailableIds().iterator().next();
                    }
                    else {
                        id = cn.getMaxId() + 1;
                        cn.setMaxId(id);
                    }

                    cn.getAvailableIds().remove(id);

                    // build our server properties
                    Map<String, String> props = new HashMap<>();
                    props.put("port", String.valueOf(port));
                    props.put("id", String.valueOf(id));

                    String serverName = config.getPrefix() + id;
                    Network.get().provision(p3, serverName, props, coord.getUuid());
                }
            }
            else {
                int i = -amt;
                for(ServerInfo info : servers) {
                    if(i == 0)
                        break;

                    if(info.getPlayers() == 0) {
                        i--;
                        Network.get().deprovision(info.getServer().getCoordinator().getUuid(), info.getServer().getUuid());
                    }
                }
            }
        }
    }

    @Data
    private static class CoordinatorNumerics {
        private int maxPort = MCBalancerPlugin.getInstance().getMinPort() - 1;
        private int maxId = 0;
        private Set<Integer> availablePorts = new HashSet<>();
        private Set<Integer> availableIds = new HashSet<>();
    }
}
