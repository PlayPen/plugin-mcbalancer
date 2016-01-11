package net.thechunk.playpen.plugin.mcbalancer;

import lombok.Data;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import net.thechunk.playpen.coordinator.network.LocalCoordinator;
import net.thechunk.playpen.coordinator.network.Network;
import net.thechunk.playpen.coordinator.network.Server;
import net.thechunk.playpen.p3.P3Package;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class Balancer {

    @Getter
    private static AtomicBoolean isBalancing = new AtomicBoolean(false);

    private static Map<String, Integer> notResponding = new HashMap<>();

    private static ExecutorService executor = Executors.newFixedThreadPool(8);

    public static void balance() {
        if(!isBalancing.compareAndSet(false, true)) {
            Network.get().pluginMessage(MCBalancerPlugin.getInstance(), "log", "Balance already in progress");
            return;
        }

        try {
            log.debug("Balancing network");

            List<ServerInfo> infoList = new LinkedList<>();
            Map<String, CoordinatorNumerics> coordNumerics = new HashMap<>();
            Map<String, PackageNumerics> p3Numerics = new HashMap<>();

            // build a list of managed servers
            for (LocalCoordinator coord : Network.get().getCoordinators().values()) {
                if (!coord.isEnabled())
                    continue;

                InetAddress ip = ((InetSocketAddress) coord.getChannel().remoteAddress()).getAddress();

                List<ServerInfo> servers = new LinkedList<>();

                for (Server server : coord.getServers().values()) {
                    if (!server.isActive())
                        continue;

                    if (!"mcbalancer".equals(server.getProperties().getOrDefault("managed_by", null)))
                        continue;

                    if (!MCBalancerPlugin.getInstance().getConfigs().containsKey(server.getP3().getId())) {
                        log.warn("Server " + server.getName() + " has a package that isn't registered despite being managed");
                        continue;
                    }

                    if (!server.getProperties().containsKey("port")) {
                        log.warn("Server " + server.getName() + " doesn't have a port set despite being managed");
                        continue;
                    }

                    int port;
                    try {
                        port = Integer.parseInt(server.getProperties().get("port"));
                    } catch (NumberFormatException e) {
                        log.warn("Server " + server.getName() + " has an invalid port despite being managed", e);
                        continue;
                    }

                    long startupTime;
                    try {
                        startupTime = Long.parseLong(server.getProperties().get("start_time"));
                    } catch(NumberFormatException e) {
                        log.warn("Server " + server.getName() + " has an invalid start time despite being managed", e);
                        continue;
                    }

                    ServerInfo info = new ServerInfo();
                    info.setServer(server);
                    info.setAddress(new InetSocketAddress(ip, port));
                    info.setConfig(MCBalancerPlugin.getInstance().getConfigs().get(server.getP3().getId()));
                    info.setStartupTime(startupTime);
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

                    PackageNumerics pn = p3Numerics.getOrDefault(server.getConfig().getPackageId(), null);
                    if(pn == null) {
                        pn = new PackageNumerics();
                        p3Numerics.put(server.getConfig().getPackageId(), pn);
                    }

                    int id = Integer.valueOf(server.getServer().getProperties().getOrDefault("id", "0"));
                    if (pn.getMaxId() < id) {
                        for (int i = pn.getMaxId() + 1; i < id; ++i) {
                            pn.getAvailableIds().add(i);
                        }

                        pn.setMaxId(id);
                    }

                    cn.getAvailablePorts().remove(port);
                    pn.getAvailableIds().remove(id);
                }

                coordNumerics.put(coord.getUuid(), cn);
            }

            // deprovision any servers that are too old
            long currentTime = System.currentTimeMillis() / 1000L;

            Iterator<ServerInfo> it = infoList.iterator();
            int lifespanDeprovisions = 0;
            while(it.hasNext()) {
                if (lifespanDeprovisions >= MCBalancerPlugin.getInstance().getLifespanDeprovisionLimit()) {
                    log.info("Deferring lifespan deprovisions to a later time as the limit per balance was hit");
                    Network.get().pluginMessage(MCBalancerPlugin.getInstance(), "log", "Deferring lifespan deprovisions to a later time as the limit per balance was hit");
                    break;
                }

                ServerInfo info = it.next();

                if(info.getConfig().getAutoRestartTime() <= 0)
                    continue;

                if(currentTime - info.getStartupTime() > info.getConfig().getAutoRestartTime()) {
                    // deprovision the server
                    log.info("Server " + info.getServer().getName() + " has reached its lifetime, requesting deprovision");

                    Network.get().deprovision(info.getServer().getCoordinator().getUuid(), info.getServer().getUuid());
                    Network.get().pluginMessage(MCBalancerPlugin.getInstance(), "log", "Server " + info.getServer().getName() + " has reached its lifetime");
                    it.remove();

                    ++lifespanDeprovisions;
                }
            }

            // IT'S THE FINAL COUNTDOWN
            // DUH DUH DUH DUH
            // DUH DUH DUH DUH DUH
            // DUH DUH DUH DUH
            // DUH DUH DUH DUH DUH DUH DUH
            // https://www.youtube.com/watch?v=9jK-NcRmVcw
            // TODO: Maybe we should wait on futures instead?
            final CountDownLatch latch = new CountDownLatch(infoList.size());

            // ping the servers
            for (final ServerInfo info : infoList) {
                info.setDnr(true);
                executor.execute(() -> {
                    ServerListPing ping = new ServerListPing();
                    ping.setTimeout(500);
                    ping.setHost(info.getAddress());
                    try {
                        ServerListPing.StatusResponse pingReply = ping.fetchData();
                        if (pingReply == null) throw new Exception("No response received");
                        info.setDnr(false);
                        info.setPlayers(pingReply.getPlayers().getOnline());
                        info.setMaxPlayers(pingReply.getPlayers().getMax());
                        info.setError(false);
                    } catch (Exception e) {
                        log.warn("Unable to ping server " + info.getServer().getName(), e);
                        info.setError(true);
                        info.setDnr(true);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            try {
                if(!latch.await(10, TimeUnit.SECONDS))
                {
                    log.warn("Some servers did not respond within 10 seconds");
                    for(ServerInfo info : infoList) {
                        if(info.isDnr()) {
                            log.warn("Server " + info.getServer().getName() + " (" + info.getServer().getUuid() + " on " +
                                            info.getServer().getCoordinator().getName() + ") did not respond");
                            info.setError(true);

                            if(notResponding.containsKey(info.getServer().getUuid())) {
                                Integer count = notResponding.get(info.getServer().getUuid());
                                count++;
                                notResponding.put(info.getServer().getUuid(), count);
                                if(count >= MCBalancerPlugin.getInstance().getDnrAttempts()) {
                                    log.warn("Server " + info.getServer().getName() + " has hit max DNR attempts, deprovisioning.");
                                    Network.get().pluginMessage(MCBalancerPlugin.getInstance(), "log", "Server " + info.getServer().getName() + " hit max DNR attempts. Forcing deprovision.");
                                    Network.get().deprovision(info.getServer().getCoordinator().getUuid(), info.getServer().getUuid(), true);
                                }
                                else
                                {
                                    Network.get().pluginMessage(MCBalancerPlugin.getInstance(), "log", "Server " + info.getServer().getName() +
                                            " (" + info.getServer().getUuid() + " on " + info.getServer().getCoordinator().getName() + ") did not respond");
                                }
                            }
                            else
                            {
                                notResponding.put(info.getServer().getUuid(), 1);
                                Network.get().pluginMessage(MCBalancerPlugin.getInstance(), "log", "Server " + info.getServer().getName() +
                                        " (" + info.getServer().getUuid() + " on " + info.getServer().getCoordinator().getName() + ") did not respond");
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for server ping responses", e);
                return;
            }

            Iterator<Map.Entry<String, Integer>> dnrIt = notResponding.entrySet().iterator();
            while(dnrIt.hasNext()) {
                Map.Entry<String, Integer> entry = dnrIt.next();
                boolean found = false;
                for(LocalCoordinator lc : Network.get().getCoordinators().values()) {
                    if(lc.getServer(entry.getKey()) != null) {
                        found = true;
                        break;
                    }
                }

                if(!found)
                    dnrIt.remove();
            }

            // sort the servers by package
            Map<String, List<ServerInfo>> packageMap = new HashMap<>();
            for (ServerInfo info : infoList) {
                if (!packageMap.containsKey(info.getServer().getP3().getId()))
                    packageMap.put(info.getServer().getP3().getId(), new LinkedList<>());

                List<ServerInfo> list = packageMap.get(info.getServer().getP3().getId());
                list.add(info);
            }

            currentTime = System.currentTimeMillis() / 1000L;

            // balance the servers
            for (ServerConfig config : MCBalancerPlugin.getInstance().getConfigs().values()) {
                List<ServerInfo> servers = packageMap.getOrDefault(config.getPackageId(), new LinkedList<>());

                int amt = config.getMinServers();
                double ratio = 0;
                double idealRatio = config.getTargetRatio();

                // figure out the ratio
                int totalPlayers = 0;
                int totalMaxPlayers = 0;
                double avgMaxPlayers = 0;
                int serverCount = 0;
                for (ServerInfo server : servers) {
                    if(server.isError())
                        continue;

                    if(server.getMaxPlayers() == 0)
                        continue;

                    totalPlayers += server.getPlayers();
                    totalMaxPlayers += server.getMaxPlayers();

                    avgMaxPlayers += server.getMaxPlayers();
                    ++serverCount;
                }

                if (serverCount != 0 && totalMaxPlayers != 0) {
                    // we need an average of max players per server since we may have servers with different max player counts
                    // due to running different versions of the same package.
                    avgMaxPlayers /= serverCount;

                    ratio = ((double) totalPlayers) / ((double) totalMaxPlayers);

                    // how many servers do we need to add/subtract to get a nice ratio
                    // idealRatio = totalPlayers / (totalMaxPlayers + avgMaxPlayers * amt)
                    // idealRatio * (totalMaxPlayers + avgMaxPlayers * amt) = totalPlayers
                    // totalMaxPlayers + avgMaxPlayers * amt = totalPlayers / idealRatio
                    // avgMaxPlayers * amt = totalPlayers / idealRatio - totalMaxPlayers
                    // amt = (totalPlayers / idealRatio - totalMaxPlayers) / avgMaxPlayers
                    amt = (int) Math.round((totalPlayers / idealRatio - totalMaxPlayers) / avgMaxPlayers);
                }

                int target = Math.max(config.getMinServers(), Math.min(config.getMaxServers(), servers.size() + amt));
                amt = target - servers.size();

                if (amt == 0) {
                    log.debug("Package " + config.getPackageId() + " needs no balancing");
                    continue;
                }

                log.info("Package " + config.getPackageId() + " needs balancing: ratio = " + ratio + ", ideal = " + idealRatio +
                        ", amt = " + amt);

                Network.get().pluginMessage(MCBalancerPlugin.getInstance(), "log", "Package " + config.getPackageId() +
                        " needs balancing (amt = " + amt + ")");

                if (amt > 0) {
                    P3Package p3 = Network.get().getPackageManager().resolve(config.getPackageId(), "promoted");
                    if (p3 == null) {
                        log.error("Unable to resolve package " + config.getPackageId() + " at promoted, can't balance");
                        continue;
                    }

                    for (int i = 0; i < amt; ++i) {
                        LocalCoordinator coord = Network.get().selectCoordinator(p3);
                        if (coord == null) {
                            log.error("Unable to select a coordinator for balancing");
                            break; // trying again and again won't get a different result
                        }

                        CoordinatorNumerics cn = coordNumerics.get(coord.getUuid());
                        if (cn == null) {
                            log.error("Unable to find numerics instance for coordinator " + coord.getUuid());
                            break; // ditto
                        }

                        PackageNumerics pn = p3Numerics.get(p3.getId());
                        if(pn == null) {
                            // Coordinators are guaranteed to have numerics, but if this is the first time
                            // we are provisioning a package, then we aren't guaranteed to have package numerics.
                            pn = new PackageNumerics();
                            p3Numerics.put(p3.getId(), pn);
                        }

                        // figure out what port and id to use
                        int port;
                        int id;

                        if (cn.getAvailablePorts().size() > 0) {
                            port = cn.getAvailablePorts().iterator().next();
                        } else {
                            port = cn.getMaxPort() + 1;
                            cn.setMaxPort(port);
                        }

                        cn.getAvailablePorts().remove(port);

                        if (pn.getAvailableIds().size() > 0) {
                            id = pn.getAvailableIds().iterator().next();
                        } else {
                            id = pn.getMaxId() + 1;
                            pn.setMaxId(id);
                        }

                        pn.getAvailableIds().remove(id);

                        // build our server properties
                        Map<String, String> props = new HashMap<>();
                        props.put("port", String.valueOf(port));
                        props.put("id", String.valueOf(id));
                        props.put("managed_by", "mcbalancer");
                        props.put("start_time", String.valueOf(currentTime));

                        String serverName = config.getPrefix() + id;
                        Network.get().provision(p3, serverName, props, coord.getUuid());
                    }
                } else {
                    servers.stream()
                            .filter(info -> info.getPlayers() == 0)
                            .limit(-amt)
                            .forEach(info -> Network.get().deprovision(info.getServer().getCoordinator().getUuid(), info.getServer().getUuid()));
                }
            }
        }
        catch(Exception e) { // it may be bad practice, but we *really* don't want the balancer failing
            log.error("There was a problem balancing servers", e);
        }
        finally {
            isBalancing.set(false);
        }
    }

    @Data
    private static class CoordinatorNumerics {
        private int maxPort = MCBalancerPlugin.getInstance().getMinPort() - 1;
        private Set<Integer> availablePorts = new HashSet<>();
    }

    @Data
    private static class PackageNumerics {
        private int maxId = 0;
        private Set<Integer> availableIds = new HashSet<>();
    }

    private Balancer() {}
}
